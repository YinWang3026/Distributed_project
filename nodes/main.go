package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Structs
// Instructions send to other Nodes
type Instruction struct {
	From   string
	Name   string
	Params []string
	Term   int
	CI     CommitInst
	CILog  []CommitInst
}

// Instructions from web server
type CommitInst struct {
	Name   string
	Params []string
}

// Entries in the form, shared with web server
type Items struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
}

// Raft Object
type Raft struct {
	State  string       //leader, candidate, or follower
	Log    []CommitInst //list of instructions from web serv to be commited
	Term   int          //Election term
	Quorum int          //What is needed for quorum
	Time   int          //Time left to become an candidate
	Voted  []int        //Record of voted term numbers
}

// Global locks
var itemMapmutex = sync.RWMutex{} //itemMap mutex
var raftVoteLock = sync.Mutex{}   //vote mutex

// Global Map
var itemMap = make(map[string]string)

// Global Raft Object
var raft = Raft{}

// Functions

// getRandomInteger - Returns a random integer between specified range
func getRandomInteger() int {
	//Random time from  5 - 15 secs for testing purposes, can be set longer
	return rand.Intn(10) + 5
}

// addItem - Adds an item to map
func addItem(key string, value string) error { //Adding item to map
	if len(key) == 0 {
		return errors.New("addItem: Key has length 0") //failed
	}
	_, ok := itemMap[key] // value, exists
	if !ok {              // Item name not used yet
		itemMap[key] = value
		return nil //success
	}
	return errors.New("addItem: Item exists") //failed
}

// deleteItem - Deletes an item from map
func deleteItem(key string) error {
	_, ok := itemMap[key]
	if ok { //If item exist, delete
		delete(itemMap, key)
		return nil //success
	}
	return errors.New("deleteItem: Item doesn't exist") //failed
}

// updateItem - Updates a key/val pair in map
func updateItem(oldKey string, key string, value string) error {
	if len(key) == 0 { //New name must not be empty
		return errors.New("updateItem: New key has length 0") //failed
	}
	_, ok := itemMap[key]
	if ok && key != oldKey { //Name cannot be taken
		return errors.New("updateItem: New key is taken") //failed
	}
	delete(itemMap, oldKey) //Remove the item
	itemMap[key] = value    //Add the updated item
	return nil              //success
}

// msgNode - Make requests to a given address
func msgNode(address string, inst Instruction) (int, error) {
	//Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Failed to dial: %q", address))
	}
	defer conn.Close()

	//Encodng and sending request
	encoder := json.NewEncoder(conn)
	err = encoder.Encode(inst)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Encoding error, Instruction Failed: %q", inst.Name))
	}

	//Decoding response message
	var response int
	decoder := json.NewDecoder(conn)
	err = decoder.Decode(&response)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Decoding error, Instruction Failed: %q", inst.Name))
	}

	return response, nil
}

func alive(backendAddr []string) {
	for {
		if raft.State == "leader" { //Only leader can heartbeat other backends
			inst := Instruction{From: "Backend", Name: "alive", Term: raft.Term, CILog: raft.Log} //Also passes Term number and Log just incase a node is out of sync
			for _, addr := range backendAddr {
				response, err := msgNode(addr, inst) //Dialing
				if err != nil || response == 0 {
					fmt.Println("Detected failure on localhost", addr, "at", time.Now().UTC())
				}
				time.Sleep(1 * time.Second) // Pause 1 second on making each call
			}
		}
		time.Sleep(5 * time.Second) // Checking backends every 5 seconds
	}
}

func raftTimer(backendAddr []string) {
	for {
		//Countdown in seconds
		for raft.Time != 0 {
			raft.Time = raft.Time - 1
			time.Sleep(1 * time.Second)
		}
		//If not leader, on time expiration become Candidate or remain Candidate
		if raft.State == "follower" {
			raft.State = "candidate"
		}
		//If candidate, hold election
		if raft.State == "candidate" {
			inst := Instruction{From: "Backend", Name: "voteForMe", Term: raft.Term}
			raft.Term = raft.Term + 1 //Increase term
			//Asking for votes
			votes := 1 //Voted for self
			for _, addr := range backendAddr {
				// Going around the room, asking for votes
				response, _ := msgNode(addr, inst)
				if response == 1 {
					votes = votes + 1
				}
			}
			//On quorum promotes to Leader
			if votes >= raft.Quorum {
				raft.State = "leader"
			}
		}
		//Resets timer
		raft.Time = getRandomInteger()
	}
}

//For debugging purposes
func checkingLeader() {
	for {
		fmt.Println("I am", raft.State, " Time:", raft.Time, "Election Term:", raft.Term)
		fmt.Println(itemMap)
		time.Sleep(1 * time.Second)
	}
}

//Editing the global map
func mapEditor(c CommitInst) error {
	var err error
	itemMapmutex.Lock()
	if c.Name == "add" {
		err = addItem(c.Params[0], c.Params[1])
	} else if c.Name == "update" {
		err = updateItem(c.Params[0], c.Params[1], c.Params[2])
	} else if c.Name == "delete" {
		err = deleteItem(c.Params[0])
	}
	itemMapmutex.Unlock()
	return err
}

func webServerInstHandler(inst *Instruction, backendAddr []string, encoder *json.Encoder) {
	if inst.Name == "leader" {
		// Leader responds with 1
		encoder.Encode(1)
	} else if inst.Name == "itemMap" {
		// Request all items in the item map
		x := []Items{}       //JSON object to send
		itemMapmutex.RLock() //Read lock
		for key, val := range itemMap {
			x = append(x, Items{Name: key, Desc: val})
		}
		itemMapmutex.RUnlock() //Read unlock
		encoder.Encode(x)      //Responses to frontend
	} else {
		//Add to log
		//send commit messages
		//Comit on quorum
		//Tell followers to commit
		//Respond to frontend

		// Create the commit instruction
		ctInst := CommitInst{Name: inst.Name, Params: inst.Params}
		// Send commit instruction to all nodes
		requestInst := Instruction{From: "Backend", Name: "addToLog", CI: ctInst}
		votes := 1
		for _, val := range backendAddr {
			response, _ := msgNode(val, requestInst) //Telling other back end to append inst
			if response == 1 {
				votes = votes + 1
			}
		}
		// Upon quorum commit the instruction
		if votes >= raft.Quorum {
			// Adding commit inst to Log
			raft.Log = append(raft.Log, ctInst)

			// Committing
			err := mapEditor(ctInst)
			if err != nil {
				// Failed to make the commit, abort
				fmt.Fprintf(os.Stderr, err.Error())

				// TODO: All other nodes need to delete the commit from log
				raft.Log = raft.Log[:len(raft.Log)-1] // Removing the failed commit from log
				encoder.Encode(0)                     // Tell webserv instruction failed
				return
			}

			// Respond to webserver with success
			encoder.Encode(1)

			// Request other nodes to commit
			x := Instruction{From: "Backend", Name: "commit"}
			for _, val := range backendAddr {
				response, _ := msgNode(val, x) //Telling other back end to commit
				if response == 0 {
					fmt.Println("Failed to commmit:", val)
					// TODO: ALl other ndoes need to re-sync with leader
				}
			}
		} else { //No quorom, tell front end operation failed
			encoder.Encode(0)
		}
	}
}

func nodeInstHandler(inst *Instruction, backendAddr []string, encoder *json.Encoder) {
	//Reset timer on any backend messages
	raft.Time = getRandomInteger()
	if inst.Name == "voteForMe" && raft.State != "leader" {
		//Increase term and vote
		raftVoteLock.Lock()
		i := inst.Term
		v := false //Did not vote
		for _, val := range raft.Voted {
			if i == val {
				v = true //voted already
			}
		}
		if v == false {
			//set term to candidate
			raft.Term = i
			raft.Voted = append(raft.Voted, i)
			encoder.Encode(1) //Vote 1
		} else {
			encoder.Encode(0) //Voted already
		}
		raftVoteLock.Unlock()

	} else if inst.Name == "addToLog" {
		//Add to log
		raft.Log = append(raft.Log, inst.CI)
		encoder.Encode(1)
	} else if inst.Name == "rmFromLog" {
		if len(raft.Log) > 0 {
			raft.Log = raft.Log[:len(raft.Log)-1]
			encoder.Encode(1)
		} else {
			encoder.Encode(0)
		}
	} else if inst.Name == "commit" {
		//Commit the last entry
		inst := raft.Log[len(raft.Log)-1] //Getting last item
		err := mapEditor(inst)            //Commit it
		if err != nil {
			fmt.Println(err.Error())
			encoder.Encode(0)
		} else {
			encoder.Encode(1)
		}
	} else if inst.Name == "alive" { //meaning there is a leader..
		if (raft.State == "leader" && raft.Term < inst.Term) || raft.State == "candidate" {
			raft.State = "follower"        //I am your follower
			raft.Log = inst.CILog          //Copying your log
			raft.Term = inst.Term          //and term
			initState()                    //Resetting data
			for _, val := range raft.Log { //Apply each instructions
				_ = mapEditor(val)
			}
		} else if raft.State == "follower" && raft.Term < inst.Term { //Outdated follower
			raft.Log = inst.CILog
			raft.Term = inst.Term
			initState()
			for _, val := range raft.Log {
				_ = mapEditor(val)
			}
		}
		// Any how, respond with I am alive
		encoder.Encode(1)
	}
}

func requestHandler(conn net.Conn, backendAddr []string) {
	// Defer the close
	defer conn.Close()

	//Encoder for sending response
	encoder := json.NewEncoder(conn)

	//Decoder for incoming instruction
	var inst Instruction
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&inst)
	if err != nil {
		encoder.Encode(0)
		fmt.Fprintf(os.Stderr, "Handler: %s\n", err.Error())
		return
	}
	fmt.Fprintf(os.Stderr, "Conn[%q], From[%q], InstName[%q]\n", conn.RemoteAddr().String(), inst.From, inst.Name)

	// Case switch
	if inst.From == "Frontend" && raft.State == "leader" {
		// Webserver communicates with leader node only
		webServerInstHandler(&inst, backendAddr, encoder)
	} else if inst.From == "Backend" {
		// Nodes
		nodeInstHandler(&inst, backendAddr, encoder)
	} else {
		// Invalid sender
		encoder.Encode(0)
	}
	fmt.Fprintln(os.Stderr, "connection ended: ", conn.RemoteAddr().String())
}

//Initializing global map
func initState() {
	_ = addItem("banana", "1111")
	_ = addItem("apple", "2222")
	_ = addItem("orange", "3333")
	_ = addItem("gapes", "tes4")
	_ = addItem("peach", "tes5")
	_ = addItem("fruits", "tes6")
	_ = addItem("oreo", "tes7")
}

func main() {
	//Adding test values to Map
	initState()

	//Getting cmd line arguments
	listenPtr := flag.String("listen", ":0000", "listen address")
	dialPtr := flag.String("backend", ":0000", "dial address")
	flag.Parse()
	if *listenPtr == ":0000" || *dialPtr == ":0000" {
		log.Fatal("Missing --listen --backend")
	}
	backendAddrList := strings.Split(*dialPtr, ",") //A list of other backends
	//Initializing raft object
	rand.Seed(time.Now().UnixNano())
	raft.State = "follower"
	raft.Log = []CommitInst{}
	raft.Term = 0
	raft.Quorum = (len(backendAddrList)+1)/2 + 1
	raft.Time = getRandomInteger()

	//Making threads
	go raftTimer(backendAddrList)
	go alive(backendAddrList)
	go checkingLeader()

	//Making listener
	ln, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		log.Fatal("Node cannot bind to socket")
	}
	fmt.Println("Node starting ...")
	for {
		//Accepting requests
		conn, err := ln.Accept()
		if err != nil { // Bad connection
			fmt.Fprintln(os.Stderr, "Failed to accept", conn.RemoteAddr().String())
			continue
		}
		// Handler - When accepted a connection, pass it into a thread
		go requestHandler(conn, backendAddrList)
	}
}
