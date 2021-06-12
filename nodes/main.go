package main

import (
	"encoding/json" // Encoders, decoders, marshal
	"errors"
	"flag" // Cmd argument parsing
	"fmt"
	"log"
	"math/rand"
	"net" // Listener and dials
	"os"  // Stderr
	"strings"
	"sync"
	"time"
)

// Structs
// Instructions send to other Nodes
type Request struct {
	From   string       `json:"from"`
	Name   string       `json:"name"`
	Key    string       `json:"key"`
	Value  string       `json:"value"`
	OldKey string       `json:"oldkey"`
	Term   int          `json:"term"`
	CI     Commitment   `json:"CI"`
	CILog  []Commitment `json:"CILog"`
}

// Instructions from web server
type Commitment struct {
	Name   string
	Key    string
	Value  string
	OldKey string
}

// Entries in the form, shared with web server
type Items struct {
	Name string `json:"name"`
	Desc string `json:"desc"`
}

// Raft Object
type Raft struct {
	State      string       //Leader, candidate, or follower
	Log        []Commitment //List of instructions from web serv that has been committed
	Term       int          //Election term
	Quorum     int          //What is needed for quorum
	Time       int          //Time left to become an candidate
	VotedTerms []int        //Record of voted term numbers
}

// Globals
var itemMapmutex = sync.RWMutex{}     //itemMap mutex
var raftVoteLock = sync.Mutex{}       //vote mutex
var itemMap = make(map[string]string) //itemMap
var raft = Raft{}                     //Raft object

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
	_, ok := itemMap[key] //value, exists
	if !ok {              //item name not used yet
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
func msgNode(address string, req Request) (int, error) {
	//Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Failed to dial: %q", address))
	}
	defer conn.Close()

	//Encodng and sending request
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Encoding error, Request Failed: %q", req.Name))
	}

	//Decoding response message
	var response int
	err = json.NewDecoder(conn).Decode(&response)
	if err != nil {
		return 0, errors.New(fmt.Sprintf("msgNode: Decoding error, Request Failed: %q", req.Name))
	}

	return response, nil
}

// alive - A thread function, constantly pinging other notes
func alive(backendAddr []string) {
	for {
		if raft.State == "leader" {
			// Only leader needs to heartbeat other nodes
			req := Request{From: "Backend", Name: "alive", Term: raft.Term, CILog: raft.Log}
			// Also passes Term number and Log just incase a node is out of sync
			for _, addr := range backendAddr {
				response, err := msgNode(addr, req) //Dialing
				if err != nil || response == 0 {
					fmt.Fprintf(os.Stderr, "Detected failure on %q at %q\n", addr, time.Now().UTC().String())
				}
				time.Sleep(1 * time.Second) // Pause 1 second on making each call
			}
		}
		time.Sleep(5 * time.Second) // Checking backends every 5 seconds
	}
}

// raftTimer - A thread function, tracks time for election
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
			req := Request{From: "Backend", Name: "voteForMe", Term: raft.Term}
			raft.Term = raft.Term + 1 //Increase term
			//Asking for votes
			votes := 1 //Voted for self
			for _, addr := range backendAddr {
				// Going around the room, asking for votes
				response, _ := msgNode(addr, req)
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

// checkingLeader - A thread function, for debugging purposes
func checkingLeader() {
	for {
		fmt.Println("I am", raft.State, " Time:", raft.Time, "Election Term:", raft.Term)
		fmt.Println(itemMap)
		time.Sleep(1 * time.Second)
	}
}

// mapEditor - Edits the item map
func mapEditor(c Commitment) error {
	var err error
	itemMapmutex.Lock()
	if c.Name == "add" {
		err = addItem(c.Key, c.Value)
	} else if c.Name == "update" {
		err = updateItem(c.OldKey, c.Key, c.Value)
	} else if c.Name == "delete" {
		err = deleteItem(c.Key)
	}
	itemMapmutex.Unlock()
	return err
}

// webHandler - Handles web server requests
func webHandler(req *Request, backendAddr []string, encoder *json.Encoder) {
	if req.Name == "leader" {
		// Leader responds with 1
		encoder.Encode(1)
	} else if req.Name == "itemMap" {
		// Request all items in the item map
		list := []Items{}    //JSON object to send
		itemMapmutex.RLock() //Read lock
		for key, val := range itemMap {
			list = append(list, Items{Name: key, Desc: val})
		}
		itemMapmutex.RUnlock() //Read unlock
		encoder.Encode(list)   //Responses to frontend
	} else {
		//Add to log
		//send commit messages
		//Comit on quorum
		//Tell followers to commit
		//Respond to web server

		// Create the commit instruction
		commit := Commitment{Name: req.Name, Key: req.Key, Value: req.Value, OldKey: req.OldKey}

		// Adding commit inst to Log
		raft.Log = append(raft.Log, commit)

		// Send commit instruction to all nodes
		nodeReq := Request{From: "Backend", Name: "addToLog", CI: commit}
		votes := 1
		for _, addr := range backendAddr {
			response, _ := msgNode(addr, nodeReq) //Telling other back end to append inst
			votes += response
		}
		// Upon quorum commit the instruction
		if votes >= raft.Quorum {
			// Committing
			err := mapEditor(commit)
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
			r := Request{From: "Backend", Name: "commit"}
			for _, addr := range backendAddr {
				response, _ := msgNode(addr, r) //Telling other back end to commit
				if response == 0 {
					fmt.Println("Failed to commmit:", addr)
					// TODO: ALl other ndoes need to re-sync with leader
				}
			}
		} else { //No quorom, tell front end operation failed
			// EVERZYONE NEEDS TO REMOVE FROM LOG
			raft.Log = raft.Log[:len(raft.Log)-1] // Removing the failed commit from log
			encoder.Encode(0)
		}
	}
}

// nodeHandler - Handles node requests
func nodeHandler(req *Request, backendAddr []string, encoder *json.Encoder) {
	//Reset timer on any backend messages
	raft.Time = getRandomInteger()

	//Only non-leader node can vote
	if req.Name == "voteForMe" && raft.State != "leader" {
		//Increase term and vote
		raftVoteLock.Lock()
		currentTerm := req.Term
		voted := false // Did I vote in this term already?
		for _, term := range raft.VotedTerms {
			if currentTerm == term {
				// currentTerm voted
				voted = true
				break
			}
		}
		if voted == false {
			//Did note vote, casting vote
			raft.Term = currentTerm
			raft.VotedTerms = append(raft.VotedTerms, currentTerm)
			encoder.Encode(1)
		} else {
			//Voted already, cannot vote again
			encoder.Encode(0)
		}
		raftVoteLock.Unlock()
	} else if req.Name == "addToLog" {
		//Add to log
		raft.Log = append(raft.Log, req.CI)
		encoder.Encode(1)
	} else if req.Name == "rmFromLog" {
		//Rm last commit from log
		if len(raft.Log) > 0 {
			raft.Log = raft.Log[:len(raft.Log)-1]
			encoder.Encode(1)
		} else {
			encoder.Encode(0)
		}
	} else if req.Name == "commit" {
		//Commit the last entry
		inst := raft.Log[len(raft.Log)-1] //Getting last item
		err := mapEditor(inst)            //Commit it
		if err != nil {
			encoder.Encode(0)
		} else {
			encoder.Encode(1)
		}
	} else if req.Name == "alive" {
		// Meaning there is a leader..
		if (raft.State == "leader" && raft.Term < req.Term) || raft.State == "candidate" {
			raft.State = "follower"        //I am your follower
			raft.Log = req.CILog           //Copying your log
			raft.Term = req.Term           //and term
			initState()                    //Resetting data
			for _, val := range raft.Log { //Apply each instructions
				_ = mapEditor(val)
			}
		} else if raft.State == "follower" && raft.Term < req.Term { //Outdated follower
			raft.Log = req.CILog
			raft.Term = req.Term
			initState()
			for _, val := range raft.Log {
				_ = mapEditor(val)
			}
		}
		// Any how, respond with I am alive
		encoder.Encode(1)
	}
}

// requestHandler - Handles request, and calls node handler or web handler
func requestHandler(conn net.Conn, backendAddr []string) {
	// Defer the close
	defer conn.Close()

	//Encoder for sending response
	encoder := json.NewEncoder(conn)

	//Decoder for incoming instruction
	var req Request
	err := json.NewDecoder(conn).Decode(&req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "requestHandler: Decoder error.")
		encoder.Encode(0)
		return
	}

	fmt.Fprintf(os.Stderr, "Conn[%q], From[%q], InstName[%q]\n", conn.RemoteAddr().String(), req.From, req.Name)

	// Mux
	if req.From == "Frontend" && raft.State == "leader" {
		// Webserver communicates with leader node only
		webHandler(&req, backendAddr, encoder)
	} else if req.From == "Backend" {
		// Nodes
		nodeHandler(&req, backendAddr, encoder)
	} else {
		// Invalid sender
		encoder.Encode(0)
	}

	fmt.Fprintln(os.Stderr, "connection ended:", conn.RemoteAddr().String())
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

	//A list of other nodes
	backendAddrList := strings.Split(*dialPtr, ",")

	//Initializing raft object
	rand.Seed(time.Now().UnixNano())
	raft.State = "follower"
	raft.Log = []Commitment{}
	raft.Term = 0
	raft.Quorum = (len(backendAddrList)+1)/2 + 1
	raft.Time = getRandomInteger()

	//Starting threads
	go raftTimer(backendAddrList)
	go alive(backendAddrList)
	go checkingLeader()

	//Starting listener
	ln, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		log.Fatal("Node cannot bind to socket.")
	}
	fmt.Println("Node starting ...")
	for {
		//Accepting requests
		conn, err := ln.Accept()
		if err != nil { // Bad connection
			fmt.Fprintln(os.Stderr, "Failed to accept:", conn.RemoteAddr().String())
			continue
		}
		fmt.Fprintln(os.Stderr, "Got a connection from:", conn.RemoteAddr().String())
		// Handler - When accepted a connection, pass it into a thread
		go requestHandler(conn, backendAddrList)
	}
}
