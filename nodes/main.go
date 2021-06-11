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
// Instruction export used for JSON encoding/decoding
type Instruction struct {
	From   string
	Name   string
	Params []string
	Term   int
	CI     CommitInst
	CILog  []CommitInst
}

type CommitInst struct {
	Name   string
	Params []string
}

type Items struct {
	Name string
	Desc string
}

type Raft struct {
	State  string       //leader candidate follower
	Log    []CommitInst //list of inst from Frontend
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

func addItem(key string, value string) error { //Adding item to map
	if len(key) == 0 {
		return errors.New("addItem: Key has length 0") //failed
	}
	_, ok := itemMap[key] // value, exists
	if !ok {              //Item name must not exist
		itemMap[key] = value
		return nil //success
	}
	return errors.New("addItem: Item exists") //failed
}

func deleteItem(key string) error { //Deleting item from map
	_, ok := itemMap[key]
	if ok { //If item exist, delete
		delete(itemMap, key)
		return nil //success
	}
	return errors.New("deleteItem: Item doesn't exist") //failed
}

func updateItem(oldKey string, key string, value string) error { //Updating item
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

//Making calls to other backends
func msgNodes(address string, inst Instruction) error {
	//Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		// fmt.Println("Connection Error, Instruction Failed:", inst.Name)
		return errors.New(fmt.Sprintf("msgNodes: Failed to dial: %q", address))
	}
	defer conn.Close()
	//Encodng and sending message
	encoder := json.NewEncoder(conn)
	encoder.Encode(inst)
	//Decoding and recieving message
	decoder := json.NewDecoder(conn)
	var x int
	err = decoder.Decode(&x)
	if err != nil {
		return errors.New(fmt.Sprintf("msgNodes: Decoding error, Instruction Failed: %q", inst.Name))
	}
	if x == 1 {
		return nil
	} else {
		return errors.New("Fail")
	}
}

func alive(backendAddr []string) {
	for {
		if raft.State == "leader" { //Only leader can heartbeat other backends
			inst := Instruction{From: "Backend", Name: "alive", Term: raft.Term, CILog: raft.Log} //Also passes Term number and Log just incase a node is out of sync
			for _, addr := range backendAddr {
				result := msgNodes(addr, inst) //Dialing
				if result != nil {
					fmt.Println("Detected failure on localhost", addr, "at", time.Now().UTC())
				}
				time.Sleep(1 * time.Second)
			}
		}
		time.Sleep(1 * time.Second) //Checking backends every 1 seconds
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
			x := Instruction{From: "Backend", Name: "voteForMe", Term: raft.Term}
			raft.Term = raft.Term + 1 //Increase term
			//Asking for votes
			votes := 1 //Voted for self
			for _, val := range backendAddr {
				err := msgNodes(val, x)
				if err == nil {
					votes = votes + 1
				}
			}
			//On quorum promotes to Leader
			if votes >= raft.Quorum {
				raft.State = "leader"
			}
		}
		//Resets timer
		raft.Time = rand.Intn(10) + 5
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
	if c.Name == "add" {
		err = addItem(c.Params[0], c.Params[1])
	} else if c.Name == "update" {
		err = updateItem(c.Params[0], c.Params[1], c.Params[2])
	} else if c.Name == "delete" {
		err = deleteItem(c.Params[0])
	}
	return err
}

func requestHandler(conn net.Conn, backendAddr []string) {
	//fmt.Fprintln(os.Stderr, "Accepted connection from", conn.RemoteAddr())

	//Decoding and receiving incoming message
	decoder := json.NewDecoder(conn)
	var inst Instruction //Declaring Instruction object
	derr := decoder.Decode(&inst)
	if derr != nil {
		fmt.Println("Handler: Decoder error")
		return
	}
	fmt.Println("Instruction From:", inst.From, "Instruction Name:", inst.Name)
	//Encoding and sending response
	encoder := json.NewEncoder(conn)

	if inst.From == "Frontend" && raft.State == "leader" {
		if inst.Name == "leader" {
			encoder.Encode(1) //Leader saying Hi I am leader
		} else if inst.Name == "itemMap" {
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
			c := CommitInst{Name: inst.Name, Params: inst.Params}      //Parse the incoming params
			x := Instruction{From: "Backend", Name: "addToLog", CI: c} //Making inst to other backend
			votes := 1
			for _, val := range backendAddr {
				result := msgNodes(val, x) //Telling other back end to append inst
				if result == nil {
					votes = votes + 1
				}
			}
			if votes >= raft.Quorum { //Upon quorum
				//Stack last inst in, first out
				raft.Log = append(raft.Log, c) //Add to log
				i := raft.Log[len(raft.Log)-1] //Getting last item
				itemMapmutex.Lock()
				result := mapEditor(i) //Commit it
				itemMapmutex.Unlock()
				encoder.Encode(result) //Respond to front end
				//Telling other backends to commit
				x := Instruction{From: "Backend", Name: "commit"}
				for _, val := range backendAddr {
					result = msgNodes(val, x) //Telling other back end to commit
					if result != nil {
						fmt.Println("Failed to commmit:", val)
					}
				}

			} else { //No quorom, tell front end operation failed
				encoder.Encode(0)
			}
		}
	} else if inst.From == "Backend" {
		//Reset timer on any backend messages
		raft.Time = rand.Intn(10) + 5
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
		} else if inst.Name == "commit" {
			//Commit the last entry
			i := raft.Log[len(raft.Log)-1] //Getting last item
			itemMapmutex.Lock()
			_ = mapEditor(i) //Commit it
			itemMapmutex.Unlock()
			encoder.Encode(1)
		} else if inst.Name == "alive" { //meaning there is a leader..
			if (raft.State == "leader" && raft.Term < inst.Term) || raft.State == "candidate" {
				raft.State = "follower" //I am your follower
				raft.Log = inst.CILog   //Copying your log
				raft.Term = inst.Term   //and term
				itemMapmutex.Lock()
				initState()                    //Resetting data
				for _, val := range raft.Log { //Apply each instructions
					_ = mapEditor(val)
				}
				itemMapmutex.Unlock()
			} else if raft.State == "follower" && raft.Term < inst.Term { //Outdated follower
				raft.Log = inst.CILog
				raft.Term = inst.Term
				itemMapmutex.Lock()
				initState()
				for _, val := range raft.Log {
					_ = mapEditor(val)
				}
				itemMapmutex.Unlock()
			}
			encoder.Encode(1)
		}
	} else {
		encoder.Encode(0)
	}
	// fmt.Fprintln(os.Stderr, "connection ended")
	fmt.Println("connection ended: ", conn.LocalAddr().String())
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
	backendAddr := strings.Split(*dialPtr, ",") //A list of other backends
	//Initializing raft object
	rand.Seed(time.Now().UnixNano())
	raft.State = "follower"
	raft.Log = []CommitInst{}
	raft.Term = 0
	raft.Quorum = (len(backendAddr)+1)/2 + 1
	raft.Time = rand.Intn(10) + 5 //Random time from  5 - 15 secs for testing purposes, can be set longer

	//Making threads
	go raftTimer(backendAddr)
	go alive(backendAddr)
	go checkingLeader()

	//Making listener
	ln, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		log.Fatal("Couldn't bind socket")
	}
	fmt.Println("Node starting")
	for {
		//Accepting requests
		conn, err := ln.Accept()
		defer conn.Close()
		if err != nil { // Bad connection
			fmt.Fprint(os.Stderr, "Failed to accept")
			continue
		}

		// Handler: When accepted a connection, pass it into a thread
		go func(conn net.Conn) {
			//fmt.Fprintln(os.Stderr, "Accepted connection from", conn.RemoteAddr())

			//Decoding and receiving incoming message
			decoder := json.NewDecoder(conn)
			var inst Instruction //Declaring Instruction object
			derr := decoder.Decode(&inst)
			if derr != nil {
				fmt.Println("Handler: Decoder error")
				return
			}
			fmt.Println("Instruction From:", inst.From, "Instruction Name:", inst.Name)
			//Encoding and sending response
			encoder := json.NewEncoder(conn)

			if inst.From == "Frontend" && raft.State == "leader" {
				if inst.Name == "leader" {
					encoder.Encode(1) //Leader saying Hi I am leader
				} else if inst.Name == "itemMap" {
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
					c := CommitInst{Name: inst.Name, Params: inst.Params}      //Parse the incoming params
					x := Instruction{From: "Backend", Name: "addToLog", CI: c} //Making inst to other backend
					votes := 1
					for _, val := range backendAddr {
						result := msgNodes(val, x) //Telling other back end to append inst
						if result == nil {
							votes = votes + 1
						}
					}
					if votes >= raft.Quorum { //Upon quorum
						//Stack last inst in, first out
						raft.Log = append(raft.Log, c) //Add to log
						i := raft.Log[len(raft.Log)-1] //Getting last item
						itemMapmutex.Lock()
						result := mapEditor(i) //Commit it
						itemMapmutex.Unlock()
						encoder.Encode(result) //Respond to front end
						//Telling other backends to commit
						x := Instruction{From: "Backend", Name: "commit"}
						for _, val := range backendAddr {
							result = msgNodes(val, x) //Telling other back end to commit
							if result != nil {
								fmt.Println("Failed to commmit:", val)
							}
						}

					} else { //No quorom, tell front end operation failed
						encoder.Encode(0)
					}
				}
			} else if inst.From == "Backend" {
				//Reset timer on any backend messages
				raft.Time = rand.Intn(10) + 5
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
				} else if inst.Name == "commit" {
					//Commit the last entry
					i := raft.Log[len(raft.Log)-1] //Getting last item
					itemMapmutex.Lock()
					_ = mapEditor(i) //Commit it
					itemMapmutex.Unlock()
					encoder.Encode(1)
				} else if inst.Name == "alive" { //meaning there is a leader..
					if (raft.State == "leader" && raft.Term < inst.Term) || raft.State == "candidate" {
						raft.State = "follower" //I am your follower
						raft.Log = inst.CILog   //Copying your log
						raft.Term = inst.Term   //and term
						itemMapmutex.Lock()
						initState()                    //Resetting data
						for _, val := range raft.Log { //Apply each instructions
							_ = mapEditor(val)
						}
						itemMapmutex.Unlock()
					} else if raft.State == "follower" && raft.Term < inst.Term { //Outdated follower
						raft.Log = inst.CILog
						raft.Term = inst.Term
						itemMapmutex.Lock()
						initState()
						for _, val := range raft.Log {
							_ = mapEditor(val)
						}
						itemMapmutex.Unlock()
					}
					encoder.Encode(1)
				}
			} else {
				encoder.Encode(0)
			}
			// fmt.Fprintln(os.Stderr, "connection ended")
			fmt.Println("connection ended: ", conn.LocalAddr().String())
		}(conn)
	}
}
