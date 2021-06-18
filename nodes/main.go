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

// Raft Object
type Raft struct {
	State      string           // Leader, candidate, or follower
	Term       int              // Election term
	Quorum     int              // What is needed for quorum
	Time       int              // Time left to become an candidate
	VotedTerms map[int]struct{} // Record of voted term numbers, map string to nothing, a way to implement set
	Log        []Commitment     // List of instructions from web serv that has been committed
}

// Globals
var (
	itemMapmutex = sync.RWMutex{}          // itemMap mutex
	raftVoteLock = sync.Mutex{}            // vote mutex
	itemMap      = make(map[string]string) // itemMap
	raft         = Raft{}                  // Raft object
	nodeAddrList []string                  // List of node addresses
)

const (
	aliveTimer int = 5 // In seconds
	debugTimer int = 3
)

// getRandomInteger - Returns a random integer between specified range
func getRandomInteger() int {
	// Random time from  5 - 15 secs for testing purposes, can be set longer
	return rand.Intn(10) + 5
}

// addItem - Adds an item to map
func addItem(key string, value string) error { // Adding item to map
	if len(key) == 0 {
		return errors.New("addItem: Key has length 0") // failed
	}
	_, ok := itemMap[key] // value, exists
	if !ok {              // item name not used yet
		itemMap[key] = value
		return nil // success
	}
	return errors.New("addItem: Item exists") // failed
}

// deleteItem - Deletes an item from map
func deleteItem(key string) error {
	_, ok := itemMap[key]
	if ok { // If item exist, delete
		delete(itemMap, key)
		return nil // success
	}
	return errors.New("deleteItem: Item doesn't exist") // failed
}

// updateItem - Updates a key/val pair in map
func updateItem(oldKey string, key string, value string) error {
	if len(key) == 0 { // New name must not be empty
		return errors.New("updateItem: New key has length 0") // failed
	}
	_, ok := itemMap[key]
	if ok && key != oldKey { //Name cannot be taken
		return errors.New("updateItem: New key is taken") // failed
	}
	delete(itemMap, oldKey) // Remove the item
	itemMap[key] = value    // Add the updated item
	return nil              // success
}

// msgNode - Make requests to a given address
func msgNode(address string, req Request) int {
	// Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Failed to dial:", address)
		return 0
	}
	defer conn.Close()
	// Encodng and sending request
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Encoding error, Request Failed:", req.Name)
		return 0
	}
	// Decoding response message
	var response int = 0
	err = json.NewDecoder(conn).Decode(&response)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Decoding error, Request Failed:", req.Name)
		return 0
	}
	// 0 = Fail, 1 = Ok, 2 = Need log
	return response
}

// alive - A thread function, constantly pinging other notes
func alive() {
	for {
		if raft.State == "leader" {
			// Only leader needs to heartbeat other nodes
			req := Request{From: "node", Name: "alive", Term: raft.Term}
			// Also passes Term number incase a node is out of sync
			for _, addr := range nodeAddrList {
				response := msgNode(addr, req) //Dialing
				if response == 0 {
					// Failed
					fmt.Fprintf(os.Stderr, "Detected failure on [%q] at [%q]\n", addr, time.Now().UTC().String())
				} else if response == 2 {
					// Response requesting current log of commits
					req := Request{From: "node", Name: "updateLog", Term: raft.Term, CILog: raft.Log}
					response := msgNode(addr, req) //Dialing
					if response == 0 {
						// Failed
						fmt.Fprintf(os.Stderr, "Detected failure on [%q] at [%q]\n", addr, time.Now().UTC().String())
					}
				}
			}
		}
		time.Sleep(time.Duration(aliveTimer) * time.Second) // Checking backends every 5 seconds
	}
}

// raftTimer - A thread function, tracks time for election
func raftTimer() {
	for {
		//Countdown in seconds
		for raft.Time != 0 {
			raft.Time = raft.Time - 1
			time.Sleep(time.Second) // Sleep for a second
		}
		//If not leader, on time expiration become Candidate or remain Candidate
		if raft.State == "follower" {
			raft.State = "candidate"
		}
		//If candidate, hold election
		if raft.State == "candidate" {
			req := Request{From: "node", Name: "voteForMe", Term: raft.Term}
			raft.Term = raft.Term + 1 //Increase term
			//Asking for votes
			votes := 1 //Voted for self
			for _, addr := range nodeAddrList {
				// Going around the room, asking for votes
				response := msgNode(addr, req)
				if response == 1 {
					votes += 1
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
		fmt.Printf("I am [%q] Time [%d] Term [%d]\n", raft.State, raft.Time, raft.Term)
		fmt.Println(itemMap)
		time.Sleep(time.Duration(debugTimer) * time.Second)
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
func webHandler(req *Request, encoder *json.Encoder) {
	if req.Name == "leader" {
		// Leader responds with 1
		encoder.Encode(1)
	} else if req.Name == "itemMap" {
		// Request all items in the item map
		itemMapmutex.RLock()    // Read lock
		encoder.Encode(itemMap) // Encode the map and send it
		itemMapmutex.RUnlock()  // Read unlock
	} else if req.Name == "add" || req.Name == "delete" || req.Name == "update" {
		// Add to log, Send commit messages, Comit on quorum, Tell followers to commit, Respond to web server
		// Create the commitment
		commit := Commitment{Name: req.Name, Key: req.Key, Value: req.Value, OldKey: req.OldKey}
		// Adding commit to Log
		raft.Log = append(raft.Log, commit)
		// Send commit req to all nodes
		nodeReq := Request{From: "node", Name: "addToLog", CI: commit}
		votes := 1
		voters := make([]string, 0)
		for _, addr := range nodeAddrList {
			response := msgNode(addr, nodeReq) //Telling other back end to append inst
			if response == 1 {
				votes += 1
				voters = append(voters, addr)
			}
		}
		// Upon quorum commit the instruction
		if votes >= raft.Quorum {
			// Committing
			err := mapEditor(commit)
			if err != nil {
				// Failed to make the commit, abort
				fmt.Fprintf(os.Stderr, err.Error())
				raft.Log = raft.Log[:len(raft.Log)-1]           // Removing the failed commit from log
				req := Request{From: "node", Name: "rmFromLog"} // Requesting voters to remove from log
				for _, addr := range voters {
					msgNode(addr, req)
				}
				encoder.Encode(0) // Respond to web serv with fail
				return
			}
			// Respond to webserver with success
			encoder.Encode(1)
			// Request other nodes to commit
			req := Request{From: "node", Name: "commit"}
			for _, addr := range voters {
				msgNode(addr, req)
			}
		} else {
			// No quorum - Removing commit from log
			raft.Log = raft.Log[:len(raft.Log)-1]
			req := Request{From: "node", Name: "rmFromLog"}
			for _, addr := range voters {
				msgNode(addr, req)
			}
			encoder.Encode(0) // Respond to web serv with fail
		}
	} else {
		// Unknown request
		fmt.Fprintln(os.Stderr, "WebHandler: Unknown request:", req.Name)
		encoder.Encode(0)
	}
}

// nodeHandler - Handles node requests
func nodeHandler(req *Request, encoder *json.Encoder) {
	// Reset timer on any backend messages
	raft.Time = getRandomInteger()

	// Only non-leader node can vote
	if req.Name == "voteForMe" && raft.State != "leader" {
		// Increase term and vote
		raftVoteLock.Lock()
		currentTerm := req.Term
		_, voted := raft.VotedTerms[currentTerm] // is term in map?
		if voted == false {
			// Did note vote, casting vote
			raft.Term = currentTerm
			raft.VotedTerms[currentTerm] = struct{}{} // Empty struct
			encoder.Encode(1)
		} else {
			// Voted already, cannot vote again
			encoder.Encode(0)
		}
		raftVoteLock.Unlock()
	} else if req.Name == "addToLog" {
		// Add to log
		raft.Log = append(raft.Log, req.CI)
		encoder.Encode(1)
	} else if req.Name == "rmFromLog" {
		// Rm last commit from log
		if len(raft.Log) > 0 {
			raft.Log = raft.Log[:len(raft.Log)-1]
			encoder.Encode(1)
		} else {
			fmt.Fprintln(os.Stderr, "rmFromLog: Removing from empty log")
			encoder.Encode(0)
		}
	} else if req.Name == "commit" {
		// Commit the last entry
		inst := raft.Log[len(raft.Log)-1] // Getting last item
		err := mapEditor(inst)            // Commit it
		if err != nil {
			encoder.Encode(0)
		} else {
			encoder.Encode(1)
		}
	} else if req.Name == "alive" {
		// Meaning there is a leader..
		if (raft.State == "leader" && raft.Term < req.Term) || raft.State == "candidate" {
			raft.State = "follower" // I am your follower
			encoder.Encode(2)       // Need re-sync

		} else if raft.State == "follower" && raft.Term < req.Term { // Outdated follower
			encoder.Encode(2) // Need re-sync
		} else {
			// Any how, respond with I am alive
			encoder.Encode(1)
		}
	} else if req.Name == "updateLog" {
		// Sync
		raft.Log = req.CILog
		raft.Term = req.Term
		initState()
		for _, commit := range raft.Log {
			mapEditor(commit) // Better have no errors ...
		}
		encoder.Encode(1)
	} else {
		// Unknown request
		fmt.Fprintln(os.Stderr, "NodeHandler: Unknown request:", req.Name)
		encoder.Encode(0)
	}
}

// requestHandler - Handles request, and calls node handler or web handler
func requestHandler(conn net.Conn) {
	// Defer the close
	defer conn.Close()

	// Encoder for sending response
	encoder := json.NewEncoder(conn)

	// Decoder for incoming instruction
	var req Request
	err := json.NewDecoder(conn).Decode(&req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "requestHandler: Decoder error.")
		encoder.Encode(0)
		return
	}

	fmt.Printf("Conn[%q], From[%q], InstName[%q]\n", conn.RemoteAddr().String(), req.From, req.Name)

	// Mux
	if req.From == "web" && raft.State == "leader" {
		// Webserver communicates with leader node only
		webHandler(&req, encoder)
	} else if req.From == "node" {
		// Nodes
		nodeHandler(&req, encoder)
	} else {
		// Invalid sender
		encoder.Encode(0)
	}

	fmt.Println("connection ended:", conn.RemoteAddr().String())
}

// initState - Adding test entries to log and map
func initState() {
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "banana", Value: "1111"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "apple", Value: "2222"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "orange", Value: "3333"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "grapes", Value: "tes4"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "peach", Value: "tes5"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "fruits", Value: "tes6"})
	raft.Log = append(raft.Log, Commitment{Name: "add", Key: "oreo", Value: "tes7"})

	for _, c := range raft.Log {
		mapEditor(c) // Ignoring error
	}
}

func main() {
	// Getting cmd line arguments
	listenPtr := flag.String("listen", ":0000", "listen address")
	dialPtr := flag.String("nodes", ":0000", "dial address")
	flag.Parse()
	if *listenPtr == ":0000" || *dialPtr == ":0000" {
		log.Fatal("Missing --listen --nodes")
	}

	// A list of other nodes
	nodeAddrList = strings.Split(*dialPtr, ",")

	// Initializing raft object
	rand.Seed(time.Now().UnixNano())
	raft.State = "follower"
	raft.Log = make([]Commitment, 0)
	raft.VotedTerms = make(map[int]struct{})
	raft.Term = 0
	raft.Quorum = (len(nodeAddrList)+1)/2 + 1
	raft.Time = getRandomInteger()

	// Adding test values to Map
	initState()

	// Starting threads
	go raftTimer()
	go alive()
	go checkingLeader()

	// Starting listener
	ln, err := net.Listen("tcp", *listenPtr)
	if err != nil {
		log.Fatal("Node cannot bind to socket.")
	}
	defer ln.Close()
	fmt.Println("Node starting ...")
	for {
		// Accepting requests
		conn, err := ln.Accept()
		if err != nil { // Bad connection
			fmt.Fprintln(os.Stderr, "Failed to accept:", conn.RemoteAddr().String())
			continue
		}
		fmt.Println("Got a connection from:", conn.RemoteAddr().String())
		// Handler - When accepted a connection, pass it into a thread
		go requestHandler(conn)
	}
}
