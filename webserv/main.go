package main

import (
	"encoding/json" // Marshal, unmarshal, encoding, decoding,
	"flag"          // Cmd line argument parsing
	"fmt"
	"log"      // log.Fatal()
	"net"      // TCP
	"net/http" // Http request handlers
	"os"
	"strings"
	"time"
)

// Request obj
type Request struct {
	From   string `json:"from"`
	Name   string `json:"name"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	OldKey string `json:"oldkey"`
}

// Globals
var nodeAddrList []string // List of node addresses
var leaderAddr string     // Address of leader node

// msgNode - sends request to given address, returns success/fail
func msgNode(address string, req Request) int {
	//Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Failed to dial:", address)
		return 0
	}
	defer conn.Close()
	//Encodng and sending message
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Encoding error, Request Failed:", req.Name)
		return 0
	}
	//Decoding and recieving message
	var response int = 0
	err = json.NewDecoder(conn).Decode(&response)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Decoding error, Request Failed:", req.Name)
		return 0
	}
	// 0 = Fail, 1 = Ok
	return response
}

// alive - Heart beat to leader node
func alive() {
	req := Request{From: "web", Name: "leader"}
	for {
		fmt.Println("Leader addr:", leaderAddr)
		result := msgNode(leaderAddr, req) //Dialing
		if result != 1 {
			//Leader fails
			fmt.Printf("Detected failure on leaderAddr[%q] at [%q]", leaderAddr, time.Now().UTC().String())
			//Find new leader
			req2 := Request{From: "web", Name: "leader"}
			for _, addr := range nodeAddrList { //Loops through backends and asks are you leader
				result := msgNode(addr, req2)
				if result == 1 {
					leaderAddr = addr
				}
			}
		}
		time.Sleep(3 * time.Second) //Dialing once every 3 seconds
	}
}

// Method:   GET
// Resource: http://localhost:8080/
func rootHandler(w http.ResponseWriter, r *http.Request) {
	//Dial connection
	conn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	//Encodng and sending message
	req := Request{From: "web", Name: "itemMap"}
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//Decoding and recieving message
	var itemMap map[string]string // itemMap
	err = json.NewDecoder(conn).Decode(&itemMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	//HTML
	ctx.HTML("<p>List of items</p>")
	ctx.HTML("<form action='/add' method='post'><input type='text' name='item' placeholder='Name of item'><input type='text' name='desc' placeholder='Description of item'>")
	ctx.HTML("<input type='submit' value='Add'></form>")
	//Displaying the items
	for key, val := range itemMap {
		ctx.HTML("<form method='post'><input type='text' name='oriItem' style='display:none;' readonly value=" + key + "><input type='text' name='item' value=" + key + "><input type='text' name='desc' value=" + val + ">")
		ctx.HTML("<input type='submit' formaction='/edit' value='Edit'><input type='submit' formaction='/delete' value='Delete'></form>")
		//ctx.WriteString("<pre>Item: " + key + "	Description: " + val + "	<a href='/edit/{" + key + "}/{" + val + "}'>Edit</a>	<a href='/delete/{" + key + "}'>Delete</a></pre>")
	}
}

// Method:   POST
// Resource: http://localhost:8080/add
func addHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "add", Key: r.FormValue("key"), Value: r.FormValue("value")}
	result := msgNode(leaderAddr, req)
	fmt.Println(req)
	if result == 1 {
		ctx.HTML("<p>Add Complete<p>")
	} else {
		ctx.HTML("<p>Add Failed. Item name exist or item name empty<p>")
	}
	ctx.HTML("<a href='/'>Return to main page<a>")
}

// Method:   POST
// Resource: http://localhost:8080/delete
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "delete", Key: r.FormValue("key")}
	result := msgNode(leaderAddr, req)
	fmt.Println(req)
	if result == 1 {
		ctx.HTML("<p>Delete Complete<p>")
	} else {
		ctx.HTML("<p>Delete Failed.<p>")
	}
	ctx.HTML("<a href='/'>Return to main page<a>")
}

// Method:   Post
// Resource: http://localhost:8080/update
func updateHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "update", Key: r.FormValue("key"), Value: r.FormValue("value"), OldKey: r.FormValue("oldkey")}
	fmt.Println(req)
	result := msgNode(leaderAddr, req)
	if result == 1 {
		ctx.HTML("<p>Edit Complete<p>")
	} else {
		ctx.HTML("<p>Edit Failed. Item name exist or item name empty<p>")
	}
	ctx.HTML("<a href='/'>Return to main page<a>")
}

func main() {
	// Getting command line arguments
	listenPtr := flag.String("listen", ":8080", "listen address")
	dialPtr := flag.String("nodes", ":8090,:8091,:8092", "node address")
	flag.Parse()

	// Initialzing globals
	nodeAddrList = strings.Split(*dialPtr, ",")
	leaderAddr = "0000"

	// Finding node leader, and heart beat the leader address
	go alive()

	http.HandleFunc("/", rootHandler)
	http.HandleFunc("/add", addHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/delete/", deleteHandler)

	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("../static/"))))
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("../static/"))))

	log.Fatal(http.ListenAndServe(*listenPtr, nil))
}
