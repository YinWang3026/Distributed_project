package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"

	// "net/http"
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
var itemMap = make(map[string]string) // itemMap
var nodeAddrList []string             // List of node addresses
var leaderAddr string                 // Address of leader node

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

func main() {
	//Getting command line arguments
	listenPtr := flag.String("listen", ":8080", "listen address")
	dialPtr := flag.String("nodes", ":8090,:8091,:8092", "node address")
	flag.Parse()
	nodeAddrList = strings.Split(*dialPtr, ",")
	leaderAddr = "0000"

	//Finding backend leader and setting leaderAddr to it.
	go alive()

	app := iris.Default()

	// Method:   GET
	// Resource: http://localhost:8080/
	app.Handle("GET", "/", func(ctx iris.Context) {
		//Dial connection
		conn, err := net.Dial("tcp", leaderAddr)
		if err != nil {
			fmt.Printf("Connection error")
			os.Exit(1)
		}
		defer conn.Close()
		//Encodng and sending message
		x := Request{From: "Frontend", Name: "itemMap", Params: []string{""}}
		encoder := json.NewEncoder(conn)
		encoder.Encode(x)
		//Decoding and recieving message
		decoder := json.NewDecoder(conn)
		var id []Items
		derr := decoder.Decode(&id)
		if derr != nil {
			fmt.Println("Decoding error")
			os.Exit(1)
		}
		//HTML
		ctx.HTML("<p>List of items</p>")
		ctx.HTML("<form action='/add' method='post'><input type='text' name='item' placeholder='Name of item'><input type='text' name='desc' placeholder='Description of item'>")
		ctx.HTML("<input type='submit' value='Add'></form>")
		//Displaying the items
		for _, i := range id {
			key := i.Name
			val := i.Desc
			ctx.HTML("<form method='post'><input type='text' name='oriItem' style='display:none;' readonly value=" + key + "><input type='text' name='item' value=" + key + "><input type='text' name='desc' value=" + val + ">")
			ctx.HTML("<input type='submit' formaction='/edit' value='Edit'><input type='submit' formaction='/delete' value='Delete'></form>")
			//ctx.WriteString("<pre>Item: " + key + "	Description: " + val + "	<a href='/edit/{" + key + "}/{" + val + "}'>Edit</a>	<a href='/delete/{" + key + "}'>Delete</a></pre>")
		}
	})

	// Method:   POST
	// Resource: http://localhost:8080/add
	app.Post("/add", func(ctx iris.Context) {
		form := ctx.FormValues()                                                                        //Reading form values
		x := Request{From: "Frontend", Name: "add", Params: []string{form["item"][0], form["desc"][0]}} //Making instruction
		result := request(leaderAddr, x)                                                                //Calling backend
		fmt.Println(x)
		if result == 1 {
			ctx.HTML("<p>Add Complete<p>")
		} else {
			ctx.HTML("<p>Add Failed. Item name exist or item name empty<p>")
		}
		ctx.HTML("<a href='/'>Return to main page<a>")

	})

	// Method:   Post
	// Resource: http://localhost:8080/edit
	app.Post("/edit", func(ctx iris.Context) {
		form := ctx.FormValues()                                                                                               //Reading form values
		x := Request{From: "Frontend", Name: "update", Params: []string{form["oriItem"][0], form["item"][0], form["desc"][0]}} //Making instruction
		fmt.Println(x)
		result := request(leaderAddr, x) //Calling backend
		if result == 1 {
			ctx.HTML("<p>Edit Complete<p>")
		} else {
			ctx.HTML("<p>Edit Failed. Item name exist or item name empty<p>")
		}
		ctx.HTML("<a href='/'>Return to main page<a>")
	})

	// Method:   POST
	// Resource: http://localhost:8080/delete
	app.Post("/delete", func(ctx iris.Context) {
		form := ctx.FormValues()
		x := Request{From: "Frontend", Name: "delete", Params: []string{form["item"][0]}}
		result := request(leaderAddr, x)
		if result == 1 {
			ctx.HTML("<p>Delete Complete<p>")
		} else {
			ctx.HTML("<p>Delete Failed.<p>")
		}
		ctx.HTML("<a href='/'>Return to main page<a>")
	})
	app.Run(iris.Addr(*listenPtr))
}
