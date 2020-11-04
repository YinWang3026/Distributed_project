package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"time"
	"strings"
	"github.com/kataras/iris"
)

//Items object export for JSON encoding/decoding
type Items struct {
	Name string
	Desc string
}

//Instruction obj export
type Instruction struct {
	From string
	Name   string
	Params []string
}

func request(dial string, inst Instruction) int {
	//Form connection
	conn, err := net.Dial("tcp", dial)
	if err != nil {
		fmt.Println("Connection Error, Instruction Failed:", inst.Name)
		return 0 //Failed
	}
	defer conn.Close()
	//Encodng and sending message
	encoder := json.NewEncoder(conn)
	encoder.Encode(inst)
	//Decoding and recieving message
	decoder := json.NewDecoder(conn)
	var x int
	derr := decoder.Decode(&x)
	if derr != nil {
		fmt.Println("Decoding error, Instruction Failed:", inst.Name)
		return 0 //Failed
	}
	return x //1 for success, 0 for fail
}

func alive(backendAddr []string, leaderAddr *string) {
	x := Instruction{From: "Frontend", Name: "leader", Params: []string{""}}
	for {
		fmt.Println("Leader addr:", *leaderAddr)
		result := request(*leaderAddr, x) //Dialing
		if result == 1 {
			//fmt.Println("Connection Ok")
			time.Sleep(1 * time.Second) //Dialing once every 3 seconds
		} else {
			fmt.Println("Detected failure on localhost", *leaderAddr, "at", time.Now().UTC()) //Leader fails
			findingLeader(backendAddr, leaderAddr) //Find new leader
			time.Sleep(1 * time.Second)
		}
	}
}

func findingLeader(backendAddr []string, leaderAddr *string){
	x := Instruction{From: "Frontend", Name: "leader", Params: []string{""}}
	for _,val := range backendAddr { //Loops through backends and asks are you leader
		result := request(val,x)
		if result == 1 {
			*leaderAddr = val
		}
	}
}


func main() {
	//Getting command line arguments
	listenPtr := flag.String("listen", ":8080", "listen address")
	dialPtr := flag.String("backend", ":8090,:8091,:8092", "dial address")
	flag.Parse()
	backendAddr := strings.Split(*dialPtr,",")
	leaderAddr := "0000"

	//Finding backend leader and setting leaderAddr to it.
	go alive(backendAddr,&leaderAddr)
	
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
		x := Instruction{From: "Frontend", Name: "itemMap", Params: []string{""}}
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
			ctx.HTML("<form method='post'><input type='text' name='oriItem' style='display:none;' readonly value=" + key + "><input type='text' name='item' value="+ key + "><input type='text' name='desc' value=" + val + ">")
			ctx.HTML("<input type='submit' formaction='/edit' value='Edit'><input type='submit' formaction='/delete' value='Delete'></form>")
			//ctx.WriteString("<pre>Item: " + key + "	Description: " + val + "	<a href='/edit/{" + key + "}/{" + val + "}'>Edit</a>	<a href='/delete/{" + key + "}'>Delete</a></pre>")
		}
	})

	// Method:   POST
	// Resource: http://localhost:8080/add
	app.Post("/add", func(ctx iris.Context) {
		form := ctx.FormValues()	//Reading form values
		x := Instruction{From: "Frontend", Name: "add", Params: []string{form["item"][0], form["desc"][0]}} //Making instruction
		result := request(leaderAddr, x) 	//Calling backend
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
		form := ctx.FormValues()	//Reading form values
		x := Instruction{From: "Frontend", Name: "update", Params: []string{form["oriItem"][0],form["item"][0], form["desc"][0]}} //Making instruction
		fmt.Println(x)
		result := request(leaderAddr, x) 	//Calling backend
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
		x := Instruction{From: "Frontend", Name: "delete", Params: []string{form["item"][0]}}
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
