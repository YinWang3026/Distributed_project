package main

import (
	"encoding/json" // Marshal, unmarshal, encoding, decoding,
	"flag"          // Cmd line argument parsing
	"fmt"
	"html/template" // Templates
	"log"           // log.Fatal()
	"net"           // TCP
	"net/http"      // Http request handlers
	"os"
	"strings" // Split
	"time"    // Sleep, Seconds
)

// Request obj
type Request struct {
	From   string `json:"from"`
	Name   string `json:"name"`
	Key    string `json:"key"`
	Value  string `json:"value"`
	OldKey string `json:"oldkey"`
}

// For Response Template
type resTempObj struct {
	Action  string
	Success bool
}

func (obj *resTempObj) setSuccess(x int) {
	if x == 1 {
		obj.Success = true
	} else {
		obj.Success = false
	}
}

// Globals
const (
	aliveTimer int    = 5 // In seconds
	resTemp    string = "responseTemplate.html"
	indexTemp  string = "indexTemplate.html"
)

var (
	nodeAddrList []string // List of node addresses
	leaderAddr   string   // Address of leader node
	templates    = template.Must(template.ParseFiles("../static/"+resTemp, "../static/"+indexTemp))
)

// msgNode - sends request to given address, returns success/fail
func msgNode(address string, req Request) int {
	// Form connection
	conn, err := net.Dial("tcp", address)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Failed to dial:", address)
		return 0
	}
	defer conn.Close()
	// Encodng and sending message
	err = json.NewEncoder(conn).Encode(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "msgNode: Encoding error, Request Failed:", req.Name)
		return 0
	}
	// Decoding and recieving message
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
		fmt.Println("Leader addr", leaderAddr)
		result := msgNode(leaderAddr, req) // Dialing
		if result != 1 {
			// Leader fails
			fmt.Printf("Detected failure on leaderAddr [%q] at [%q]\n", leaderAddr, time.Now().UTC().String())
			// Find new leader
			req2 := Request{From: "web", Name: "leader"}
			for _, addr := range nodeAddrList { // Loops through backends and asks are you leader
				result := msgNode(addr, req2)
				if result == 1 {
					leaderAddr = addr
				}
			}
		}
		time.Sleep(time.Duration(aliveTimer) * time.Second) // Dialing once every 3 seconds
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
	// Execute template
	t := struct {
		Leader  string
		ItemMap map[string]string
	}{
		leaderAddr,
		itemMap,
	}
	err = templates.ExecuteTemplate(w, indexTemp, t)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func renderResponseTemplate(w http.ResponseWriter, action string, result int) {
	// Creating template obj
	var t resTempObj
	t.Action = action
	t.setSuccess(result)
	// Execute template
	err := templates.ExecuteTemplate(w, resTemp, t) //w, file name (not the path), object
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// Method:   POST
// Resource: http://localhost:8080/add
func addHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "add", Key: r.FormValue("key"), Value: r.FormValue("value")}
	result := msgNode(leaderAddr, req)
	fmt.Println(req)
	// Template response
	renderResponseTemplate(w, "Add", result)
}

// Method:   POST
// Resource: http://localhost:8080/delete
func deleteHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "delete", Key: r.FormValue("key")}
	result := msgNode(leaderAddr, req)
	fmt.Println(req)
	// Template response
	renderResponseTemplate(w, "Delete", result)
}

// Method:   Post
// Resource: http://localhost:8080/update
func updateHandler(w http.ResponseWriter, r *http.Request) {
	req := Request{From: "web", Name: "update", Key: r.FormValue("key"), Value: r.FormValue("value"), OldKey: r.FormValue("oldkey")}
	fmt.Println(req)
	result := msgNode(leaderAddr, req)
	// Template response
	renderResponseTemplate(w, "Update", result)
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
	http.HandleFunc("/delete", deleteHandler)
	http.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("../static/"))))

	log.Fatal(http.ListenAndServe(*listenPtr, nil))
}
