# Distributed Project

## Background
Data is often stored across multiple nodes/devices to provide consistency, availability, and partition tolerance. There are a few famous algorithms that is used to synchronize data across nodes. In this project, the RAFT consensus algorithm is implemented across multiple nodes to maximize data consistency and availability.
## Running the Program
### Setting Up the Webserver
- Command line arguments for frontend web server
- --listen addr:port
- --backend addr:port,addr:port...
- If no argument provided, then default listen :8080 and default backend :8090,:8091,:8092

These commands should have two web servers running on localhost:8080 and :8081
- cd /webserv
- go run main.go --listen :8080
- go run main.go --listen :8081 

### Setting Up the Nodes
- Command line arguments for the nodes
- --listen addr:port
- --backend addr:port,addr:port...
- Must provide arguments

These commands should have the nodes running on localhost:8090, :8091, and :8092
- cd /nodes
- go run main.go --listen :8090 --nodes :8091,:8092
- go run main.go --listen :8091 --nodes :8090,:8092
- go run main.go --listen :8092 --nodes :8090,:8091

## Misc
- Add - Adds a new item to the list
- Update - Edits the item by giving it a new name or new description
- Delete - Removes the item from list
- For add/edit, cannot use a name that already exist

Need colon for the ports. (":port"), and if the addr is empty, default is localhost.

On starting web server, there might be error messages, because the nodes have not elected a leader, the leader address is set to "0000" as default. Then the web server corrects that address by asking the nodes who the leader is.
