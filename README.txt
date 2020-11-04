go run frontend.go --listen :8080
go run frontend.go --listen :8081

go run backend.go --listen :8090 --backend :8091,:8092
go run backend.go --listen :8091 --backend :8090,:8092
go run backend.go --listen :8092 --backend :8090,:8091
These commands should have the web servers running on localhost:8080 and :8081

Command line arguments for frontend
--listen addr:port
--backend addr:port,addr:port...
If no argument provided
Default listen :8080
Default backend :8090,:8091,:8092

Command line arguments for backend
--listen addr:port
--backend addr:port,addr:port...
Must provide arguments

Need colon for the ports. (":port")
If addr is empty, default localhost

Add - Adds a new item to the list
Edit - Edits the item by giving it a new name or new description
Delete - Removes the item from list
For add/edit, cannot use a name that already exist

On starting web server, frontend.go, there might be error messages, because the distributed server, backend.go, leader address is set to "0000" as default.
Then the web server corrects that address by asking the distributed server who the leader is.
