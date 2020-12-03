package serverless

import (
	"log"
	"net/rpc"
)

// What follows are RPC types and methods.
// Field names must start with capital letters, otherwise RPC will
// break.

// WorkerRegisterArgs holds the argument passed when a worker
// registers with the driver.
type WorkerRegisterArgs struct {
	WorkerAddr string
}

// ServiceRegisterArgs holds the arguments passed to a worker when
// the driver registers a new plugin Service.
type ServiceRegisterArgs struct {
	ServiceName string // the name of the service
	ApiName     string // the name of the interface exposed by the plugin
}

// RPCArgs holds the arguments passed to a worker when the driver invokes a
// Service.
type RPCArgs struct {
	Name string // the name of the service
	Args []byte // the raw []byte array that holds the plugin arguments, serialized by the driver
}

// Call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// Call() returns true if the server responded, and false
// if Call() was not able to contact the server. in particular,
// reply's contents are valid if and only if Call() returned true.
//
// You should assume that Call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use Call() to send all RPCs, in driver.go, worker.go,
// and schedule.go.
// Please don't change this function.
//
func Call(srv string, rpcname string, args interface{}, reply interface{}) bool {
	log.Printf("Dialing %s for RPC call for function %s now...\n", srv, rpcname)
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	log.Printf("Making RPC call for function %s to remote %s now...\n", rpcname, srv)
	err := c.Call(rpcname, args, reply)
	if err == nil {
		log.Printf("RPC call for function %s to remote %s was SUCCESSFUL.\n", rpcname, srv)
		return true
	}

	log.Printf("RPC call for function %s to remote %s FAILED: %s\n", rpcname, srv, err)
	return false
}

// The following const defines the generic plugin API together with
// the default plugins dir.
const (
	ServiceSymbolName = "Interface"
	PluginDir         = "../plugins"
)

// type Interface defines the generic API exposed by a user-defined
// plugin Service library.
type Interface interface {
	DoService(args []byte) error
	ClosePool() error 
}
