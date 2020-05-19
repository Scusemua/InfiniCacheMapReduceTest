//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Worker registers with driver and receives requests to do work.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"log"
	"net"
	"net/rpc"
	"os"
	"plugin"
	"strconv"
	"sync"
)

// Worker holds the state for a server waiting for:
// 1) RegisterService,
// 2) InvokeService,
// 3) Shutdown RPCs.
type Worker struct {
	sync.Mutex

	address    string // address of this worker process
	masterAddr string // address of the driver process
	nThreads   int    // misc. statistics (usage not required)
	nRPC       int    // number of RPC requests to serve before the worker exists
	nTasks     int    // misc. statistics (usage not required)
	concurrent int    // misc. statistics (usage not required)
	l          net.Listener

	shutdown chan struct{}
}

// Service struct maintains the state of a plugin Service.
type Service struct {
	pluginDir string
	name      string
	interf    serverless.Interface
}

// serviceMap is a global map that keeps track of all registered plugin Services.
// You should insert the newly registered service into this map.
var serviceMap = make(map[string]*Service)

// newService initializes a new plugin Service.
func newService(serviceName string) *Service {
	return &Service{
		pluginDir: serverless.PluginDir,
		name:      serviceName,
		interf:    nil,
	}
}

// Finds and opens the plugin.

func (svc *Service) openPlugin() error {
	plug, err := plugin.Open(svc.pluginDir + "/" + svc.name + ".so")

	if err != nil {
		serverless.Debug("Failed to open plugin %s: %v\n", svc.name, err)
		return err
	}
	serverless.Debug("Opened plugin %s\n", svc.name)

	symInterface, err := plug.Lookup("Interface")
	if err != nil {
		serverless.Debug("Error looking up symbol 'Interface' in %s: %v\n", svc.name, err)
		return err
	}
	serverless.Debug("Successfully looked up Interface %s\n", svc.name)

	var interf serverless.Interface
	interf, ok := symInterface.(serverless.Interface)
	if !ok {
		serverless.Debug("Unable to load Interface from %s.\n", svc.name)
		return fmt.Errorf("Unable to load type %s", svc.name)
	}

	svc.interf = interf

	return nil
}

// RegisterService is caled by the driver to plugin a new service that has already been
// compiled into a .so static object library.
func (wk *Worker) RegisterService(args *serverless.ServiceRegisterArgs, _ *struct{}) error {
	serverless.Debug("Register called for %s\n", args.ServiceName)

	service := newService(args.ServiceName)
	err := service.openPlugin()
	if err != nil {
		return err
	}
	serviceMap[args.ServiceName] = service

	serverless.Debug("Successfully registered new service %s\n", args.ServiceName)
	return nil
}

// InvokeService is called by the driver (schedule) when a new task
// is being scheduled on this worker.
func (wk *Worker) InvokeService(args serverless.RPCArgs, _ *struct{}) error {
	serverless.Debug("worker InvokeService: %s\n", args.Name)

	svc, ok := serviceMap[args.Name]
	if !ok {
		msg := fmt.Sprintf("Unknown service in call to InvokeService: %s\n", args.Name)
		serverless.Debug(msg)
		return fmt.Errorf(msg)
	}
	svc.interf.DoService(args.Args)

	return nil
}

// Shutdown is called by the driver when all work has been completed.
// No response needed.
func (wk *Worker) Shutdown(_ *struct{}, _ *struct{}) error {
	serverless.Debug("Worker shutdown %s\n", wk.address)
	close(wk.shutdown)
	wk.l.Close()
	return nil
}

// Tell the driver I exist and ready to work:
// register is the internal function that calls the RPC method of Driver.Register
// at the remote driver to register the worker itself.
func (wk *Worker) register(driver string) {
	args := new(serverless.WorkerRegisterArgs)
	args.WorkerAddr = wk.address

	ok := serverless.Call(driver, "Driver.Register", args, new(struct{}))
	if ok == true {
		fmt.Printf("Successfully registered worker %s\n", wk.address)
	} else {
		fmt.Printf("Failed to register worker %s\n", wk.address)
	}
}

// startRPCServer sets up a connection with the driver, registers its address,
// and waits for any of the following two events:
// 1) plugin Services to be registered,
// 2) tasks to be scheduled.
func (wk *Worker) startRPCServer() {
	rpcs := rpc.NewServer()
	rpcs.Register(wk)
	l, err := net.Listen("tcp", wk.address)
	if err != nil {
		log.Fatal("Worker: worker listen error: ", err)
	}
	wk.l = l
	defer wk.l.Close()
	wk.register(wk.masterAddr)

	// DON'T MODIFY CODE BELOW
	serverless.Debug("Worker: %v To start the RPC server...\n", wk.address)
loop:
	for {
		select {
		case <-wk.shutdown:
			break loop
		default:
		}
		//fmt.Println("Worker", wk.address, "attempting to acquire worker lock...")
		wk.Lock()
		//fmt.Println("Worker", wk.address, "successfully acquired worker lock.")
		if wk.nRPC == 0 {
			fmt.Println("Worker reached maximum number of RPC messages.")
			wk.Unlock()
			break
		}
		wk.Unlock()
		conn, err := wk.l.Accept()
		if err == nil {
			fmt.Println("Worker at address", wk.address, "received RPC connection...")
			wk.Lock()
			wk.nRPC--
			wk.Unlock()
			go rpcs.ServeConn(conn)
			wk.Lock()
			wk.nTasks++
			wk.Unlock()
		} else {
			break
		}
	}
	serverless.Debug("Worker: %v RPC server exist\n", wk.address)
}

// The main entrance of worker.go
func main() {
	wk := new(Worker)
	wk.address = os.Args[1]               // the 1st cmd-line argument: worker hostname and ip addr
	wk.masterAddr = os.Args[2]            // the 2nd cmd-line argument: driver hostname and ip addr
	nRPC, err := strconv.Atoi(os.Args[3]) // the 3rd cmd-line argument: number of RPC requests
	if err != nil {
		log.Fatal("strconv.Atoi failed")
	}
	wk.nRPC = nRPC
	wk.shutdown = make(chan struct{})
	wk.nTasks = 0

	wk.startRPCServer()
}
