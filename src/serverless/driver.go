//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Controls communication between client and workers. Controls phases
// of map-reduce excution through scheduler.
//
//////////////////////////////////////////////////////////////////////

package serverless

import (
    "fmt"
    "log"
    "net"
    "net/rpc"
    "sync"
)

// Driver holds all the state that the driver needs to keep track of.
type Driver struct {
    sync.Mutex

    address     string
    newCond     *sync.Cond
    doneChannel chan bool

    jobName string   // the job name of the MapReduce job
    inFiles []string // a list of input file names
    nReduce int      // number of reduce tasks

    shutdown chan struct{} // to shut down the driver's RPC server
    workers  []string      // a list of workers that get registered on driver
    l        net.Listener
}

// NewDriver initializes a new serverless driver
func NewDriver(address string) (drv *Driver) {
    drv = new(Driver)
    drv.address = address
    drv.newCond = sync.NewCond(drv)
    drv.doneChannel = make(chan bool)
    drv.shutdown = make(chan struct{})
    return
}

// Register is an RPC method that is called by workers after they
// have started up to report that they are ready to:
// 1) plugin new services;
// 2) receive and execute tasks on the already plugged-in services.
func (drv *Driver) Register(args *WorkerRegisterArgs, _ *struct{}) error {
    drv.Lock()
    defer drv.Unlock()

    drv.workers = append(drv.workers, args.WorkerAddr)
    drv.newCond.Broadcast()

    return nil
}

// Shutdown is an RPC method that shuts down the Driver's RPC server
func (drv *Driver) Shutdown(_, _ *struct{}) error {
    Debug("Shutdown: registration server\n")
    close(drv.shutdown)
    drv.l.Close()
    return nil
}

// startRPCServer starts the Driver's RPC server. It continues
// accepting RPC calls (Register worker in particular) for as long as
// the worker(s) are alive.
func (drv *Driver) startRPCServer() {
    Debug("Registration server starting\n")
    rpcs := rpc.NewServer()
    rpcs.Register(drv)
    l, e := net.Listen("tcp", drv.address)
    if e != nil {
        log.Fatal("Registration server ", drv.address, ": listen error: ", e)
    }
    drv.l = l

    go func() {
    loop:
        for {
            select {
            case <-drv.shutdown:
                break loop
            default:
            }
            conn, err := drv.l.Accept()
            if err == nil {
                go func() {
                    rpcs.ServeConn(conn)
                    conn.Close()
                }()
            } else {
                fmt.Printf("Registration server %s: accept error: %s\n", drv.address, err)
            }
        }
        Debug("Registration server done\n")
    }()
}

// stopRPCServer stops the Driver RPC server.
// This must be done through an RPC to avoid race conditions between
// the RPC server thread (goroutine) and the current thread
// (goroutine).
func (drv *Driver) stopRPCServer() {
    ok := Call(drv.address, "Driver.Shutdown", new(struct{}), new(struct{}))
    if ok == false {
        fmt.Printf("Driver cleanup: RPC %s error\n", drv.address)
    }
    Debug("cleanup Registration server: done\n")
}

// registerService constructs ServiceRegisterArgs and issues an RPC
// call to:
// the worker (specified as the 1st parameter worker) to register
// the new service (specified as the 2nd parameter serviceName).
// The 3rd parameter, registerChan, is used to keep track of the
// available workers, and to notify the driver of workers that have
// gone idle and are in need of new work.
func (drv *Driver) registerService(
    worker string,
    serviceName string,
    registerChan chan string,
) {
    Debug("Driver: to register new service: %v\n", serviceName)

    args := new(ServiceRegisterArgs)
    args.ServiceName = serviceName
    args.ApiName = "Interface"

    ok := Call(worker, "Worker.RegisterService", args, new(struct{}))
    if ok == true {
        fmt.Printf("Successfully registered worker %s\n", worker)
        go func() { registerChan <- worker }()
    } else {
        fmt.Printf("Failed to register worker %s\n", worker)
    }
}

// prepareService is provided for you.
// It enters a for loop over all registered workers to perform
// service registration by calling registerService.
func (drv *Driver) prepareService(ch chan string, serviceName string) {
    fmt.Printf("Driver: enter the worker registration service loop...\n")
    i := 0
    for {
        drv.Lock()
        if len(drv.workers) > i {
            w := drv.workers[i]
            go drv.registerService(w, serviceName, ch)
            i = i + 1
        } else {
            drv.newCond.Wait()
        }
        drv.Unlock()
    }
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (drv *Driver) Wait() {
    <-drv.doneChannel
    Debug("Driver: done signal captured\n")
}

// run executes tasks.
//
// First, it registers the compiled plugin service library at the
// remote worker side.  Second, it schedules tasks (the just
// registered plugin service) on remote workers.  Last, it wraps up
// by killing remote workers and shut down itself.
//
// A MapReduce job consists of multiple phases: a Map phase, a Reduce
// phase, and possibly a final Merge phase.
// finish() wraps over the job and shutdown the RPC servers of the
// workers and driver process.
func (drv *Driver) run(
    jobName string,
    inFiles []string,
    nReduce int,
    schedule func(phase jobPhase, serviceName string),
    finish func(),
) {
    drv.jobName = jobName
    drv.inFiles = inFiles
    drv.nReduce = nReduce

    fmt.Printf("%s: Starting MapReduce job: %s\n", drv.address, jobName)

    // E.g., for word count, the name of the map plugin service
    // module would be 'wcm_service'; for inverted indexing, the name
    // would be 'iim_service'.
    fmt.Printf("%s: To start the Map phase...\n", drv.address)
    schedule(mapPhase, jobName)

    // E.g., for word count, the name of the reduce plugin service
    // module would be 'wcr_service'; for inverted indexing, the name
    // would be 'iir_service'.
    fmt.Printf("%s: To start he Reduce phase...\n", drv.address)
    schedule(reducePhase, jobName)
    finish()
    drv.merge()

    drv.doneChannel <- true
}

// Run is a function exposed to client.
// Run calls the internal call `run` to register plugin services and
// schedule tasks with workers over RPC.
func (drv *Driver) Run(jobName string, inFiles []string, nReduce int) {
    Debug("%s: Starting driver RPC server\n", drv.address)
    drv.startRPCServer()

    go drv.run(jobName, inFiles, nReduce,
        func(phase jobPhase, serviceName string) { // func schedule()
            registerChan := make(chan string)
            go drv.prepareService(registerChan, ServiceName(serviceName, phase))
            drv.schedule(phase, serviceName, registerChan)
        },
        func() { // func finish()
            drv.killWorkers()
            drv.stopRPCServer()
        })
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
func (drv *Driver) killWorkers() {
    drv.Lock()
    defer drv.Unlock()
    for _, w := range drv.workers {
        Debug("Driver: shutdown worker %s\n", w)
        ok := Call(w, "Worker.Shutdown", new(struct{}), new(struct{}))
        if ok == false {
            fmt.Printf("Driver: RPC %s shutdown error\n", w)
        } else {
            fmt.Printf("Driver: RPC %s shutdown gracefully\n", w)
        }
    }
}
