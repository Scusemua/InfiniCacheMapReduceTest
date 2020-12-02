//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Schedules jobs to be executed on workers.
//
//////////////////////////////////////////////////////////////////////

package serverless

import (
	"bytes"
	"encoding/gob"
	"log"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (drv *Driver) schedule(
	phase jobPhase,
	serviceName string,
	registerChan chan string,
	dataShards int,
	parityShards int,
	maxGoRoutines int,
	pattern string,
) {
	Debug("Driver: schedule %s\n", phase)
	// MapReduceArgs defines the format of your MapReduce service plugins.
	// type MapReduceArgs struct {
	// 	JobName       string
	// 	S3Key         string
	// 	TaskNum       int
	// 	NReduce       int
	// 	NOthers       int
	// 	SampleKeys    []string
	// 	StorageIPs    []string
	// 	DataShards    int
	// 	ParityShards  int
	// 	MaxGoroutines int
	// 	Pattern 	  string 
	// }

	// Make, for example, serviceName "wc", and phase "Map", into
	// a valid service, like, for example, "wcm_service".
	pluginName := ServiceName(serviceName, phase)

	// The jobChan is a queue of jobs to be exected. Allows failed jobs to
	// be requeued by invokeService when they fail
	var nTasks int
	var jobChan chan *MapReduceArgs
	Debug("Driver: Creating jobs. nTasks: %d\n", len(drv.s3Keys))
	if phase == mapPhase {
		nTasks = len(drv.s3Keys)
		jobChan = make(chan *MapReduceArgs, nTasks)
		for i, fileName := range drv.s3Keys {
			arg := new(MapReduceArgs)
			arg.TaskNum = i
			arg.JobName = serviceName
			arg.S3Key = string(fileName)
			//arg.RedisEndpoints = drv.redisHostnames
			arg.StorageIPs = drv.storageIps
			arg.NReduce = drv.nReduce
			arg.SampleKeys = drv.sampleKeys
			arg.DataShards = dataShards
			arg.ParityShards = parityShards
			arg.MaxGoroutines = maxGoRoutines
			arg.Pattern = pattern
			jobChan <- arg
		}
	} else {
		nTasks = drv.nReduce
		jobChan = make(chan *MapReduceArgs, nTasks)
		for i := 0; i < nTasks; i++ {
			arg := new(MapReduceArgs)
			arg.TaskNum = i
			arg.JobName = serviceName
			arg.NReduce = drv.nReduce
			//arg.RedisEndpoints = drv.redisHostnames
			arg.NOthers = len(drv.s3Keys)
			arg.StorageIPs = drv.storageIps
			arg.SampleKeys = drv.sampleKeys
			arg.DataShards = dataShards
			arg.ParityShards = parityShards
			arg.MaxGoroutines = maxGoRoutines
			arg.Pattern = pattern
			jobChan <- arg
		}
	}
	Debug("Driver: Creating jobs\n")

	// readyChan is a bounded buffer that is used to notify the
	// scheduler of workers that are *TRULY* ready for executing the
	// service tasks.
	readyChan := make(chan string, nTasks)

	// Complete chan allow main thread to know
	// when all workers have finished.
	completeChan := make(chan bool, nTasks)

	// invokeService is a goroutine that is used to call the RPC
	// method of Worker.InvokeService at the worker side.
	invokeService := func(worker string, args *MapReduceArgs) {
		var buf bytes.Buffer

		log.Printf("Schedule: scheduling %s task #%d onto worker %s now...", phase, args.TaskNum, worker)

		//log.Println("Schedule: scheduling task", args.TaskNum, "on worker", worker, "now...")

		enc := gob.NewEncoder(&buf)
		err := enc.Encode(args)
		checkError(err)

		rpc_args := new(RPCArgs)
		rpc_args.Name = pluginName
		rpc_args.Args = buf.Bytes()

		success := Call(worker, "Worker.InvokeService", rpc_args, new(struct{}))

		// If success, then put the worker back in the queue, and report the task done.
		if success {
			// Notify the scheduler that this worker is back to ready state.
			readyChan <- worker

			// Notify scheduler that this task is complete.
			completeChan <- true

			log.Printf("Schedule: %s task #%d executed successfully on worker %v.\n", phase, args.TaskNum, worker)
		} else {
			// Job failed, so put job back in queue to be executed
			jobChan <- args
			log.Printf("Schedule: %s task #%d FAILED to execute by %v: %s task #%v\n", phase, args.TaskNum, worker, phase, args.TaskNum)
		}
	}

	// Get new workers, or ones ready for
	// more work, and assign tasks to them.
	runner := func() {
		Debug("Driver: Job runner waiting for jobs\n")
		// wait for a job
		for arg := range jobChan {
			Debug("Driver: Job runner got job\n")
			var worker string
			// assign
			select {
			case worker = <-registerChan:
				go invokeService(worker, arg)
			case worker = <-readyChan:
				go invokeService(worker, arg)
			}
		}
	}

	Debug("Driver: starting runner\n")
	go runner()

	// Wait for all tasks to complete before returning
	Debug("Driver: Waiting for tasks to be complete\n")
	for i := 0; i < nTasks; i++ {
		<-completeChan
	}

	// Work done: finish the task scheduling
	Debug("Driver: Task scheduling done\n")
}
