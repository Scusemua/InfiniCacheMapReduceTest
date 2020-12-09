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
	"bufio"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	//"github.com/Scusemua/PythonGoBridge"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// Driver holds all the state that the driver needs to keep track of.
type Driver struct {
	sync.Mutex

	address     string
	newCond     *sync.Cond
	doneChannel chan bool

	jobName    string   // the job name of the MapReduce job
	s3Keys     []string // a list of input file names
	nReduce    int      // number of reduce tasks
	sampleKeys []string // sample keys used for constructing a trie for TeraSort
	storageIps []string // The IP addresses of one or more proxies.

	shutdown chan struct{} // to shut down the driver's RPC server
	workers  []string      // a list of workers that get registered on driver
	l        net.Listener
}

// Retrieve sample data from S3 for TeraGen phase of TeraSort.
func getSampleKeys(sampleFileS3Key string, nReduce int) []string {
	var err error
	var b []byte
	var stepSize int

	var s3KeyFile *os.File
	var sampleKeys []string

	if _, err := os.Stat(sampleFileS3Key); os.IsNotExist(err) {
		log.Printf("Sample file \"%s\" does not exist locally. Downloading it from S3 now...\n", sampleFileS3Key)

		// The session the S3 Downloader will use
		sess := session.Must(session.NewSession(&aws.Config{
			Region: aws.String("us-east-1")},
		))

		// Create a downloader with the session and default options
		downloader := s3manager.NewDownloader(sess)

		// Create a file to write the S3 Object contents to.
		s3KeyFile, err = os.Create(sampleFileS3Key)
		checkError(err)

		now := time.Now()

		// Write the contents of S3 Object to the file
		n, err := downloader.Download(s3KeyFile, &s3.GetObjectInput{
			Bucket: aws.String("infinistore-mapreduce"),
			Key:    aws.String(sampleFileS3Key),
		})
		checkError(err)

		downloadDuration := time.Since(now)

		log.Printf("File %s downloaded, %d bytes, time elapsed = %d ms\n", sampleFileS3Key, n, downloadDuration)
	} else {
		log.Printf("Sample file \"%s\" DOES exist locally.", sampleFileS3Key)
		s3KeyFile, err = os.Open(sampleFileS3Key)
		checkError(err)
	}

	log.Println("Driver is generating sample keys now...")
	log.Println("Driver is reading data from file", sampleFileS3Key, "to generate the sample keys.")

	b, err = ioutil.ReadFile(sampleFileS3Key)
	checkError(err)

	var arr []string
	// Split up string line-by-line.
	for _, s := range strings.FieldsFunc(string(b), func(r rune) bool {
		if r == '\n' {
			return true
		}
		return false
	}) {
		sep := strings.Split(s, "\n")
		arr = append(arr, sep[0])
	}

	sort.Strings(arr)

	stepSize = len(arr) / nReduce
	for i := 0; i < len(arr); i += stepSize {
		sampleKeys = append(sampleKeys, arr[i])
	}

	log.Println("Driver successfully generated", len(arr), "sample keys.")

	return sampleKeys
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
	log.Println("Driver received worker registration from worker at address:", args.WorkerAddr)
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
				log.Printf("Registration server %s: accept error: %s\n", drv.address, err)
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
		log.Printf("Driver cleanup: RPC %s error\n", drv.address)
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
		log.Printf("Successfully registered worker %s\n", worker)
		go func() { registerChan <- worker }()
	} else {
		log.Printf("Failed to register worker %s\n", worker)
	}
}

// prepareService is provided for you.
// It enters a for loop over all registered workers to perform
// service registration by calling registerService.
func (drv *Driver) prepareService(ch chan string, serviceName string) {
	log.Printf("Driver: enter the worker registration service loop...\n")
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
// First, it registeFrs the compiled plugin service library at the
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
	s3Keys []string,
	nReduce int,
	sampleKeys []string,
	dataShards int,
	parityShards int,
	maxGoRoutines int,
	storageIps []string,
	usePocket bool,
	schedule func(phase jobPhase, serviceName string),
	finish func(),
) {
	drv.jobName = jobName
	drv.s3Keys = s3Keys
	drv.nReduce = nReduce
	drv.sampleKeys = sampleKeys
	//drv.redisHostnames = redisHostnames
	drv.storageIps = storageIps

	jobStartTime := time.Now()
	log.Println("JOB START: ", jobStartTime.Format("2006-01-02 15:04:05:.99999"))

	log.Printf("%s: Starting MapReduce job: %s\n", drv.address, jobName)

	// E.g., for word count, the name of the map plugin service
	// module would be 'wcm_service'; for inverted indexing, the name
	// would be 'iim_service'.
	log.Printf("%s: To start the Map phase...\n", drv.address)
	schedule(mapPhase, jobName)

	endOfMap := time.Now()
	mapPhaseDuration := time.Since(jobStartTime)

	log.Printf("Map phase duration: %d ms\n", mapPhaseDuration/1e6)

	// E.g., for word count, the name of the reduce plugin service
	// module would be 'wcr_service'; for inverted indexing, the name
	// would be 'iir_service'.
	log.Printf("%s: To start the Reduce phase...\n", drv.address)
	schedule(reducePhase, jobName)

	reducePhaseDuration := time.Since(endOfMap)
	mapReduceDuration := time.Since(jobStartTime)

	log.Printf("Map phase duration: %d ms\n", mapPhaseDuration/1e6)
	log.Printf("Reduce phase duration: %d ms", reducePhaseDuration/1e6)
	log.Printf("DURATION OF MAP PHASE + REDUCE PHASE: %d ms", mapReduceDuration/1e6)

	finish()

	log.Printf("Map phase duration: %d ms\n", mapPhaseDuration/1e6)
	log.Printf("Reduce phase duration: %d ms", reducePhaseDuration/1e6)
	log.Printf("DURATION OF MAP PHASE + REDUCE PHASE: %d ms", mapReduceDuration/1e6)

	startOfMerge := time.Now()
	drv.merge(storageIps, dataShards, parityShards, maxGoRoutines, usePocket)
	mergePhaseDuration := time.Since(startOfMerge)

	jobEndTime := time.Now()
	jobDuration := time.Since(jobStartTime)

	log.Println("JOB END: ", jobEndTime.Format("2006-01-02 15:04:05:.99999"))

	log.Printf("Map phase duration: %d ms\n", mapPhaseDuration/1e6)
	log.Printf("Reduce phase duration: %d ms", reducePhaseDuration/1e6)
	log.Printf("Merge phase duration: %d ms", mergePhaseDuration/1e6)
	log.Printf("DURATION OF MAP PHASE + REDUCE PHASE: %d ms", mapReduceDuration/1e6)
	log.Printf("Job Duration: %d ms\n", jobDuration/1e6)

	drv.doneChannel <- true
}

// Run is a function exposed to client.
// Run calls the internal call `run` to register plugin services and
// schedule tasks with workers over RPC.
func (drv *Driver) Run(
	jobName string,
	s3KeyFile string,
	sampleFileS3Key string,
	nReduce int,
	dataShards int,
	parityShards int,
	maxGoRoutines int,
	pattern string,
	clientPoolCapacity int,
	chunkThreshold int,
	usePocket bool,
	numLambdasPocket int,
	capacityGbPocket int,
	peakMbpsPocket int,
	latencySensitivePocket int,
	storageIps []string,
) {
	Debug("%s: Starting driver RPC server\n", drv.address)
	drv.startRPCServer()

	// ======================================
	// Get the list of S3 Akeys from the file.
	// ======================================
	file, err := os.Open(s3KeyFile)
	checkError(err)

	defer file.Close()

	scanner := bufio.NewScanner(file)
	var s3Keys []string

	// Read in all the S3 keys from the files.
	for scanner.Scan() {
		txt := scanner.Text()
		log.Printf("Read S3 key from file: \"%s\"\n", txt)
		s3Keys = append(s3Keys, txt)
	}

	// ======================================
	// Get list of Redis endpoints from file.
	// ======================================
	// file2, err2 := os.Open("/home/ubuntu/project/src/InfiniCacheMapReduceTest/main/redis_hosts.txt")
	// file2, err2 := os.Open("/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main/redis_hosts.txt")
	// checkError(err2)

	// defer file2.Close()

	// scanner2 := bufio.NewScanner(file2)
	// var redisHostnames []string

	// // Read in all the Redis endpoints from the files.
	// for scanner2.Scan() {
	// 	txt := scanner2.Text()
	// 	log.Printf("Read Redis endpoint from file: \"%s\"\n", txt)
	// 	redisHostnames = append(redisHostnames, txt)
	// }

	log.Printf("About to generate sample keys...")

	start := time.Now()
	sampleKeys := getSampleKeys(sampleFileS3Key, nReduce)
	end := time.Now()
	elapsed := end.Sub(start)

	log.Printf("Driver generating sample keys took %d ms.", elapsed)
	log.Printf("Sample keys: %s\n", strings.Join(sampleKeys, ","))

	log.Printf("Number of S3 keys: %d\n", len(s3Keys))

	pocketJobId := "N/A"
	// if usePocket {
	// 	log.Printf("Registering job with Pocket now...\n")
	// 	pocketJobId = PythonGoBridge.RegisterJob("", numLambdasPocket, capacityGbPocket, peakMbpsPocket, latencySensitivePocket)
	// }

	go drv.run(jobName, s3Keys, nReduce, sampleKeys, dataShards, parityShards, maxGoRoutines, storageIps, usePocket,
		func(phase jobPhase, serviceName string) { // func schedule()
			registerChan := make(chan string)
			go drv.prepareService(registerChan, ServiceName(serviceName, phase))
			drv.schedule(phase, serviceName, registerChan, dataShards, parityShards, maxGoRoutines,
				clientPoolCapacity, pattern, chunkThreshold, usePocket, pocketJobId)
		},
		func() { // func finish()
			log.Printf("Driver executing finish() function now. First, killing workers.\n")
			drv.killWorkers()
			log.Printf("Workers killed. Next, stopping RPC server.\n")
			drv.stopRPCServer()
			log.Printf("RPC server stopped. Next, deregistering job from Pocket.\n")
			res := PythonGoBridge.DeregisterJob(pocketJobId)
			if res == 0 {
				log.Printf("Job deregistered successfully.\n")
			} else {
				log.Printf("[ERROR] Job failed to deregister successfully.\n")
			}
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
			log.Printf("Driver: RPC %s shutdown error\n", w)
		} else {
			log.Printf("Driver: RPC %s shutdown gracefully\n", w)
		}
	}
}
