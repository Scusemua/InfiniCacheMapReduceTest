package serverless

import (
	"github.com/ScottMansfield/nanolog"
	"github.com/mason-leap-lab/infinicache/client"
	"log"
	"os"
	"strconv"
)

// Debugging enabled?
const debugEnabled = true

//logCreate create the nanoLog
func logCreate(filename string) error {
	// Set up nanoLog writer
	path := filename + "_playback.clog"
	nanoLogout, err := os.Create(path)
	if err != nil {
		return err
	}
	err = nanolog.SetWriter(nanoLogout)
	if err != nil {
		return err
	}
	client.SetLogger(nanolog.Log)
	return nil
}

// DPrintf will only print if the debugEnabled const has been set to true
func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		log.Printf(format, a...)
	}
	return 0, nil
}

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		log.Printf("Error: %s\n", err)
		log.Fatal(err)
	}
}

// (Lab 2 only) jobPhase indicates whether a task is scheduled as a
// map or reduce task.
type jobPhase string

// (Lab 2 only) jobPhase indicates whether a task is scheduled as a
// map or reduce task.
const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// (Lab 2 only) KeyValue is a type used to hold the key/value pairs
// passed to the map and reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// (Lab 2 only) reduceName constructs the name of the intermediate
// file which map task <mapTask> produces for reduce task
// <reduceTask>.
func ReduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// (Lab 2 only) mergeName constructs the name of the output file of
// reduce task <reduceTask>
func MergeName(jobName string, reduceTask int) string {
	return "mr." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

// Maximum number of retry attempts during exponential backoff.
const MaxAttemptsDuringBackoff = 10

// Maximum amount of time workers/clients can sleep during an exponentially backed-off read.
const MaxBackoffSleepReads = 10000

// Maximum amount of time workers/clients can sleep during an exponentially backed-off write.
const MaxBackoffSleepWrites = 10000

var phaseServiceMap = map[jobPhase]string{
	mapPhase:    "m",
	reducePhase: "r",
}

// Make, for example, serviceName "wc", and phase "Map", into
// a valid service, like, for example, "wcm_service".
func ServiceName(service string, phase jobPhase) string {
	return service + phaseServiceMap[phase] + "_service"
}

type MapReduceArgs struct {
	JobName            string   // Name of the MapReduce job being executed.
	S3Key              string   // S3 key of data that needs to be downloaded for the job.
	TaskNum            int      // Identifier for the task.
	NReduce            int      // The number of Reducers. Used to partition keys to reducers during Map.
	NOthers            int      // The number of Mappers. Used in Reduce.
	SampleKeys         []string // Used to partition the keys to reducers during TeraSort such that they're relatively soretd.
	StorageIPs         []string // The hostnames (format "<IP>:<PORT>") of remote/intermediate storage.
	DataShards         int      // InfiniStore parameter.
	ParityShards       int      // InfiniStore parameter.
	MaxGoroutines      int      // InfiniStore parameter.
	ClientPoolCapacity int      // Maximum capacity of the pool of InfiniStore clients.
	Pattern            string   // Regex, used for Grep.
	UsePocket          bool     // Use Pocket for intermediate storage?
	PocketJobId        string   // The JobID of the Pocket job, if we're using pocket.
	ChunkThreshold     int      // Threshold above which we break intermediate data into ChunkThreshold-sized pieces for reading/writing to storage.
}
