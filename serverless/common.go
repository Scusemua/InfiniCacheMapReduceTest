package serverless

import (
	"fmt"
	"log"
	"strconv"
)

// Debugging enabled?
const debugEnabled = true

// DPrintf will only print if the debugEnabled const has been set to true
func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		fmt.Printf("Error: %s\n", err)
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

var phaseServiceMap = map[jobPhase]string{
	mapPhase:    "m",
	reducePhase: "r",
}

// Make, for example, serviceName "wc", and phase "Map", into
// a valid service, like, for example, "wcm_service".
func ServiceName(service string, phase jobPhase) string {
	return service + phaseServiceMap[phase] + "_service"
}
