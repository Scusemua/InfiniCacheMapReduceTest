//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Performs receives input and service. Calls Driver to schedule
// execution on workers.
//
//////////////////////////////////////////////////////////////////////

package main

import (
    "cs675-spring20-labs/lab2/serverless"
    "log"
    "os"
    "strconv"
)

// The main entrance of client.go.
//
// The client can be run in the following way for example:
// go run client.go localhost:1234 wc pg-*.txt
// where localhost:1234 is the driver (client)'s hostname and ip address,
// wc is the job name,
// and pg-*.txt are the input files to pass to the worker(s).
func main() {
    drv := serverless.NewDriver(os.Args[1]) // the 1st cmd-line argument: driver hostname and ip addr
    jobName := os.Args[2]                   // the 2nd cmd-line argument: MapReduce job name
    nReduce, err := strconv.Atoi(os.Args[3])

    if err != nil {
        log.Fatal(err)
    }

    inFiles := os.Args[4:] // the rest of cmd-line argument: the input file names of the job

    go drv.Run(jobName, inFiles, nReduce)

    drv.Wait()
}
