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
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"io"
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
	log.Println("os.Args =", os.Args)
	drv := serverless.NewDriver(os.Args[1]) // the 1st cmd-line argument: driver hostname and ip addr
	jobName := os.Args[2]                   // the 2nd cmd-line argument: MapReduce job name
	nReduce, err := strconv.Atoi(os.Args[3])

	if err != nil {
		log.Fatal(err)
	}

	f, err := os.OpenFile("Client-"+string(os.Args[1])+".out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(mw)

	serverless.Debug("jobName: %s, nReduce: %d\n", jobName, nReduce)

	sampleDataKey := os.Args[4] // The S3 key of the sample data to use for generating sample keys and building the trie.
	s3KeyFile := os.Args[5]     // File which contains the S3 keys of the input data partitions.

	go drv.Run(jobName, s3KeyFile, sampleDataKey, nReduce)

	drv.Wait()
}
