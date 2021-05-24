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
	"flag"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"io"
	"log"
	"os"
	"strings"
	//"strconv"
)

type arrayFlags []string

var storageIps arrayFlags

func (i *arrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

// The main entrance of client.go.
//
// The client can be run in the following way for example:
// go run client.go localhost:1234 wc pg-*.txt
// where localhost:1234 is the driver (client)'s hostname and ip address,
// wc is the job name,
// and pg-*.txt are the input files to pass to the worker(s).
func main() {
	log.Println("os.Args =", os.Args)

	// Get command-line arguments.
	driverHostname := flag.String("driverHostname", "127.0.0.1:1234", "The driver hostname and IP address.")
	jobName := flag.String("jobName", "srt", "The name of the MapReduce job.")
	nReduce := flag.Int("nReduce", 10, "Number of MapReduce reducers.")
	sampleDataKey := flag.String("sampleDataKey", "sample_data.dat", "The S3 key of the sample data to use for generating sample keys and building the trie.")
	s3KeyFile := flag.String("s3KeyFile", "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt", "File which contains the S3 keys of the input data partitions.")
	dataShards := flag.Int("dataShards", 10, "InfiniStore proxy data shards parameter.")
	parityShards := flag.Int("parityShards", 2, "InfiniStore proxy parity shards parameter.")
	maxGoRoutines := flag.Int("maxGoRoutines", 32, "InfiniStore proxy max goroutines parameter.")
	pattern := flag.String("pattern", "[a-zA-Z]+", "Regular expression pattern for Grep. Default is matching one or more letters (uppercase or lowercase) in a row.")
	clientPoolCapacity := flag.Int("clientPoolCapacity", 10, "Maximum capacity for the pool of InfiniStore clients.")
	chunkThreshold := flag.Int("chunkThreshold", 512000000, "The size (in bytes) above which we break a piece of data up into chunk-sized pieces and read/write those chunks rather than the entire piece of data all at once.")
	usePocket := flag.Bool("usePocket", false, "When set to true, workers will use Pocket for intermediate data storage.")
	numLambdasPocket := flag.Int("numLambdasPocket", 0, "The number of Lambdas to use for Pocket...?")
	capacityGbPocket := flag.Int("capacityGbPocket", 0, "Pocket capacity.")
	peakMbpsPocket := flag.Int("peakMbpsPocket", 0, "Peak Mbps Pocket.")
	latencySensitivePocket := flag.Int("latencySensitivePocket", 1, "Latency sensitive for Pocket?")
	flag.Var(&storageIps, "storageIps", "IP addresses for the intermediate storage (e.g., Redis shards, InfiniStore proxies, Pocket endpoints, etc.). At least one required.")
	flag.Parse()

	drv := serverless.NewDriver(*driverHostname) // the 1st cmd-line argument: driver hostname and ip addr

	filename := "Client-" + (*driverHostname) + "-" + (*jobName)
	f, err := os.OpenFile(filename+".out", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	serverless.logCreate(filename)

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	mw := io.MultiWriter(os.Stdout, f)
	log.SetOutput(mw)

	serverless.Debug("jobName: %s, nReduce: %d\n", *jobName, nReduce)

	//sampleDataKey := os.Args[4] // The S3 key of the sample data to use for generating sample keys and building the trie.
	//s3KeyFile := os.Args[5]     // File which contains the S3 keys of the input data partitions.

	// dataShards, err := strconv.Atoi(os.Args[6])
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// parityShards, err := strconv.Atoi(os.Args[7])
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// maxGoRoutines, err := strconv.Atoi(os.Args[8])
	// if err != nil {
	// 	log.Fatal(err)
	// }

	go drv.Run(*jobName, *s3KeyFile, *sampleDataKey, *nReduce, *dataShards, *parityShards,
		*maxGoRoutines, *pattern, *clientPoolCapacity, *chunkThreshold, *usePocket,
		*numLambdasPocket, *capacityGbPocket, *peakMbpsPocket, *latencySensitivePocket, storageIps)

	drv.Wait()
	nanolog.Flush()
}
