// MapReduce MAP function for GREP.

package main

import (
	"bytes"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"encoding/gob"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"hash/fnv"
	"regexp"
	"log"
	"os"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o wcm_service.so wcm_service.go

const debugEnabled = true

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Define grep's map service
type grepmService string

// MapReduceArgs defines this plugin's argument format
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
// }

type KeyValue struct {
	Key   string
	Value string
}

var pattern *regexp.Regexp

// The mapping function is called once for each piece of the input.
func mapF(s3Key string, text string) (res []KeyValue) {
	matches := pattern.FindAllString(text, -1)

	for _, match := range matches {
		res = append(res, KeyValue{text, match})
	}

	return res
}

// doMap does the job of a map worker: it reads one of the input
// files (inFile), calls the user-defined function (mapF) for that
// file's contents, and partitions the output into nReduce
// intermediate files.
func doMap(
	jobName string,
	S3Key string,
	storageIPs []string,
	taskNum int,
	nReduce int,
	dataShards int,
	parityShards int,
	maxGoRoutines int,
	pattern string,
) {
	var err error
	var b []byte
	var s3KeyFile *os.File
	var ioData *os.File

	keyTest := "mr.srt-res-1"
	fmt.Printf("[TEST] srtm doMap -- Hash of key \"%s\": %v\n", keyTest, xxhash.Sum64([]byte(keyTest)))

	// The session the S3 Downloader will use
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1")},
	))

	// Create a downloader with the session and default options
	downloader := s3manager.NewDownloader(sess)

	log.Printf("Creating file \"%s\" to read S3 data into...\n", S3Key)

	// Create a file to write the S3 Object contents to.
	s3KeyFile, err = os.Create(S3Key)
	checkError(err)

	// Write the contents of S3 Object to the file
	n, err := downloader.Download(s3KeyFile, &s3.GetObjectInput{
		Bucket: aws.String("infinistore-mapreduce"),
		Key:    aws.String(S3Key),
	})
	checkError(err)

	log.Printf("File %s downloaded, %d bytes\n", S3Key, n)

	log.Println("Performing grep map function now...")
	results := make(map[string][]KeyValue)
	for _, result := range mapF(S3Key, string(b)) {
		reducerNum := ihash(result.Key) % nReduce
		storageKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[storageKey] = append(results[storageKey], result)
	}

	return nil 
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// DON'T MODIFY THIS FUNCTION
func (s grepmService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		return err
	}
	log.Printf("MAPPER -- args.S3Key: \"%s\"\n", args.S3Key)
	log.Printf("Compiling the regex pattern \"%s\"\n", args.Pattern)

	// Compile the regex pattern.
	pattern = regexp.MustCompile(args.Pattern)

	doMap(args.JobName, args.S3Key, args.StorageIPs, args.TaskNum, args.NReduce, args.DataShards, args.ParityShards, args.MaxGoroutines)

	return nil
}

var Interface grepmService