// MapReduce MAP function for GREP.

package main

import (
	"bytes"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"encoding/gob"
	"io/ioutil"
	"encoding/json"
	"time"
	"fmt"
	"math/rand"
	"github.com/mason-leap-lab/infinicache/client"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"hash/fnv"
	"regexp"
	"strconv"
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

type IORecord struct {
	TaskNum  int
	RedisKey string
	Bytes    int
	Start    int64
	End      int64
}

var pattern *regexp.Regexp

// The mapping function is called once for each piece of the input.
func mapF(s3Key string, text string) (res []KeyValue) {
	log.Printf("Searching for matches in string \"%s\".\n", text)
	matches := pattern.FindAllString(text, -1)

	for idx, match := range matches {
			log.Printf("Match #%d: \"%s\".\n", idx, match)
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
) {
	var err error
	var b []byte
	var s3KeyFile *os.File
	var ioData *os.File
	
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

	Debug("Reading data for S3 key \"%s\" from downloaded file now...\n", S3Key)
	b, err = ioutil.ReadFile(S3Key)
	checkError(err)

	log.Println("Performing grep map function now...")
	results := make(map[string][]KeyValue)
	for _, result := range mapF(S3Key, string(b)) {
		reducerNum := ihash(result.Key) % nReduce
		storageKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[storageKey] = append(results[storageKey], result)
	}

	ioRecords := make([]IORecord, 0)

	log.Printf("Creating storage client for IPs: %v\n", storageIPs)
	cli := client.NewClient(dataShards, parityShards, maxGoRoutines)
	cli.Dial(storageIPs)

	log.Println("Successfully created storage client.")

	log.Println("Storing results in storage now...")

	for k, v := range results {
		marshalled_result, err := json.Marshal(v)
		checkError(err)
		start := time.Now()
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f \n", k, float64(len(marshalled_result))/float64(1e6))
		writeStart := time.Now()

		// Exponential backoff.
		success := false
		for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
			log.Printf("Attempt %d/%d for write to key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, key)
			_, ok := cli.EcSet(k, marshalled_result)

			if !ok {
				max_duration := (2 << uint(current_attempt + 4)) - 1
				if max_duration > serverless.MaxBackoffSleepWrites { 
					max_duration = serverless.MaxBackoffSleepWrites
				}
				duration := rand.Intn(max_duration + 1)
				log.Printf("[ERROR] Failed to write key \"%s\". Backing off for %d ms.\n", k, duration)
				time.Sleep(time.Duration(duration) * time.Millisecond)
			} else {
				log.Printf("Successfully wrote key \"%s\" on attempt %d.\n", k, current_attempt)
				success = true
				break
			}
		}

		if !success {
			log.Fatal("Failed to write key \"%s\" to storage in minimum number of attempts.")
		}

		writeEnd := time.Since(writeStart)
		log.Printf("storage WRITE END. Key: \"%s\", Size: %f, Time: %d ms \n", k, float64(len(marshalled_result))/float64(1e6), writeEnd.Nanoseconds()/1e6)
		end := time.Now()
		rec := IORecord{TaskNum: taskNum, RedisKey: k, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
	}
	
	Debug("Writing metric data to file now...\n")
	ioData, err = os.Create("IOData/map_io_data_" + jobName + strconv.Itoa(taskNum) + ".dat")
	checkError(err)
	defer ioData.Close()
	for _, rec := range ioRecords {
		_, err := ioData.WriteString(fmt.Sprintf("%v\n", rec))
		checkError(err)
	}	
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