//////////////////////////////////////////////////////////////////////
//
// Performs map actions for sort.
//
//////////////////////////////////////////////////////////////////////
package main

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/cespare/xxhash"
	"math/rand"
	//"github.com/go-redis/redis/v7"
	"github.com/mason-leap-lab/infinicache/client"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

const debugEnabled = true

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		log.Printf(format, a...)
	}
	return 0, nil
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// To compile the map plugin: run:
// go build --buildmode=plugin -o srtr_service.so srtr_service.go
// go build --buildmode=plugin -o srtm_service.so srtm_service.go

// Define Inverted Indexing's map service
type srtmService string

// MapReduceArgs defines this plugin's argument format
//type MapReduceArgs struct {
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

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being
// processed, and the value is the file's contents. The return value
// should be a slice of key/value pairs, each represented by a
// mapreduce.KeyValue.
func mapF(document string, value string) (res []KeyValue) {
	//var arr []string
	// Split up string line-by-line.
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if r == '\n' {
			return true
		}
		return false
	}) {
		res = append(res, KeyValue{s[0:10], s})
	}
	return res
}

// doMap does the job of a map worker: it reads one of the input
// files (S3Key), calls the user-defined function (mapF) for that
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
	trie serverless.TrieNode,
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

	//storageIps2 := []string{"10.0.109.88:6378", "10.0.121.202:6378"}
	log.Printf("Creating storage client for IPs: %v\n", storageIPs)
	cli := client.NewClient(dataShards, parityShards, maxGoRoutines)
	// var addrList = "127.0.0.1:6378"
	// addrArr := strings.Split(addrList, ",")
	cli.Dial(storageIPs)
	// log.Println("Creating Redis client for Redis @ 127.0.0.1:6378")
	// redis_client := redis.NewClient(&redis.Options{
	// 	Addr:         "127.0.0.1:6378",
	// 	Password:     "",
	// 	DB:           0,
	// 	ReadTimeout:  30 * time.Second,
	// 	WriteTimeout: 30 * time.Second,
	// 	MaxRetries:   3,
	// })

	log.Println("Successfully created storage client for storage @ 127.0.0.1:6378")

	Debug("Reading data for S3 key \"%s\" from downloaded file now...\n", S3Key)
	b, err = ioutil.ReadFile(S3Key)
	checkError(err)

	log.Println("Performing map function/operations now...")
	results := make(map[string][]KeyValue)
	for _, result := range mapF(S3Key, string(b)) {
		reducerNum := ihash(result.Key, trie) % nReduce
		redisKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[redisKey] = append(results[redisKey], result)
	}

	ioRecords := make([]IORecord, 0)

	log.Println("Storing results in storage now...")

	for k, v := range results {
		marshalled_result, err := json.Marshal(v)
		checkError(err)
		start := time.Now()
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f \n", k, float64(len(marshalled_result))/float64(1e6))
		writeStart := time.Now()
		//err = redis_client.Set(k, marshalled_result, 0).Err()
		log.Printf("Hash of key \"%s\": %v\n", k, xxhash.Sum64([]byte(k)))
		log.Printf("md5 of key \"%s\": %v\n", k, md5.Sum([]byte(k)))

		// Exponential backoff.
		success := false
		for current_attempt := 0; current_attempt < 10; current_attempt++ {
			log.Printf("Attempt %d/%d for key \"%s\".\n", current_attempt, 5, k)
			_, ok := cli.EcSet(k, marshalled_result)

			if !ok {
				max_duration := (2 << uint(current_attempt)) - 1
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

	//redis_client.Close()
	cli.Close()
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string, trie serverless.TrieNode) int {
	partition := serverless.GetPartition(s, trie)
	//fmt.Printf("Partition for key \"%s\": %d\n", s, partition)
	return partition
}

// DON'T MODIFY THIS FUNCTION
func (s srtmService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		return err
	}
	trie := serverless.BuildTrie(args.SampleKeys, 0, len(args.SampleKeys), "", 2)

	//keyTest := "mr.srt-res-1"
	//fmt.Printf("[TEST] srtr DoService -- Hash of key \"%s\": %v\n", keyTest, xxhash.Sum64([]byte(keyTest)))

	log.Printf("MAPPER -- args.S3Key: \"%s\"\n", args.S3Key)

	doMap(args.JobName, args.S3Key, args.StorageIPs, args.TaskNum, args.NReduce, args.DataShards, args.ParityShards, args.MaxGoroutines, trie)

	return nil
}

var Interface srtmService
