//////////////////////////////////////////////////////////////////////
//
// Performs map actions for sort.
//
//////////////////////////////////////////////////////////////////////
package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-redis/redis/v7"
	"github.com/lafikl/consistent"
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
		n, err = log.Printf(format, a...)
	}
	return
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
type MapReduceArgs struct {
	JobName        string
	S3Key          string
	TaskNum        int
	NReduce        int
	NOthers        int
	SampleKeys     []string
	RedisEndpoints []string
}

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
		//arr = append(arr, s)
		res = append(res, KeyValue{s[0:10], s})
	}
	//Debug("\narr:\n%s\n", strings.Join(arr, ","))
	//res = append(res, KeyValue{document, arr})
	//Debug("\nRes:\n%s\n", res)
	return res
}

// doMap does the job of a map worker: it reads one of the input
// files (S3Key), calls the user-defined function (mapF) for that
// file's contents, and partitions the output into nReduce
// intermediate files.
func doMap(
	jobName string,
	S3Key string,
	redisEndpoints []string,
	taskNum int,
	nReduce int,
	trie serverless.TrieNode,
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

	c := consistent.New()
	clientMap := make(map[string]*redis.Client)

	log.Println("Populating hash ring and client map now...")

	// Add the IP addresses of the Reds instances to the ring.
	// Create the Redis clients and store them in the map.
	for _, hostname := range redisEndpoints {
		// Add hostname to hash ring.
		c.Add(hostname)

		log.Println("Creating Redis client for Redis listening at", hostname)

		// Create client.
		client := redis.NewClient(&redis.Options{
			Addr:     hostname,
			Password: "",
			DB:       0,
		})

		// Store client in map.
		clientMap[hostname] = client
	}

	Debug("Reading %s\n", S3Key)
	b, err = ioutil.ReadFile(S3Key)
	checkError(err)

	results := make(map[string][]KeyValue)
	for _, result := range mapF(S3Key, string(b)) {
		reducerNum := ihash(result.Key, trie) % nReduce
		redisKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[redisKey] = append(results[redisKey], result)
	}

	ioRecords := make([]IORecord, 0)

	for k, v := range results {
		marshalled_result, err := json.Marshal(v)
		checkError(err)
		start := time.Now()
		host, err := c.Get(k)
		checkError(err)
		client := clientMap[host]
		err = client.Set(k, marshalled_result, 0).Err()
		end := time.Now()
		rec := IORecord{TaskNum: taskNum, RedisKey: k, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
		checkError(err)
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
func ihash(s string, trie serverless.TrieNode) int {
	partition := serverless.GetPartition(s, trie)
	//fmt.Printf("Partition for key \"%s\": %d\n", s, partition)
	return partition
	//h := fnv.New32a()
	//h.Write([]byte(s))
	//return int(h.Sum32() & 0x7fffffff)
}

// DON'T MODIFY THIS FUNCTION
func (s srtmService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		return err
	}
	trie := serverless.BuildTrie(args.SampleKeys, 0, len(args.SampleKeys), "", 2)

	log.Printf("DoService srtm -- args.S3Key: \"%s\"\n", args.S3Key)

	doMap(args.JobName, args.S3Key, args.RedisEndpoints, args.TaskNum, args.NReduce, trie)

	return nil
}

var Interface srtmService
