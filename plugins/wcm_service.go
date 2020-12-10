//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Performs map actions for word count service.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"bytes"
	"encoding/gob"

	//"encoding/json"
	"crypto/md5"
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	//"github.com/go-redis/redis/v7"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/mason-leap-lab/infinicache/client"
)

var clientCreated = false // Has this InfiniStore client been created yet?
var clientDialed = false  // Have we called the client's Dial function yet?
var poolCreated = false

var poolLock = &sync.Mutex{}
var clientPool *serverless.Pool

func InitPool(dataShard int, parityShard int, ecMaxGoroutine int, addrArr []string, clientPoolCapacity int) {
	clientPool = serverless.InitPool(&serverless.Pool{
		New: func() interface{} {
			cli := client.NewClient(dataShard, parityShard, ecMaxGoroutine)
			log.Printf("Client created. Dialing addresses now: %v\n", addrArr)
			cli.Dial(addrArr)
			log.Printf("Dialed successfully.\n")
			return cli
		},
		Finalize: func(c interface{}) {
			c.(*client.Client).Close()
		},
	}, clientPoolCapacity, serverless.PoolForStrictConcurrency)
}

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

// Define Word Count's map service
type wcmService string

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
func mapF(document string, value string) []KeyValue {
	res := make([]KeyValue, 0, 50000000) // Preallocate very large array...
	every := 100
	thresh := 1000
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) {
			return true
		}
		return false
	}) {
		res = append(res, KeyValue{s, "1"})

		if len(res)%every == 0 {
			log.Printf("Identified %d words so far.\n", len(res))
		}

		// Every time the order of magnitude of the number of results increases,
		// we decrease the frequency that we print something.
		if len(res) >= thresh {
			thresh = thresh * 10
			every = every * 10

			// Cap at printing every 5mil words.
			if every >= 10000000 {
				every = 5000000
			}
		}
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

	s3_start_time := time.Now()

	// Write the contents of S3 Object to the file
	num_bytes_s3, err := downloader.Download(s3KeyFile, &s3.GetObjectInput{
		Bucket: aws.String("infinistore-mapreduce"),
		Key:    aws.String(S3Key),
	})
	checkError(err)

	s3_end_time := time.Now()
	s3_duration := time.Since(s3_start_time)

	log.Printf("File %s downloaded in %d ms, %d bytes\n", S3Key, s3_duration.Nanoseconds()/1e6, num_bytes_s3)

	// =====================================================================
	// Storage Client Creation
	// ---------------------------------------------------------------------
	// In theory, you would create whatever clients that Pocket uses here...
	// =====================================================================
	// log.Printf("Creating storage client for IPs: %v\n", storageIPs)
	//cli := client.NewClient(dataShards, parityShards, maxGoRoutines)
	//cli.Dial(storageIPs)

	log.Printf("Mapper getting storage client from client pool now...\n")
	cli := clientPool.Get().(*client.Client)

	log.Println("Mapper successfully created storage client.")

	Debug("Reading data for S3 key \"%s\" from downloaded file now...\n", S3Key)
	b, err = ioutil.ReadFile(S3Key)
	checkError(err)

	log.Println("Performing map function/operations now...")
	results := make(map[string][]KeyValue)
	for _, result := range mapF(S3Key, string(b)) {
		reducerNum := ihash(result.Key) % nReduce
		redisKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[redisKey] = append(results[redisKey], result)
	}

	log.Println("Storing results in storage now...")

	ioRecords := make([]IORecord, 0, len(results))

	// Create record for S3.
	s3rec := IORecord{TaskNum: taskNum, RedisKey: "S3", Bytes: int(num_bytes_s3), Start: s3_start_time.UnixNano(), End: s3_end_time.UnixNano()}
	ioRecords = append(ioRecords, s3rec)

	for k, v := range results {
		var byte_buffer bytes.Buffer
		gobEncoder := gob.NewEncoder(&byte_buffer)
		err := gobEncoder.Encode(v)
		checkError(err)
		marshalled_result := byte_buffer.Bytes() // should be of type []byte now.
		//marshalled_result, err := json.Marshal(v)
		//checkError(err)
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f \n", k, float64(len(marshalled_result))/float64(1e6))
		var writeStart time.Time
		//err = redis_client.Set(k, marshalled_result, 0).Err()

		// Exponential backoff.
		success := false
		for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
			log.Printf("Attempt %d/%d for write to key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, k)
			log.Printf("md5 of marshalled result for key \"%s\": %x\n", k, md5.Sum(marshalled_result))
			writeStart = time.Now()
			// IOHERE - This is a write (k is the key, it is a string, marshalled_result is the value, it is []byte).
			_, ok := cli.EcSet(k, marshalled_result)

			if !ok {
				max_duration := (2 << uint(current_attempt+4)) - 1
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

		writeDuration := time.Since(writeStart)
		writeEnd := time.Now()
		log.Printf("storage WRITE END. Key: \"%s\", Size: %f, Time: %d ms \n", k, float64(len(marshalled_result))/float64(1e6), writeDuration.Nanoseconds()/1e6)
		rec := IORecord{TaskNum: taskNum, RedisKey: k, Bytes: len(marshalled_result), Start: writeStart.UnixNano(), End: writeEnd.UnixNano()}
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

	clientPool.Put(cli)
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

func (s wcmService) ClosePool() error {
	if clientPool != nil {
		log.Printf("Closing the wcm_service client pool...")
		clientPool.Close()
	}

	return nil
}

// DON'T MODIFY THIS FUNCTION
func (s wcmService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		log.Printf("Word Count Service: Failed to decode!\n")
		return err
	}
	log.Printf("MAPPER -- args.S3Key: \"%s\"\n", args.S3Key)

	if args.UsePocket {
		log.Printf("=-=-= USING POCKET FOR INTERMEDIATE DATA STORAGE =-=-=\n")
	} else {
		log.Printf("=-=-= USING INFINISTORE FOR INTERMEDIATE DATA STORAGE =-=-=\n")
	}

	poolLock.Lock()
	if !poolCreated {
		log.Printf("Initiating client pool now. Pool size = %d.\n", args.ClientPoolCapacity)
		InitPool(args.DataShards, args.ParityShards, args.MaxGoroutines, args.StorageIPs, args.ClientPoolCapacity)

		poolCreated = true
	}
	poolLock.Unlock()

	doMap(args.JobName, args.S3Key, args.StorageIPs, args.TaskNum, args.NReduce,
		args.DataShards, args.ParityShards, args.MaxGoroutines)

	return nil
}

var Interface wcmService
