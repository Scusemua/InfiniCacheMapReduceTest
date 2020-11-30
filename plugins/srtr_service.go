//////////////////////////////////////////////////////////////////////
//
// Performs reduce actions for sort.
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
	"github.com/cespare/xxhash"
	//"github.com/go-redis/redis/v7"
	"github.com/mason-leap-lab/infinicache/client"
	//infinicache "github.com/mason-leap-lab/infinicache/client"
	//"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	//"unsafe"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o srtr_service.so srtr_service.go
// go build --buildmode=plugin -o srtm_service.so srtm_service.go

// Define Inverted Indexing's reduce service
type srtrService string

// MapReduceArgs defines this plugin's argument format
type MapReduceArgs struct {
	JobName       string
	S3Key         string
	TaskNum       int
	NReduce       int
	NOthers       int
	SampleKeys    []string
	StorageIPs    []string
	DataShards    int
	ParityShards  int
	MaxGoroutines int
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

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const debugEnabled = true

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		log.Printf(format, a...)
	}
	return 0, nil
}

// func mergeSort(arr []string) []string {
// 	var arr_length = len(arr)

// 	if arr_length == 1 {
// 		return arr
// 	}

// 	midpoint := int(arr_length / 2)

// 	return merge(mergeSort(arr[:midpoint]), mergeSort(arr[midpoint:]))
// }

func merge(left []string, right []string) (merged []string) {
	// Store merged values in here.
	merged = make([]string, len(left)+len(right))

	Debug("Merging... (len(left) = %d, len(right) = %d)\n", len(left), len(right))

	i := 0

	for len(left) > 0 && len(right) > 0 {
		// The values are the rows from gensort, which are "<actual string>  <string index>  <checksum>"
		left_split := strings.Split(left[0], "  ")
		right_split := strings.Split(right[0], "  ")

		if left_split[0] < right_split[0] {
			merged[i] = left[0]
			left = left[1:]
		} else {
			merged[i] = right[0]
			right = right[1:]
		}
		i++
	}

	// Add remaining elements from left/right to the merged array.
	for j := 0; j < len(left); j++ {
		merged[i] = left[j]
		i++
	}
	for j := 0; j < len(right); j++ {
		merged[i] = right[j]
		i++
	}

	return merged
}

// The reduce function is called once for each key generated by Map,
// with a list of that key's string value (merged across all inputs).
// The return value should be a single output value for that key.
func reduceF(key string, values []string) string {
	// Just sort and count the documents. Output as value.
	//sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	// fmt.Printf("Values (in Reducer):\n")
	// for i, s := range values[0] {
	// 	fmt.Printf("%d: %s\n", i, s)
	// }

	// Perform merge sort.
	//sorted := mergeSort(values[0])
	return strings.Join(values, "\n")

	//Debug("Merge-sort completed. Sorted values:\n")
	//for _, s := range sorted {
	//	Debug("%s\n", s)
	//}
	//return sorted //fmt.Sprintf("%s", strings.Join(sorted, ","))
}

func split(buf []byte, lim int) [][]byte {
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}

// doReduce does the job of a reduce worker: it reads the
// intermediate key/value pairs (produced by the map phase) for this
// task, sorts the intermediate key/value pairs by key, calls the
// user-defined reduce function (reduceF) for each key, and writes
// the output to disk. Each reduce generates an output file named
// using serverless.MergeName(jobName, reduceTask).
func doReduce(
	jobName string,
	storageIps []string,
	reduceTaskNum int,
	nMap int,
	dataShards int,
	parityShards int,
	maxEcGoroutines int,
) {
	// log.Println("Creating Redis client for Redis @ 127.0.0.1:6378")
	// redis_client := redis.NewClient(&redis.Options{
	// 	Addr:         "127.0.0.1:6378",
	// 	Password:     "",
	// 	DB:           0,
	// 	ReadTimeout:  30 * time.Second,
	// 	WriteTimeout: 30 * time.Second,
	// 	MaxRetries:   3,
	// })
	//storageIps2 := []string{"10.0.109.88:6378", "10.0.121.202:6378"}
	log.Println("Creating storage client for storage ", storageIPs)
	cli := client.NewClient(dataShards, parityShards, maxEcGoroutines)
	// var addrList = "127.0.0.1:6378"
	// addrArr := strings.Split(addrList, ",")
	cli.Dial(storageIPs)

	log.Println("Successfully created storage client")

	ioRecords := make([]IORecord, 0)

	log.Println("Retrieving input data for reduce task #", reduceTaskNum)
	inputs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		// nMap is the number of Map tasks (i.e., the number of S3 keys or # of initial data partitions).
		// So the variable i here refers to the associated Map task/initial data partition, or where
		// the data we're processing came from, essentially.
		redisKey := serverless.ReduceName(jobName, i, reduceTaskNum)

		var kvs []KeyValue
		start := time.Now()
		log.Printf("storage READ START. Key: \"%s\", Reduce Task #: %d.", redisKey, reduceTaskNum)
		//marshalled_result, err := redis_client.Get(redisKey).Result()
		log.Printf("Hash of key \"%s\": %v\n", redisKey, xxhash.Sum64([]byte(redisKey)))
		log.Printf("md5 of key \"%s\": %v\n", redisKey, md5.Sum([]byte(redisKey)))
		reader, ok := cli.Get(redisKey)
		if !ok || reader == nil {
			log.Printf("ERROR: Failed to retrieve data from storage with key \"%s\"", redisKey)
			continue
		}
		marshalled_result, err := reader.ReadAll()
		reader.Close()
		if (err != nil) || (!ok) {
			log.Printf("ERROR: storage encountered exception for key \"%s\"...", "addr", redisKey)
			log.Printf("ERROR: Just skipping the key \"%s\"...", redisKey)
			// In theory, there was just no task mapped to this Reducer for this value of i. So just move on...
			continue
		}
		end := time.Now()
		readDuration := time.Since(start)
		log.Printf("storage READ END. Key: \"%s\", Reduce Task #: %d, Bytes read: %f, Time: %d ms", redisKey, reduceTaskNum, float64(len(marshalled_result))/float64(1e6), readDuration.Nanoseconds()/1e6)
		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: redisKey, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
		json.Unmarshal([]byte(marshalled_result), &kvs)
		for _, kv := range kvs {
			inputs = append(inputs, kv)
		}
	}
	//fmt.Println("inputs =")
	//fmt.Println(inputs)
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Key < inputs[j].Key })

	fileName := serverless.MergeName(jobName, reduceTaskNum)

	var results []KeyValue

	doReduce := func(k string, v []string) {
		output := reduceF(k, v)
		new_kv := new(KeyValue)
		new_kv.Key = k
		new_kv.Value = output
		//Debug("Output:\n%s\n", output)
		//err = enc.Encode(&new_kv)
		//checkError(err)
		results = append(results, *new_kv)
	}

	var lastKey string
	values := make([]string, 0)
	for i, kv := range inputs {
		if kv.Key != lastKey && i > 0 {
			doReduce(lastKey, values)
			values = make([]string, 0)
		}
		lastKey = kv.Key
		values = append(values, kv.Value)
	}
	doReduce(lastKey, values)

	marshalled_result, err := json.Marshal(results)
	checkError(err)
	log.Println("Writing final result to Redis at key", fileName, ". Size:", float64(len(marshalled_result))/float64(1e6), "MB.")

	chunk_threshold := 512 * 1e6

	/* Chunk up the final results if necessary. */
	if len(marshalled_result) > int(chunk_threshold) {
		log.Printf("Final result is larger than %dMB. Storing it in pieces...", int(chunk_threshold/1e6))
		chunks := split(marshalled_result, int(chunk_threshold))
		num_chunks := len(chunks)
		log.Println("Created", num_chunks, " chunks for final result", fileName)
		base_key := fileName + "-part"
		for i, chunk := range chunks {
			key := base_key + string(i)
			log.Printf("storage WRITE CHUNK START. Chunk #: %d, Key: \"%s\", Size: %f MB\n", i, key, float64(len(chunk))/float64(1e6))
			start := time.Now()
			//err := redis_client.Set(key, chunk, 0).Err()
			log.Printf("Hash of key \"%s\": %v\n", key, xxhash.Sum64([]byte(key)))
			log.Printf("md5 of key \"%s\": %v\n", key, md5.Sum([]byte(key)))
			_, ok := cli.EcSet(key, chunk)
			end := time.Now()
			writeEnd := time.Since(start)
			//checkError(err)
			if !ok {
				log.Fatal("\n\nERROR while storing value in storage, key is: \"", key, "\"")
			}
			log.Printf("storage WRITE CHUNK END. Chunk #: %d, Key: \"%s\", Size: %f, Time: %v ms \n", i, key, float64(len(chunk))/float64(1e6), writeEnd.Nanoseconds()/1e6)

			rec := IORecord{TaskNum: reduceTaskNum, RedisKey: key, Bytes: len(chunk), Start: start.UnixNano(), End: end.UnixNano()}
			ioRecords = append(ioRecords, rec)
		}
		num_chunks_serialized, err3 := json.Marshal(num_chunks)
		checkError(err3)
		//err := redis_client.Set(fileName, num_chunks_serialized, 0).Err()
		log.Printf("Hash of key \"%s\": %v\n", fileName, xxhash.Sum64([]byte(fileName)))
		log.Printf("md5 of key \"%s\": %v\n", fileName, md5.Sum([]byte(fileName)))
		_, ok := cli.EcSet(fileName, num_chunks_serialized)
		if !ok {
			log.Fatal("ERROR while storing value in storage, key is: \"", fileName, "\"")
		}
		checkError(err)
	} else {
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f MB\n", fileName, float64(len(marshalled_result))/float64(1e6))
		start := time.Now()
		//err := redis_client.Set(fileName, marshalled_result, 0).Err()
		log.Printf("Hash of key \"%s\": %v\n", fileName, xxhash.Sum64([]byte(fileName)))
		log.Printf("md5 of key \"%s\": %v\n", fileName, md5.Sum([]byte(fileName)))
		_, ok := cli.EcSet(fileName, marshalled_result)
		if !ok {
			log.Fatal("ERROR while storing value in storage, key is key is: \"", fileName, "\"")
		}
		//checkError(err)
		end := time.Now()
		writeEnd := time.Since(start)

		log.Printf("storage WRITE END. Key: \"%s\", storage Hostname: %s, Size: %f, Time: %d ms \n", fileName, "127.0.0.1:6378", float64(len(marshalled_result))/float64(1e6), writeEnd.Nanoseconds()/1e6)

		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: fileName, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
	}

	f2, err2 := os.Create("IOData/reduce_io_data_" + jobName + strconv.Itoa(reduceTaskNum) + ".dat")
	checkError(err2)
	defer f2.Close()
	for _, rec := range ioRecords {
		_, err2 := f2.WriteString(fmt.Sprintf("%v\n", rec))
		checkError(err2)
	}

	// Close all of the Redis clients now that we're
	//redis_client.Close()
	cli.Close()
}

// DON'T MODIFY THIS FUNCTION
func (s srtrService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		log.Printf("Sort: Failed to decode!\n")
		return err
	}
	log.Printf("REDUCER for Reducer Task # \"%d\"\n", args.TaskNum)

	doReduce(args.JobName, args.StorageIPs, args.TaskNum, args.NOthers, args.DataShards, args.ParityShards, args.MaxGoroutines)

	return nil
}

var Interface srtrService
