//////////////////////////////////////////////////////////////////////
//
// Performs reduce actions for sort.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"github.com/go-redis/redis/v7"
	"github.com/lafikl/consistent"
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

func mergeSort(arr []string) []string {
	var arr_length = len(arr)

	if arr_length == 1 {
		return arr
	}

	midpoint := int(arr_length / 2)

	return merge(mergeSort(arr[:midpoint]), mergeSort(arr[midpoint:]))
}

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

// doReduce does the job of a reduce worker: it reads the
// intermediate key/value pairs (produced by the map phase) for this
// task, sorts the intermediate key/value pairs by key, calls the
// user-defined reduce function (reduceF) for each key, and writes
// the output to disk. Each reduce generates an output file named
// using serverless.MergeName(jobName, reduceTask).
func doReduce(
	jobName string,
	redisEndpoints []string,
	reduceTaskNum int,
	nMap int,
) {
	c := consistent.New()
	clientMap := make(map[string]*redis.Client)

	log.Println("Populating hash ring and client map now...")

	// Add the IP addresses of the Reds instances to the ring.
	// Create the Redis clients and store them in the map.
	for _, hostname := range redisEndpoints {
		// Add hostname to hash ring.
		c.Add(hostname)

		log.Println("Creating Redis client for Redis @", hostname)

		// Create client.
		client := redis.NewClient(&redis.Options{
			Addr:         hostname,
			Password:     "",
			DB:           0,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 10 * time.Second,
			MaxRetries:   3,
		})

		log.Println("Successfully created Redis client for Redis @", hostname)

		// Store client in map.
		clientMap[hostname] = client
	}
	ioRecords := make([]IORecord, 0)

	inputs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		redisKey := serverless.ReduceName(jobName, i, reduceTaskNum)

		//for {
		var kvs []KeyValue
		start := time.Now()
		host, err := c.Get(redisKey)
		checkError(err)
		client := clientMap[host]
		fmt.Printf("Retrieving value from Redis %s for reduce task #%d at key \"%s\"...\n", host, reduceTaskNum, redisKey)
		marshalled_result, err := client.Get(redisKey).Result()
		fmt.Printf("Successfully retrieved value from Redis @ %s, key = \"%s\", reduce task # = %d\n", host, redisKey, reduceTaskNum)
		end := time.Now()
		checkError(err)
		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: redisKey, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
		json.Unmarshal([]byte(marshalled_result), &kvs)
		//fmt.Printf("Retrieved list of %d KeyValue structs from Redis.\n", len(kvs))
		for _, kv := range kvs {
			//fmt.Printf("%+v\n", kv)
			inputs = append(inputs, kv)
		}
		//}
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

	log.Println("Writing final result to Redis at key", fileName)
	marshalled_result, err := json.Marshal(results)
	checkError(err)
	start := time.Now()
	host, err := c.Get(fileName)
	checkError(err)
	client := clientMap[host]
	err = client.Set(fileName, marshalled_result, 0).Err()
	checkError(err)
	end := time.Now()

	rec := IORecord{TaskNum: reduceTaskNum, RedisKey: fileName, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
	ioRecords = append(ioRecords, rec)

	f2, err2 := os.Create("IOData/reduce_io_data_" + jobName + strconv.Itoa(reduceTaskNum) + ".dat")
	checkError(err2)
	defer f2.Close()
	for _, rec := range ioRecords {
		_, err2 := f2.WriteString(fmt.Sprintf("%v\n", rec))
		checkError(err2)
	}
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
	log.Printf("REDUCER for S3 Key \"%s\"\n", args.S3Key)

	doReduce(args.JobName, args.RedisEndpoints, args.TaskNum, args.NOthers)

	return nil
}

var Interface srtrService
