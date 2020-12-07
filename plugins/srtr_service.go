//////////////////////////////////////////////////////////////////////
//
// Performs reduce actions for sort.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"bytes"
	//"crypto/md5"
	"encoding/gob"
	//"encoding/json"
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"math/rand"
	//"github.com/go-redis/redis/v7"
	"github.com/mason-leap-lab/infinicache/client"
	//infinicache "github.com/mason-leap-lab/infinicache/client"
	//"io"
	"log"
	"os"
	"sync"
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

//var cli *client.Client		// The InfiniStore client.
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

// func CreateInfiniStoreClient(taskNum int, dataShards int, parityShards int, maxGoRoutines int) {
// 	log.Printf("[Mapper #%d] Creating InfiniStore client now...\n", taskNum)
// 	cli = client.NewClient(dataShards, parityShards, maxGoRoutines)

// 	clientCreated = true
// }

// func DialInfiniStoreClient(taskNum int, storageIps []string) {
// 	log.Printf("[Mapper #%d] Dialing InfiniStore client now...\n", taskNum)
// 	cli.Dial(storageIps)

// 	clientDialed = true
// }

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
	log.Printf("REDUCE: Joining %d strings for key \"%s\" now...\n", len(values), key)
	if len(values) == 1 {
		return values[0]
	}
	return strings.Join(values, "\n")
}

func split(buf []byte, lim int) [][]byte {
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, rest := buf[:lim], buf[lim:]
		newBuff := make([]byte, len(rest))
		copy(newBuff, rest)
		buf = newBuff
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
func doReduceDriver(
	jobName string,
	storageIps []string,
	reduceTaskNum int,
	nMap int,
	dataShards int,
	parityShards int,
	maxEcGoroutines int,
	chunkThreshold int,
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
	log.Printf("[REDUCER #%d] Creating storage client for IPs %v.\n", reduceTaskNum, storageIps)

	// =====================================================================
	// Storage Client Creation
	// ---------------------------------------------------------------------
	// In theory, you would create whatever clients that Pocket uses here...
	// =====================================================================

	log.Printf("[REDUCER #%d] Getting storage client from client pool now...\n", reduceTaskNum)

	// This creates a new InfiniStore EcClient object.
	// cli := client.NewClient(dataShards, parityShards, maxEcGoroutines)
	cli := clientPool.Get().(*client.Client)

	// This effectively connects the InfiniStore EcClient to all of the proxies.
	//cli.Dial(storageIps)

	log.Printf("[REDUCER #%d] Successfully created storage client\n", reduceTaskNum)

	ioRecords := make([]IORecord, 0)

	log.Printf("[REDUCER #%d] Retrieving input data.\n", reduceTaskNum)
	inputs := make([]KeyValue, 0)

	// We flip this to true the first time we receive at least one element from a mapper.
	// This is used when approximating the final size of the inputs slice.
	receivedDataForTheFirstTime := false

	for i := 0; i < nMap; i++ {
		// nMap is the number of Map tasks (i.e., the number of S3 keys or # of initial data partitions).
		// So the variable i here refers to the associated Map task/initial data partition, or where
		// the data we're processing came from, essentially.
		dataKey := serverless.ReduceName(jobName, i, reduceTaskNum)

		var kvs []KeyValue
		start := time.Now()
		log.Printf("storage READ START. Key: \"%s\", Reduce Task #: %d.", dataKey, reduceTaskNum)
		//marshalled_result, err := redis_client.Get(dataKey).Result()

		var readAllCloser client.ReadAllCloser
		var ok bool
		success := false
		// Exponential backoff.
		for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
			// This is a read operation from InfiniStore. The code that follows is also related.
			// Basically, the Get function returns a tuple where the first element of the tuple is
			// an object of type ReadAllCloser, and the second element of the tuple is a boolean
			// which indicates whether or not the read operation went well.
			log.Printf("[REDUCER #%d] Attempt %d/%d for read key \"%s\".\n", reduceTaskNum, current_attempt, serverless.MaxAttemptsDuringBackoff, dataKey)
			readAllCloser, ok = cli.Get(dataKey)

			// Check for failure, and backoff exponentially on-failure.
			// If the bool is false, then there was an error. If the key was not found, then readAllCloser will be null.
			if !ok {
				max_duration := (2 << uint(current_attempt)) - 1
				duration := rand.Intn(max_duration + 1)
				log.Printf("[ERROR] Failed to read key \"%s\". Backing off for %d ms.\n", dataKey, duration)
				time.Sleep(time.Duration(duration) * time.Millisecond)
			} else {
				log.Printf("[REDUCER #%d] Successfully read data with key \"%s\" on attempt %d.\n", reduceTaskNum, dataKey, current_attempt)
				success = true
				break
			}
		}

		// If ultimately the read failed after multiple attempts during exponential backoff,
		// we log the error with log.Fatal(), which prints the error and then exits.
		if !success {
			log.Fatal("ERROR: Failed to retrieve data from storage with key \"" + dataKey + "\" in allotted number of attempts.\n")
		}

		// If the ReadAllCloser is nil but there was no error, then that means the key was not found.
		if readAllCloser == nil {
			log.Printf("WARNING: Key \"%s\" does not exist in intermediate storage.\n", dataKey)
			log.Printf("WARNING: Skipping key \"%s\"...\n", dataKey)
			// In theory, there was just no task mapped to this Reducer for this value of i. So just move on...
			continue
		}

		log.Printf("[REDUCER #%d] Calling .ReadAll() on ReadAllCloser for key \"%s\" now...\n", reduceTaskNum, dataKey)
		// To get the data from the read, we call ReadAll() on the ReadAllCloser object. This is still
		// InfiniStore specific. That's just how it works. After reading, we call Close().
		encoded_result, err := readAllCloser.ReadAll()
		readAllCloser.Close()
		if err != nil {
			log.Fatal("Unexpected exception during ReadAll() for key \"%s\".\n", dataKey)
			//log.Printf("ERROR: storage encountered exception for key \"%s\"...", dataKey)
			//log.Printf("ERROR: Just skipping the key \"%s\"...", dataKey)
			// In theory, there was just no task mapped to this Reducer for this value of i. So just move on...
			continue
		}
		end := time.Now()
		readDuration := time.Since(start)
		log.Printf("storage READ END. Key: \"%s\", Reduce Task #: %d, Bytes read: %f, Time: %d ms", dataKey, reduceTaskNum, float64(len(encoded_result))/float64(1e6), readDuration.Nanoseconds()/1e6)
		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: dataKey, Bytes: len(encoded_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)

		log.Printf("[REDUCER #%d] Decoding data for key \"%s\" now...\n", reduceTaskNum, dataKey)
		byte_buffer_res := bytes.NewBuffer(encoded_result)
		//byte_buffer_res.Write(encoded_result)
		gobDecoder := gob.NewDecoder(byte_buffer_res)
		err = gobDecoder.Decode(&kvs)

		checkError(err)

		log.Printf("[REDUCER #%d] Successfully decoded data for key \"%s\".\n", reduceTaskNum, dataKey)
		log.Printf("[REDUCER #%d] Received %d KeyValue structs from mapper #%d.\n", reduceTaskNum, len(kvs), i)

		// If this is the first time we're obtaining data, I preemptively expand the length of the array
		// equal to the number of elements we just received * nMap (this, of course, assumes we'll be
		// receiving approximately the same amount of data from all mappers, which or may not be true.)
		//
		// The goal here is to prevent a significant number of memory-reallocations. For larger problem sizes,
		// such as 100GB, the final size of inputs could grow to be as large as 10-10.5mil elements. We may be
		// able to approximate that here, thereby avoiding a significant number of unnecessary memory reallocations.
		if len(kvs) > 0 && !receivedDataForTheFirstTime {
			log.Printf("[REDUCER #%d] Allocating slice of capacity %d for inputs.\n", reduceTaskNum, len(kvs)*nMap)

			capacity := len(kvs) * nMap

			// Don't allocate anything too absurd. 100GB uses at most 10-11M entries.
			if capacity > 11000000 {
				log.Printf("[REDUCER #%d] Capping capacity to 11,000,000 for inputs slice.\n", reduceTaskNum)
				capacity = 11000000
			}

			inputs = make([]KeyValue, 0, capacity)

			// Now that we've received data for the first time, flip this to true.
			receivedDataForTheFirstTime = true
		}

		inputs = append(inputs, kvs...)

		log.Printf("[REDUCER #%d] Finished adding decoded data to inputs list. Added %d entries.\n", reduceTaskNum, len(kvs))
	}

	log.Printf("[REDUCER #%d] Sorting the inputs. There are %d inputs to sort.\n", reduceTaskNum, len(inputs))

	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Key < inputs[j].Key })

	fileName := serverless.MergeName(jobName, reduceTaskNum)

	results := make([]KeyValue, len(inputs), len(inputs))

	// Calculate five percent of the inputs. We'll print an update every five percent
	// during reduce operations (e.g., 0% done, 5% done, 10% done, ...., 90% done, 95% done, 100% done).
	five_percent := int(len(inputs) * 0.05)

	doReduce := func(k string, v []string, i int, max int) {
		//log.Printf("[REDUCER #%d] Reduce %d/%d: key = \"%s\"...\n", reduceTaskNum, i, max, k)
		output := reduceF(k, v)
		//log.Printf("[REDUCER #%d] Reduce() for key \"%s\" SUCCESS.\n", reduceTaskNum, k)
		new_kv := new(KeyValue)
		new_kv.Key = k
		new_kv.Value = output

		// Print a message every increment of 5%.
		if i % five_percent == 0 {
			percent_done := float64(i) / float64(max)
			log.Printf("Completed %d PERCENT (%d/%d) of REDUCE operations.\n", i, max, percent_done)
		}

		// The value of i passed is actually one higher than it should be. This is because we basically process
		// the key from the LAST iteration of the for-loop calling doReduce. On the first (0th) iteration of the
		// for-loop, we don't call doReduce, we just set value of lastKey. Then on second iteration, we call
		// doReduce for the FIRST (0th) input. 
		results[i-1] = *new_kv // = append(results, *new_kv)
	}

	log.Printf("[REDUCER #%d] Performing Reduce() function now. There are %d inputs.\n", reduceTaskNum, len(inputs))

	var lastKey string
	values := make([]string, 0, 10)
	num_inputs := len(inputs)
	for i, kv := range inputs {
		if kv.Key != lastKey && i > 0 {
			doReduce(lastKey, values, i, num_inputs)
			values = values[:0]
		}
		lastKey = kv.Key
		values = append(values, kv.Value)
	}
	// If inputs is length 0, then lastKey was never set to a value and thus we should just skip this...
	if len(inputs) > 0 {
		log.Printf("Calling doReduce() for the last time. lastKey = \"%s\".\n", lastKey)
		doReduce(lastKey, values, len(inputs), num_inputs)
	}

	log.Printf("Completed reduce operations. Encoding the results now...\n")

	var byte_buffer bytes.Buffer
	gobEncoder := gob.NewEncoder(&byte_buffer)
	err := gobEncoder.Encode(results)
	checkError(err)
	marshalled_result := byte_buffer.Bytes() // should be of type []byte now.

	//marshalled_result, err := json.Marshal(results)
	//checkError(err)
	log.Printf("Results encoded successfully. Writing final result to Redis at key \"%s\". Size: %f MB.\n", fileName, float64(len(marshalled_result))/float64(1e6))

	/* Chunk up the final results if necessary. */
	if len(marshalled_result) > int(chunkThreshold) {
		log.Printf("Data for final result \"%s\" is larger than %d bytes. Storing it in pieces...\n", fileName, chunkThreshold)
		//log.Printf("md5 of marshalled_result with key \"%s\": %x\n", fileName, md5.Sum(marshalled_result))
		chunks := split(marshalled_result, int(chunkThreshold))
		num_chunks := len(chunks)
		log.Printf("Created %d chunks for final result.\n", num_chunks)
		base_key := fileName + "-part"
		counter := 0
		for _, chunk := range chunks {
			chunk_key := base_key + strconv.Itoa(counter)
			log.Printf("Writing chunk #%d with key \"%s\" (size = %f MB) to storage.\n", counter, chunk_key, float64(len(chunk))/float64(1e6))
			//log.Printf("md5 of chunk with key \"%s\": %x\n", chunk_key, md5.Sum(chunk))
			start := time.Now()

			// The exponentialBackoffWrite encapsulates the Set/Write procedure with exponential backoff.
			// I put it in its own function bc there are several write calls in this file and I did not
			// wanna reuse the same code in each location.
			success := exponentialBackoffWrite(chunk_key, chunk, cli)
			//success := exponentialBackoffWrite(chunk_key, chunk)

			end := time.Now()
			writeEnd := time.Since(start)
			//checkError(err)
			if !success {
				log.Fatal("\n\nERROR while storing value in storage, key is: \"", chunk_key, "\"")
			}

			log.Printf("SUCCESSFULLY wrote chunk #%d, \"%s\", to storage. Size: %f MB, Time: %v ms.\n", counter, chunk_key, float64(len(chunk))/float64(1e6), writeEnd.Nanoseconds()/1e6)

			rec := IORecord{TaskNum: reduceTaskNum, RedisKey: chunk_key, Bytes: len(chunk), Start: start.UnixNano(), End: end.UnixNano()}
			ioRecords = append(ioRecords, rec)

			//log.Printf("Chunk \"%s\":\n%s", chunk_key, string(chunk))

			// readAllCloser2, ok := cli.Get(chunk_key)

			// if !ok {
			// 	log.Fatal("Got error reading back chunk \"", chunk_key, "\" after writing it.\n")
			// }

			// marshalled_result2, err2 := readAllCloser2.ReadAll()
			// checkError(err2)

			// log.Printf("md5 of chunk \"%s\" read back immediately after writing: %x\n", chunk_key, md5.Sum(marshalled_result2))

			counter = counter + 1
		}
		var byte_buffer bytes.Buffer
		gobEncoder := gob.NewEncoder(&byte_buffer)
		err3 := gobEncoder.Encode(num_chunks)
		checkError(err3)
		numberOfChunksSerialized := byte_buffer.Bytes() // should be of type []byte now.

		//numberOfChunksSerialized, err3 := json.Marshal(num_chunks)
		checkError(err3)

		// The exponentialBackoffWrite encapsulates the Set/Write procedure with exponential backoff.
		// I put it in its own function bc there are several write calls in this file and I did not
		// wanna reuse the same code in each location.
		success := exponentialBackoffWrite(fileName, numberOfChunksSerialized, cli)
		//success := exponentialBackoffWrite(fileName, numberOfChunksSerialized)
		if !success {
			log.Fatal("ERROR while storing value in storage, key is: \"", fileName, "\"")
		}
		checkError(err)
	} else {
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f MB\n", fileName, float64(len(marshalled_result))/float64(1e6))
		start := time.Now()

		// The exponentialBackoffWrite encapsulates the Set/Write procedure with exponential backoff.
		// I put it in its own function bc there are several write calls in this file and I did not
		// wanna reuse the same code in each location.
		success := exponentialBackoffWrite(fileName, marshalled_result, cli)
		//success := exponentialBackoffWrite(fileName, marshalled_result)
		if !success {
			log.Fatal("ERROR while storing value in storage with key \"", fileName, "\"")
		}
		end := time.Now()
		writeEnd := time.Since(start)

		log.Printf("storage WRITE END. Key: \"%s\", Size: %f, Time: %d ms \n", fileName, float64(len(marshalled_result))/float64(1e6), writeEnd.Nanoseconds()/1e6)

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

	// Close the client when we're done with it.
	// cli.Close()
	clientPool.Put(cli)
}

// Encapsulates a write operation. Currently, this is an InfiniStore write operation.
func exponentialBackoffWrite(key string, value []byte, ecClient *client.Client) bool {
	//func exponentialBackoffWrite(key string, value []byte) bool {
	success := false
	for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
		log.Printf("Attempt %d/%d for write key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, key)
		// Call the EcSet InfiniStore function to store 'value' at key 'key'.
		_, ok := ecClient.EcSet(key, value)

		if !ok {
			max_duration := (2 << uint(current_attempt+4)) - 1
			if max_duration > serverless.MaxBackoffSleepWrites {
				max_duration = serverless.MaxBackoffSleepWrites
			}
			duration := rand.Intn(max_duration + 1)
			log.Printf("[ERROR] Failed to write key \"%s\". Backing off for %d ms.\n", key, duration)
			time.Sleep(time.Duration(duration) * time.Millisecond)
		} else {
			log.Printf("Successfully wrote key \"%s\" on attempt %d.\n", key, current_attempt)
			success = true
			break
		}
	}

	return success
}

func (s srtrService) ClosePool() error {
	if clientPool != nil {
		log.Printf("Closing the srtm_service client pool...")
		clientPool.Close()
	}

	return nil
}

// DON'T MODIFY THIS FUNCTION
func (s srtrService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		log.Printf("Sort: Failed to decode!\n")
		return err
	}
	log.Printf("REDUCER for Reducer Task # \"%d\"\n", args.TaskNum)
	
	// Make sure only one worker at a time can check this in order to ensure that the pool has been created.
	poolLock.Lock() 
	if !poolCreated {
		log.Printf("Initiating client pool now. Pool size = %d.\n", args.ClientPoolCapacity)
		InitPool(args.DataShards, args.ParityShards, args.MaxGoroutines, args.StorageIPs, args.ClientPoolCapacity)

		poolCreated = true
	}
	poolLock.Unlock()

	// if !clientCreated {
	// 	CreateInfiniStoreClient(args.TaskNum, args.DataShards, args.ParityShards, args.MaxGoroutines)
	// }

	// if !clientDialed {
	// 	DialInfiniStoreClient(args.TaskNum, args.StorageIPs)
	// }

	doReduceDriver(args.JobName, args.StorageIPs, args.TaskNum, args.NOthers, args.DataShards, args.ParityShards, args.MaxGoroutines, args.ChunkThreshold)

	return nil
}

var Interface srtrService
