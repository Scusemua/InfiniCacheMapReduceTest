// MapReduce Reduce function for GREP.

package main

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"math/rand"

	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"

	//"github.com/Scusemua/PythonGoBridge"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mason-leap-lab/infinicache/client"
)

type greprService string

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

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		log.Printf(format, a...)
	}
	return 0, nil
}

// The reduce function is called once for each key generated by Map,
// with a list of that key's string value (merged across all inputs).
// The return value should be a single output value for that key.
func reduceF(key string, values []string) string {
	//log.Printf("Reducing key=\"%s\", values = %v\n", key, values)
	res := strings.Join(values, "\n")
	//log.Printf("Result: %v\n", res)
	return res
}

func (s greprService) ClosePool() error {
	if clientPool != nil {
		log.Printf("Closing the srtm_service client pool...")
		clientPool.Close()
	}

	return nil
}

// Used to chunk up the final results to prevent writing huge blocks of data at once.
// This was more relevant for Redis, as Redis had a hard-limit for key/value size (512MB, I think?).
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
	usePocket bool,
) {
	//log.Println("Creating storage client for storage ", storageIps)
	//cli := client.NewClient(dataShards, parityShards, maxEcGoroutines)
	//cli.Dial(storageIps)

	//log.Println("Successfully created storage client")

	log.Printf("[REDUCER #%d] Getting storage client from client pool now...\n", reduceTaskNum)
	cli := clientPool.Get().(*client.Client)
	log.Printf("[REDUCER #%d] Successfully created storage client\n", reduceTaskNum)

	ioRecords := make([]IORecord, 0)

	log.Printf("Retrieving input data for reduce task #%d\n", reduceTaskNum)
	inputs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		// nMap is the number of Map tasks (i.e., the number of S3 keys or # of initial data partitions).
		// So the variable i here refers to the associated Map task/initial data partition, or where
		// the data we're processing came from, essentially.
		dataKey := serverless.ReduceName(jobName, i, reduceTaskNum)

		var kvs []KeyValue
		var readStart time.Time
		log.Printf("storage READ START. Key: \"%s\", Reduce Task #: %d.", dataKey, reduceTaskNum)
		var readAllCloser client.ReadAllCloser
		var ok bool
		success := false
		// Exponential backoff.
		for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
			log.Printf("Attempt %d/%d for read key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, dataKey)
			readStart = time.Now()
			// IOHERE - This is a read (dataKey is the key, it is a string).
			readAllCloser, ok = cli.Get(dataKey)

			// Check for failure, and backoff exponentially on-failure.
			if !ok {
				max_duration := (2 << uint(current_attempt)) - 1
				duration := rand.Intn(max_duration + 1)
				log.Printf("[ERROR] Failed to read key \"%s\". Backing off for %d ms.\n", dataKey, duration)
				time.Sleep(time.Duration(duration) * time.Millisecond)
			} else {
				log.Printf("Successfully read data with key \"%s\" on attempt %d.\n", dataKey, current_attempt)
				success = true
				break
			}
		}

		if readAllCloser == nil {
			log.Printf("\nWARNING: ReadAllCloser returned by ECClient.Get() for key %s is null. Skipping...\n", dataKey)
			continue
		}

		if !success {
			log.Fatal("ERROR: Failed to retrieve data from storage with key \"" + dataKey + "\" in allotted number of attempts.\n")
		}

		encoded_result, err := readAllCloser.ReadAll()
		readAllCloser.Close()
		if err != nil {
			log.Printf("ERROR: storage encountered exception for key \"%s\"...", "addr", dataKey)
			log.Printf("ERROR: Just skipping the key \"%s\"...", dataKey)

			// In theory, there was just no task mapped to this Reducer for this value of i. So just move on...
			continue
		}
		readEnd := time.Now()
		readDuration := time.Since(readStart)
		log.Printf("storage READ END. Key: \"%s\", Reduce Task #: %d, Bytes read: %f, Time: %d ms", dataKey, reduceTaskNum, float64(len(encoded_result))/float64(1e6), readDuration.Nanoseconds()/1e6)
		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: dataKey, Bytes: len(encoded_result), Start: readStart.UnixNano(), End: readEnd.UnixNano()}
		ioRecords = append(ioRecords, rec)
		byte_buffer_res := bytes.NewBuffer(encoded_result)
		gobDecoder := gob.NewDecoder(byte_buffer_res)
		err = gobDecoder.Decode(&kvs)
		for _, kv := range kvs {
			inputs = append(inputs, kv)
		}
	}

	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Key < inputs[j].Key })

	fileName := serverless.MergeName(jobName, reduceTaskNum)

	var results []KeyValue

	doReduce := func(k string, v []string) {
		output := reduceF(k, v)
		new_kv := new(KeyValue)
		new_kv.Key = k
		new_kv.Value = output
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

	var byte_buffer bytes.Buffer
	gobEncoder := gob.NewEncoder(&byte_buffer)
	err := gobEncoder.Encode(results)
	checkError(err)
	encoded_result := byte_buffer.Bytes()

	log.Printf("Writing final result to Redis at key \"%s\". Size: %f MB. md5: %x\n",
		fileName, float64(len(encoded_result))/float64(1e6), md5.Sum(encoded_result))

	chunk_threshold := 512 * 1e6

	/* Chunk up the final results if necessary. */
	if len(encoded_result) > int(chunk_threshold) {
		log.Printf("Final result is larger than %d MB. Storing it in pieces.\n", int(chunk_threshold/1e6))
		chunks := split(encoded_result, int(chunk_threshold))
		num_chunks := len(chunks)
		log.Println("Created", num_chunks, " chunks for final result", fileName)
		base_key := fileName + "-part"
		for i, chunk := range chunks {
			key := base_key + string(i)
			log.Printf("storage WRITE CHUNK START. Chunk #: %d, Key: \"%s\", Size: %f MB. md5: %x\n", i, key, float64(len(chunk))/float64(1e6), md5.Sum(chunk))
			success, writeStart := exponentialBackoffWrite(key, chunk, cli)
			writeDuration := time.Since(writeStart)
			writeEnd := time.Now()
			//checkError(err)
			if !success {
				log.Fatal("\n\nERROR while storing value in storage, key is: \"", key, "\"")
			}
			log.Printf("storage WRITE CHUNK END. Chunk #: %d, Key: \"%s\", Size: %f, Time: %v ms. md5: %x\n",
				i, key, float64(len(chunk))/float64(1e6), writeDuration.Nanoseconds()/1e6, md5.Sum(chunk))

			rec := IORecord{TaskNum: reduceTaskNum, RedisKey: key, Bytes: len(chunk), Start: writeStart.UnixNano(), End: writeEnd.UnixNano()}
			ioRecords = append(ioRecords, rec)
		}
		var byte_buffer bytes.Buffer
		gobEncoder := gob.NewEncoder(&byte_buffer)
		err3 := gobEncoder.Encode(num_chunks)
		checkError(err3)
		numberOfChunksSerialized := byte_buffer.Bytes()

		// Store number of chunks at original key.
		success, writeStart := exponentialBackoffWrite(fileName, numberOfChunksSerialized, cli)

		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: fileName, Bytes: len(numberOfChunksSerialized), Start: writeStart.UnixNano(), End: time.Now().UnixNano()}
		ioRecords = append(ioRecords, rec)

		if !success {
			log.Fatal("ERROR while storing value in storage, key is: \"", fileName, "\"")
		}
		checkError(err)
	} else {
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f MB\n", fileName, float64(len(encoded_result))/float64(1e6))

		success, writeStart := exponentialBackoffWrite(fileName, encoded_result, cli)
		if !success {
			log.Fatal("ERROR while storing value in storage with key \"", fileName, "\"")
		}
		//checkError(err)
		writeDuration := time.Since(writeStart)
		writeEnd := time.Now()

		log.Printf("storage WRITE END. Key: \"%s\", Size: %f, Time: %d ms \n", fileName, float64(len(encoded_result))/float64(1e6), writeDuration.Nanoseconds()/1e6)

		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: fileName, Bytes: len(encoded_result), Start: writeStart.UnixNano(), End: writeEnd.UnixNano()}
		ioRecords = append(ioRecords, rec)
	}

	f2, err2 := os.Create("IOData/reduce_io_data_" + jobName + strconv.Itoa(reduceTaskNum) + ".dat")
	checkError(err2)
	defer f2.Close()
	for _, rec := range ioRecords {
		_, err := f2.WriteString(fmt.Sprintf("%d\t%s\t%d\t%v\t%v\n", rec.TaskNum, rec.RedisKey, rec.Bytes, rec.Start, rec.End))
		checkError(err2)
	}

	//cli.Close()
	clientPool.Put(cli)
}

// Return bool indicating success as well as the value of time.Now() when
// either the successful write operation began or when the last write operation (that ended
// up ultimately failing) began.
func exponentialBackoffWrite(key string, value []byte, cli *client.Client) (bool, time.Time) {
	success := false
	var writeStart time.Time
	for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
		log.Printf("Attempt %d/%d for write key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, key)
		writeStart = time.Now()
		// IOHERE - This is a write (key is the key, it is a string, value is the value, it is []byte).
		_, ok := cli.EcSet(key, value)

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

	return success, writeStart
}

// DON'T MODIFY THIS FUNCTION
func (s greprService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		log.Printf("Sort: Failed to decode!\n")
		return err
	}
	log.Printf("Grep Reducer for Reducer Task # \"%d\"\n", args.TaskNum)

	if args.UsePocket {
		log.Printf("=-=-= USING POCKET FOR INTERMEDIATE DATA STORAGE =-=-=\n")
	} else {
		log.Printf("=-=-= USING INFINISTORE FOR INTERMEDIATE DATA STORAGE =-=-=\n")
	}

	poolLock.Lock()
	if !poolCreated && args.UsePocket {
		log.Printf("Initiating client pool now. Pool size = %d.\n", args.ClientPoolCapacity)
		InitPool(args.DataShards, args.ParityShards, args.MaxGoroutines, args.StorageIPs, args.ClientPoolCapacity)

		poolCreated = true
	}
	poolLock.Unlock()

	doReduce(args.JobName, args.StorageIPs, args.TaskNum, args.NOthers, args.DataShards,
		args.ParityShards, args.MaxGoroutines, args.UsePocket)

	return nil
}

var Interface greprService
