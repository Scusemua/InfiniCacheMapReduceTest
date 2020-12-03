// MapReduce Reduce function for GREP.

package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"math/rand"
	"github.com/mason-leap-lab/infinicache/client"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
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
) {
	log.Println("Creating storage client for storage ", storageIps)
	cli := client.NewClient(dataShards, parityShards, maxEcGoroutines)
	cli.Dial(storageIps)

	log.Println("Successfully created storage client")

	ioRecords := make([]IORecord, 0)

	log.Printf("Retrieving input data for reduce task #%d\n", reduceTaskNum)
	inputs := make([]KeyValue, 0)
	for i := 0; i < nMap; i++ {
		// nMap is the number of Map tasks (i.e., the number of S3 keys or # of initial data partitions).
		// So the variable i here refers to the associated Map task/initial data partition, or where
		// the data we're processing came from, essentially.
		dataKey := serverless.ReduceName(jobName, i, reduceTaskNum)

		var kvs []KeyValue
		start := time.Now()
		log.Printf("storage READ START. Key: \"%s\", Reduce Task #: %d.", dataKey, reduceTaskNum)
		var readAllCloser client.ReadAllCloser
		var ok bool
		success := false
		// Exponential backoff.
		for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
			log.Printf("Attempt %d/%d for read key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, dataKey)
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

		marshalled_result, err := readAllCloser.ReadAll()
		readAllCloser.Close()
		if err != nil {
			log.Printf("ERROR: storage encountered exception for key \"%s\"...", "addr", dataKey)
			log.Printf("ERROR: Just skipping the key \"%s\"...", dataKey)
			
			// In theory, there was just no task mapped to this Reducer for this value of i. So just move on...
			continue
		}
		end := time.Now()
		readDuration := time.Since(start)
		log.Printf("storage READ END. Key: \"%s\", Reduce Task #: %d, Bytes read: %f, Time: %d ms", dataKey, reduceTaskNum, float64(len(marshalled_result))/float64(1e6), readDuration.Nanoseconds()/1e6)
		rec := IORecord{TaskNum: reduceTaskNum, RedisKey: dataKey, Bytes: len(marshalled_result), Start: start.UnixNano(), End: end.UnixNano()}
		ioRecords = append(ioRecords, rec)
		json.Unmarshal([]byte(marshalled_result), &kvs)
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
			//_, ok := cli.EcSet(key, chunk)
			success := exponentialBackoffWrite(key, chunk, cli)
			end := time.Now()
			writeEnd := time.Since(start)
			//checkError(err)
			if !success {
				log.Fatal("\n\nERROR while storing value in storage, key is: \"", key, "\"")
			}
			log.Printf("storage WRITE CHUNK END. Chunk #: %d, Key: \"%s\", Size: %f, Time: %v ms \n", i, key, float64(len(chunk))/float64(1e6), writeEnd.Nanoseconds()/1e6)

			rec := IORecord{TaskNum: reduceTaskNum, RedisKey: key, Bytes: len(chunk), Start: start.UnixNano(), End: end.UnixNano()}
			ioRecords = append(ioRecords, rec)
		}
		num_chunks_serialized, err3 := json.Marshal(num_chunks)
		checkError(err3)
		//err := redis_client.Set(fileName, num_chunks_serialized, 0).Err()
		//_, ok := cli.EcSet(fileName, num_chunks_serialized)
		success := exponentialBackoffWrite(fileName, num_chunks_serialized, cli)
		if !success {
			log.Fatal("ERROR while storing value in storage, key is: \"", fileName, "\"")
		}
		checkError(err)
	} else {
		log.Printf("storage WRITE START. Key: \"%s\", Size: %f MB\n", fileName, float64(len(marshalled_result))/float64(1e6))
		start := time.Now()
		//err := redis_client.Set(fileName, marshalled_result, 0).Err()
		//_, ok := cli.EcSet(fileName, marshalled_result)
		success := exponentialBackoffWrite(fileName, marshalled_result, cli)
		if !success {
			log.Fatal("ERROR while storing value in storage with key \"", fileName, "\"")
		}
		//checkError(err)
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

	cli.Close()
}

func exponentialBackoffWrite(key string, value []byte, cli *client.Client) bool {
	success := false
	for current_attempt := 0; current_attempt < serverless.MaxAttemptsDuringBackoff; current_attempt++ {
		log.Printf("Attempt %d/%d for write key \"%s\".\n", current_attempt, serverless.MaxAttemptsDuringBackoff, key)
		_, ok := cli.EcSet(key, value)

		if !ok {
			max_duration := (2 << uint(current_attempt + 4)) - 1
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

	doReduce(args.JobName, args.StorageIPs, args.TaskNum, args.NOthers, args.DataShards, args.ParityShards, args.MaxGoroutines)

	return nil
}

var Interface greprService
