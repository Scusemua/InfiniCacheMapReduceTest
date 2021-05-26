package serverless

import (
	"bufio"
	"bytes"
	"crypto/md5"

	//"encoding/json"
	"encoding/gob"
	"fmt"

	//"github.com/go-redis/redis/v7"
	//"github.com/Scusemua/PythonGoBridge"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"

	"github.com/mason-leap-lab/infinicache/client"

	//"strings"
	"time"
)

// merge combines the results of the many reduce jobs into a single
// output file XXX use merge sort
//func (drv *Driver) merge(redisHostnames []string, dataShards int, parityShards int, maxGoRoutines int) {
func (drv *Driver) merge(
	storageIps []string,
	dataShards int,
	parityShards int,
	maxGoRoutines int,
	usePocket bool,
) {
	Debug("Merge phase\n")
	now := time.Now()

	// redis_client := redis.NewClient(&redis.Options{
	// 	Addr:         "127.0.0.1:6378",
	// 	Password:     "",
	// 	DB:           0,
	// 	ReadTimeout:  30 * time.Second,
	// 	WriteTimeout: 30 * time.Second,
	// 	MaxRetries:   3,
	// })

	// Create new InfiniStore client.
	cli := client.NewClient(dataShards, parityShards, maxGoRoutines)
	//var addrList = "127.0.0.1:6378"
	//addrArr := strings.Split(addrList, ",")
	//storageIps2 := []string{"10.0.109.88:6378", "10.0.121.202:6378"}
	log.Printf("Creating storage client for IPs: %v\n", storageIps)
	cli.Dial(storageIps)

	kvs := make(map[string]string)
	for i := 0; i < drv.nReduce; i++ {
		p := MergeName(drv.jobName, i)
		log.Printf("Merge: reading from InfiniStore: %s\n", p)

		log.Printf("InfiniStore READ START. Key: \"%s\"\n", p)
		start := time.Now()
		reader, success := readExponentialBackoff(p, cli)

		//if err2 != nil {
		if !success {
			log.Printf("ERROR: Storage encountered exception for key \"%s\".\n", p)
			log.Fatal("Cannot create sorted file if data is missing.")
		}

		if reader == nil {
			log.Printf("WARNING: Key \"%s\" does not exist.\n", p)
			log.Printf("Skipping for now...")
			continue
		}

		result, err2 := reader.ReadAll()
		reader.Close()

		if err2 != nil {
			log.Printf("ERROR: Storage encountered exception when calling ReadAll for key \"%s\"...\n", p)
			log.Fatal(err2)
		}

		firstReadDuration := time.Since(start)

		var res_int int // If encoding to []KeyValue fails, we'll try to decode an int in case everything was chunked.
		results := make([]KeyValue, 0)

		log.Println("Unmarshalling data retrieved from storage now...")

		//log.Printf("md5 of data with key \"%s\": %x\n", p, md5.Sum(result))

		byte_buffer_res := bytes.NewBuffer(result)
		gobDecoder := gob.NewDecoder(byte_buffer_res)
		err := gobDecoder.Decode(&results)

		// Try to deserialize into a list of KeyValue. If it breaks, then try to deserialize to an int.
		// If that works, then eveyrthing was chunked so grab all the pieces and combine them.
		// err := json.Unmarshal([]byte(result), &results)

		if err != nil {
			byte_buffer_res := bytes.NewBuffer(result)
			gobDecoder := gob.NewDecoder(byte_buffer_res)
			err = gobDecoder.Decode(&res_int)
			//err = json.Unmarshal([]byte(result), &res_int)

			if err != nil {
				panic(err)
			} else {
				log.Printf("Obtained integer %d for final result. Result must've been chunked.\n", res_int)

				all_bytes := make([]byte, 0)
				base_key := p + "-part"
				for i := 0; i < res_int; i++ {
					key := base_key + strconv.Itoa(i)

					log.Printf("storage READ CHUNK START. Key: \"%s\", Chunk #: %d.\n", key, i)
					chunkStart := time.Now()
					reader, success := readExponentialBackoff(key, cli)
					if reader == nil || !success {
						log.Printf("ERROR: storage encountered exception for key \"%s\". This occurred while retrieving chunks.\n", key)
					}
					res, err2 := reader.ReadAll()
					reader.Close()
					readDuration := time.Since(chunkStart)
					checkError(err2)

					log.Printf("storage READ CHUNK END. Key: \"%s\", Chunk #: %d, Bytes read: %f, Time: %d ms, md5: %x\n",
						key, i, float64(len(res))/float64(1e6), readDuration.Nanoseconds()/1e6, md5.Sum(res))

					//log.Printf("md5 of chunk with key \"%s\": %x\n", key, md5.Sum(all_bytes))

					//log.Printf("Chunk \"%s\":\n%s\n", key, string(res))

					all_bytes = append(all_bytes, res...)
				}

				//log.Printf("md5 of all bytes for key \"%s\": %x\n", p, md5.Sum(all_bytes))

				log.Printf("Final size of all %d chunks for key \"%s\" combined together: %f MB. md5: %x\n",
					res_int, p, float64(len(all_bytes))/float64(1e6), md5.Sum(all_bytes))

				//log.Printf("All data for \"%s\":\n%s\n", p, string(all_bytes))

				byte_buffer_res := bytes.NewBuffer(all_bytes)
				gobDecoder := gob.NewDecoder(byte_buffer_res)
				err := gobDecoder.Decode(&results)
				//err = json.Unmarshal(all_bytes, &results)

				if err != nil {
					log.Fatal("Merge: ", err)
					panic(err)
				} else {
					log.Println("Successfully retrieved data from storage!\n")
					for _, kv := range results {
						kvs[kv.Key] = kv.Value
					}
				}
			}
		} else {
			log.Printf("storage READ END. Key: \"%s\", Bytes read: %f, Time: %d ms\n", p, float64(len(result))/float64(1e6), firstReadDuration.Nanoseconds()/1e6)
			for _, kv := range results {
				kvs[kv.Key] = kv.Value
			}
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	log.Println("There are", len(keys), "keys in the data retrieved from storage.\n")
	sort.Strings(keys)

	file, err := os.Create("mr-final." + drv.jobName + ".out")
	if err != nil {
		log.Fatal("Merge: create ", err)
	}
	w := bufio.NewWriter(file)
	for _, k := range keys {
		fmt.Fprintf(w, "%s\n", kvs[k])
	}
	since := time.Since(now)
	log.Printf("Merge phase took %d ms.\n", since/1e6)
	w.Flush()
	file.Close()
}

func readExponentialBackoff(key string, cli *client.Client) (client.ReadAllCloser, bool) {
	var readAllCloser client.ReadAllCloser
	var ok bool
	success := false
	// Exponential backoff.
	for current_attempt := 0; current_attempt < MaxAttemptsDuringBackoff; current_attempt++ {
		log.Printf("Attempt %d/%d for read to key \"%s\".\n", current_attempt, MaxAttemptsDuringBackoff, key)
		// IOHERE - This is a read (key is the key, it is a string).
		readAllCloser, ok = cli.Get(key)

		// Check for failure, and backoff exponentially on-failure.
		if !ok {
			max_duration := (2 << uint(current_attempt)) - 1
			duration := rand.Intn(max_duration + 1)
			log.Printf("[ERROR] Failed to read key \"%s\". Backing off for %d ms.\n", key, duration)
			time.Sleep(time.Duration(duration) * time.Millisecond)
		} else {
			readAllCloserSizeMB := float64(readAllCloser.Len()) / float64(1e6)
			log.Printf("Successfully read data with key \"%s\" on attempt %d. Size = %f MB.\n", key, current_attempt, readAllCloserSizeMB)
			success = true
			break
		}
	}

	return readAllCloser, success
}
