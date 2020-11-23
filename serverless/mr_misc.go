package serverless

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/mason-leap-lab/infinicache/client"
	"log"
	"os"
	"sort"
	"time"
)

// merge combines the results of the many reduce jobs into a single
// output file XXX use merge sort
func (drv *Driver) merge(redisHostnames []string, dataShards int, parityShards int, maxGoRoutines int) {
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
	cli.Dial("127.0.0.1:6378")

	kvs := make(map[string]string)
	for i := 0; i < drv.nReduce; i++ {
		p := MergeName(drv.jobName, i)
		log.Printf("Merge: reading from InfiniStore: %s\n", p)

		log.Printf("InfiniStore READ START. Key: \"%s\", InfiniStore Hostname: %s.", p, "127.0.0.1:6378")
		start := time.Now()
		//result, err2 := redis_client.Get(p).Result()
		reader, ok := cli.Get(p)
		result, err2 := reader.ReadAll()
		reader.Close()

		//if err2 != nil {
		if !ok {
			log.Printf("ERROR: InfiniStore @ %s encountered exception for key \"%s\"...", "127.0.0.1:6378", p)
			//log.Fatal(err2)
		}

		if err != nil {
			log.Printf("ERROR: InfiniStore @ %s encountered exception when calling ReadAll for key \"%s\"...", "127.0.0.1:6378", p)
			log.Fatal(err2)
		}

		firstReadDuration := time.Since(start)

		var res_int int
		results := make([]KeyValue, 0)

		log.Println("Unmarshalling data retrieved from InfiniStore now...")

		// Try to deserialize into a list of KeyValue. If it breaks, then try to deserialize to an int.
		// If that works, then eveyrthing was chunked so grab all the pieces and combine them.
		err := json.Unmarshal([]byte(result), &results)

		if err != nil {
			err = json.Unmarshal([]byte(result), &res_int)

			if err != nil {
				panic(err)
			} else {
				log.Println("Obtained integer for final result. Result must've been chunked.")

				all_bytes := make([]byte, 0)
				base_key := p + "-part"
				for i := 0; i < res_int; i++ {
					key := base_key + string(i)

					log.Printf("InfiniStore READ CHUNK START. Key: \"%s\", InfiniStore Hostname: %s, Chunk #: %d.", key, "127.0.0.1:6378", i)
					chunkStart := time.Now()
					//res, err2 := redis_client.Get(key).Result()
					reader, ok := cli.Get(key)
					if !ok {
						log.Printf("ERROR: InfiniStore @ %s encountered exception for key \"%s\". This occurred while retrieving chunks...", "127.0.0.1:6378", key)
					}
					res, err2 := reader.ReadAll()
					reader.Close()
					readDuration := time.Since(chunkStart)
					if err2 != nil {
						log.Printf("ERROR: InfiniStore @ %s encountered exception for key \"%s\". This occurred while calling ReadAll...", "127.0.0.1:6378", key)
						log.Fatal(err2)
					}
					checkError(err2)

					log.Printf("InfiniStore READ CHUNK END. Key: \"%s\", InfiniStore Hostname: %s, Chunk #: %d, Bytes read: %f, Time: %d ms", key, "127.0.0.1:6378", i, float64(len(res))/float64(1e6), readDuration.Nanoseconds()/1e6)

					all_bytes = append(all_bytes, []byte(res)...)
				}

				log.Println("Final size of all chunks combined together:", float64(len(all_bytes))/float64(1e6), "MB")

				err = json.Unmarshal([]byte(all_bytes), &results)

				if err != nil {
					log.Fatal("Merge: ", err)
					panic(err)
				} else {
					log.Println("Successfully retrieved data from InfiniStore!")
					for _, kv := range results {
						kvs[kv.Key] = kv.Value
					}
				}
			}
		} else {
			log.Printf("InfiniStore READ END. Key: \"%s\", InfiniStore Hostname: %s, Bytes read: %f, Time: %d ms", p, "127.0.0.1:6378", float64(len(result))/float64(1e6), firstReadDuration.Nanoseconds()/1e6)
			for _, kv := range results {
				kvs[kv.Key] = kv.Value
			}
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	log.Println("There are", len(keys), "keys in the data retrieved from InfiniStore.")
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
	log.Printf("Merge phase took %d ms.", since/1e6)
	w.Flush()
	file.Close()
}
