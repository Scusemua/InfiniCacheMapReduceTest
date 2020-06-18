package serverless

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/serialx/hashring"
	"log"
	"os"
	"sort"
	"time"
)

// merge combines the results of the many reduce jobs into a single
// output file XXX use merge sort
func (drv *Driver) merge(redisHostnames []string) {
	Debug("Merge phase\n")
	now := time.Now()

	redis_client := redis.NewClient(&redis.Options{
		Addr:         "127.0.0.1:6378",
		Password:     "",
		DB:           0,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   3,
	})

	kvs := make(map[string]string)
	for i := 0; i < drv.nReduce; i++ {
		p := MergeName(drv.jobName, i)
		log.Printf("Merge: reading from Redis: %s\n", p)

		log.Printf("REDIS READ START. Key: \"%s\", Redis Hostname: %s.", p, host)
		start := time.Now()
		result, err2 := redis_client.Get(p).Result()
		firstReadDuration := time.Since(start)
		if err2 != nil {
			log.Printf("ERROR: Redis @ %s encountered exception for key \"%s\"...", host, p)
			log.Fatal(err2)
		}

		var res_int int
		results := make([]KeyValue, 0)

		log.Println("Unmarshalling data retrieved from Redis now...")

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

					log.Printf("REDIS READ CHUNK START. Key: \"%s\", Redis Hostname: %s, Chunk #: %d.", key, host, i)
					chunkStart := time.Now()
					res, err2 := redis_client.Get(key).Result()
					readDuration := time.Since(chunkStart)
					if err2 != nil {
						log.Printf("ERROR: Redis @ %s encountered exception for key \"%s\". This occurred while retrieving chunks...", host, key)
						log.Fatal(err2)
					}
					checkError(err2)

					log.Printf("REDIS READ CHUNK END. Key: \"%s\", Redis Hostname: %s, Chunk #: %d, Bytes read: %f, Time: %d ms", key, host, i, float64(len(res))/float64(1e6), readDuration.Nanoseconds()/1e6)

					all_bytes = append(all_bytes, []byte(res)...)
				}

				log.Println("Final size of all chunks combined together:", float64(len(all_bytes))/float64(1e6), "MB")

				err = json.Unmarshal([]byte(all_bytes), &results)

				if err != nil {
					log.Fatal("Merge: ", err)
					panic(err)
				} else {
					log.Println("Successfully retrieved data from Redis!")
					for _, kv := range results {
						kvs[kv.Key] = kv.Value
					}
				}
			}
		} else {
			log.Printf("REDIS READ END. Key: \"%s\", Redis Hostname: %s, Bytes read: %f, Time: %d ms", p, host, float64(len(result))/float64(1e6), firstReadDuration.Nanoseconds()/1e6)
			for _, kv := range results {
				kvs[kv.Key] = kv.Value
			}
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	log.Println("There are", len(keys), "keys in the data retrieved from Redis.")
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
