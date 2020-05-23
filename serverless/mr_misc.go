package serverless

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/lafikl/consistent"
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

	c := consistent.New()
	clientMap := make(map[string]*redis.Client)

	log.Println("Populating hash ring and Redis client map now...")

	// Add the IP addresses of the Reds instances to the ring.
	// Create the Redis clients and store them in the map.
	for _, hostname := range redisHostnames {
		// Add hostname to hash ring.
		c.Add(hostname)

		log.Println("Creating Redis client for Redis listening at", hostname)

		// Create client.
		// TODO: If still getting errors with workers not finding data in Redis, we could
		// try sorting the list of redis endpoints before placing them into consistent hash ring.
		client := redis.NewClient(&redis.Options{
			Addr:         hostname,
			Password:     "",
			DB:           0,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			MaxRetries:   3,
		})

		// Store client in map.
		clientMap[hostname] = client
	}

	kvs := make(map[string]string)
	for i := 0; i < drv.nReduce; i++ {
		p := MergeName(drv.jobName, i)
		log.Printf("Merge: reading from Redis: %s\n", p)
		//file, err := os.Open(p)

		// Read result from Redis.
		// Previously, we would be reading the result from a file on-disk.
		host, err := c.Get(p)
		checkError(err)
		client := clientMap[host]
		result, err2 := client.Get(p).Result()
		if err2 != nil {
			log.Printf("ERROR: Redis @ %s encountered exception for key \"%s\"...", host, p)
			log.Fatal(err2)
		}

		var res_int int
		results := make([]KeyValue, 0)

		log.Println("Unmarshalling data retrieved from Redis now...")

		// Try to deserialize into a list of KeyValue. If it breaks, then try to deserialize to an int.
		// If that works, then eveyrthing was chunked so grab all the pieces and combine them.
		err = json.Unmarshal([]byte(result), &results)

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

					host, err5 := c.Get(key)
					checkError(err5)
					client := clientMap[host]

					log.Printf("Attempting to read chunk #%d from Redis @ %s, key: %s\n", i, host, key)
					res, err2 := client.Get(key).Result()
					if err2 != nil {
						log.Printf("ERROR: Redis @ %s encountered exception for key \"%s\". This occurred while retrieving chunks...", host, key)
						log.Fatal(err2)
					}
					checkError(err2)

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
			log.Println("Successfully retrieved data from Redis!")
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
