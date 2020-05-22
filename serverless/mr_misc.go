package serverless

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/lafikl/consistent"
	"log"
	"os"
	"reflect"
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
		checkError(err2)

		typeOfResult := reflect.TypeOf(result)

		if typeOfResult == "int": 
			log.Println("Obtained integer for final result. Result must've been chunked.")

			all_bytes := make([]byte,0)
			base_key = p + "-part"
			for i := 0; i < result; i++ {
				key := base_key + string(i)

				res, err2 := client.Get(p).Result()
				checkError(err2)

				all_bytes = append(all_bytes, result...)
			}

			result = all_bytes

			log.Println("Final size of all chunks combined together:", float64(len(result)) / float64(1e6), "MB")

		log.Println("Successfully retrieved data from Redis!")

		if err != nil {
			log.Fatal("Merge: ", err)
		}

		results := make([]KeyValue, 0)

		log.Println("Unmarshalling data retrieved from Redis now...")
		json.Unmarshal([]byte(result), &results)

		for _, kv := range results {
			kvs[kv.Key] = kv.Value
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
