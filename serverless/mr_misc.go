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

	fmt.Println("Populating hash ring and Redis client map now...")

	// Add the IP addresses of the Reds instances to the ring.
	// Create the Redis clients and store them in the map.
	for _, hostname := range redisHostnames {
		// Add hostname to hash ring.
		c.Add(hostname)

		fmt.Println("Creating Redis client for Redis listening at", hostname)

		// Create client.
		client := redis.NewClient(&redis.Options{
			Addr:     hostname,
			Password: "",
			DB:       0,
		})

		// Store client in map.
		clientMap[hostname] = client
	}

	kvs := make(map[string]string)
	for i := 0; i < drv.nReduce; i++ {
		p := MergeName(drv.jobName, i)
		fmt.Printf("Merge: read %s\n", p)
		//file, err := os.Open(p)

		host, err := c.Get(p)
		checkError(err)
		client := clientMap[host]
		marshalled_result, err2 := client.Get(p).Result()

		fmt.Println("Successfully retrieved data from Redis!")

		checkError(err2)

		if err != nil {
			log.Fatal("Merge: ", err)
		}

		fmt.Println("Unmarshalling data retrieved from Redis now...")
		json.Unmarshal([]byte(marshalled_result), &kvs)
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	fmt.Println("There are", len(keys), "keys in the data retrieved from Redis.")
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
	fmt.Printf("Merge phase took %d ms.", since/1e6)
	w.Flush()
	file.Close()
}
