package main

import (
	"github.com/go-redis/redis/v7"
	"github.com/mason-leap-lab/infinicache/client"
	//"math/rand"
	//"io/ioutil"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	//"io/ioutil"
	//"bytes"
	//"os"
)

var addr = "127.0.0.1:6378"

func main() {
	// initial object with random value
	//var val []byte
	//val = make([]byte, 1024)
	//rand.Read(val)

	//fmt.Println(val)

	marshalled_result, err := json.Marshal(5)
	if err != nil {
		panic(err)
	}

	// parse server address
	addrArr := strings.Split(addr, ",")

	redis_client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     "",
		DB:           0,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		MaxRetries:   3,
	})

	// initial new ecRedis client
	cli := client.NewClient(10, 2, 32)

	// start dial and PUT/GET
	cli.Dial(addrArr)
	cli.EcSet("foo", marshalled_result)
	_, reader, ok := cli.EcGet("foo", 0)

	if ok == false {
		panic("Internal error!")
	}

	buf, err := reader.ReadAll()
	fmt.Println("buf:", buf)

	if err != nil {
		panic(err)
	}

	fmt.Println("Unmarshalling now...")
	var v int64
	json.Unmarshal([]byte(buf), &v)
	fmt.Println(v)

	fmt.Println("Now trying with redis library go-redis...")

	marshalled_result2, err := json.Marshal(100)
	if err != nil {
		panic(err)
	}

	err = redis_client.Set("testKey", marshalled_result2, 0).Err()
	fmt.Println("Finished calling redis_client.Set().")

	marshalled_result3, err := redis_client.Get("testKey").Result()
	if err != nil {
		panic(err)
	}

	fmt.Println("Unmarshalling now... #2")
	var v2 int64
	json.Unmarshal([]byte(marshalled_result3), &v2)
	fmt.Println(v)
}
