//////////////////////////////////////////////////////////////////////
//
// Performs map actions for sort.
//
//////////////////////////////////////////////////////////////////////
package main

import (
	"bytes"
	"cs675-spring20-labs/lab2/serverless"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"io/ioutil"
	"log"
	//"os"
	"strings"
)

const debugEnabled = true

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// To compile the map plugin: run:
// go build --buildmode=plugin -o srtr_service.so srtr_service.go
// go build --buildmode=plugin -o srtm_service.so srtm_service.go

// Define Inverted Indexing's map service
type srtmService string

// MapReduceArgs defines this plugin's argument format
type MapReduceArgs struct {
	JobName    string
	InFile     string
	TaskNum    int
	NReduce    int
	NOthers    int
	SampleKeys []string
}

type KeyValue struct {
	Key   string
	Value string
}

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being
// processed, and the value is the file's contents. The return value
// should be a slice of key/value pairs, each represented by a
// mapreduce.KeyValue.
func mapF(document string, value string) (res []KeyValue) {
	//var arr []string
	// Split up string line-by-line.
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if r == '\n' {
			return true
		}
		return false
	}) {
		//arr = append(arr, s)
		res = append(res, KeyValue{s[0:10], s})
	}
	//Debug("\narr:\n%s\n", strings.Join(arr, ","))
	//res = append(res, KeyValue{document, arr})
	//Debug("\nRes:\n%s\n", res)
	return res
}

// doMap does the job of a map worker: it reads one of the input
// files (inFile), calls the user-defined function (mapF) for that
// file's contents, and partitions the output into nReduce
// intermediate files.
func doMap(
	jobName string,
	inFile string,
	taskNum int,
	nReduce int,
	trie serverless.TrieNode,
) {
	//reduceEncoders := make([]*gob.Encoder, nReduce)

	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	// for i := 0; i < nReduce; i++ {
	// 	fileName := serverless.ReduceName(jobName, taskNum, i)
	// 	//Debug("Creating %s\n", fileName)
	// 	f, err := os.Create(fileName)
	// 	checkError(err)
	// 	defer f.Close()
	// 	//enc := json.NewEncoder(f)
	// 	//reduceEncoders[i] = enc
	// }

	var err error
	var b []byte

	//Debug("Reading %s\n", inFile)
	b, err = ioutil.ReadFile(inFile)
	checkError(err)

	results := make(map[string][]KeyValue)
	for _, result := range mapF(inFile, string(b)) {
		reducerNum := ihash(result.Key, trie) % nReduce
		redisKey := serverless.ReduceName(jobName, taskNum, reducerNum)
		results[redisKey] = append(results[redisKey], result)
	}

	for k, v := range results {
		marshalled_result, err := json.Marshal(v)
		checkError(err)
		//fmt.Printf("Storing result containing %d KeyValue structs in Redis at key \"%s\".\n", len(v), k)
		//fmt.Println("Contents of the list:")
		// for _, r := range v {
		// 	fmt.Println(r)
		// }
		err = client.Set(k, marshalled_result, 0).Err()
		checkError(err)
	}
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string, trie serverless.TrieNode) int {
	partition := serverless.GetPartition(s, trie)
	//fmt.Printf("Partition for key \"%s\": %d\n", s, partition)
	return partition
	//h := fnv.New32a()
	//h.Write([]byte(s))
	//return int(h.Sum32() & 0x7fffffff)
}

// DON'T MODIFY THIS FUNCTION
func (s srtmService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		//fmt.Printf("Sort: Failed to decode!\n")
		return err
	}
	//fmt.Printf("Hello from sort service plugin: %s\n", args.InFile)

	//fmt.Println("About to execute doMap()...")

	//fmt.Printf("Sample keys: %s\n", strings.Join(args.SampleKeys, ","))

	//fmt.Println("Constructing the Trie now...")
	trie := serverless.BuildTrie(args.SampleKeys, 0, len(args.SampleKeys), "", 2)

	doMap(args.JobName, args.InFile, args.TaskNum, args.NReduce, trie)

	return nil
}

var Interface srtmService
