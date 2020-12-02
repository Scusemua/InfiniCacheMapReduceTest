//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Performs map actions for word count service.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"bytes"
	"github.com/Scusemua/InfiniCacheMapReduceTest/serverless"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"unicode"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o wcm_service.so wcm_service.go

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

// Define Word Count's map service
type wcmService string

// MapReduceArgs defines this plugin's argument format
// type MapReduceArgs struct {
	// 	JobName       string
	// 	S3Key         string
	// 	TaskNum       int
	// 	NReduce       int
	// 	NOthers       int
	// 	SampleKeys    []string
	// 	StorageIPs    []string
	// 	DataShards    int
	// 	ParityShards  int
	// 	MaxGoroutines int
	// 	Pattern 	  string 
	// }

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
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if !unicode.IsLetter(r) {
			return true
		}
		return false
	}) {
		res = append(res, KeyValue{s, "1"})
	}
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
) {
	reduceEncoders := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		fileName := serverless.ReduceName(jobName, taskNum, i)
		Debug("Creating %s\n", fileName)
		f, err := os.Create(fileName)
		checkError(err)
		defer f.Close()
		enc := json.NewEncoder(f)
		reduceEncoders[i] = enc
	}

	var err error
	var b []byte

	// Read in whole file. Not scalable, but OK for a
	// lab.
	Debug("Reading %s", inFile)
	b, err = ioutil.ReadFile(inFile)
	checkError(err)

	for _, result := range mapF(inFile, string(b)) {
		reducerNum := ihash(result.Key) % nReduce
		err = reduceEncoders[reducerNum].Encode(&result)
		checkError(err)
	}
}

// We supply you an ihash function to help with mapping of a given
// key to an intermediate file.
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

// DON'T MODIFY THIS FUNCTION
func (s wcmService) DoService(raw []byte) error {
	var args serverless.MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		log.Printf("Word Count Service: Failed to decode!\n")
		return err
	}
	log.Printf("Hello from wordCountService plugin: %s\n", args.S3Key)

	doMap(args.JobName, args.S3Key, args.TaskNum, args.NReduce)

	return nil
}

var Interface wcmService
