//////////////////////////////////////////////////////////////////////
//
// G00616949
//
// Paul McKerley
//
// CS675 Spring 2020 -- Lab2
//
// Performs map actions for inverted index service.
//
//////////////////////////////////////////////////////////////////////
package main

import (
	"bytes"
	"cs675-spring20-labs/lab2/serverless"
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
// go build --buildmode=plugin -o iim_service.so iim_service.go

const debugEnabled = false

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

// Define Inverted Indexing's map service
type iimService string

// MapReduceArgs defines this plugin's argument format
type MapReduceArgs struct {
	JobName string
	InFile  string
	TaskNum int
	NReduce int
	NOthers int
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

	// map to track which words we've seen already
	seen := make(map[string]bool)

	// Split document up into words on non-word boundaries.
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if !unicode.IsLetter(r) {
			return true
		}
		return false
	}) {
		_, ok := seen[s]

		if !ok {
			res = append(res, KeyValue{s, document})
			seen[s] = true
		}
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
func (s iimService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Inverted Indexing Service: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from inverted indexing service plugin: %s\n", args.InFile)

	doMap(args.JobName, args.InFile, args.TaskNum, args.NReduce)

	return nil
}

var Interface iimService
