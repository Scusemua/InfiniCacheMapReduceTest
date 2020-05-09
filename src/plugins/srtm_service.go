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
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

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

// To compile the map plugin: run:
// go build --buildmode=plugin -o srtr_service.so srtr_service.go
// go build --buildmode=plugin -o srtm_service.so srtm_service.go

// Define Inverted Indexing's map service
type srtmService string

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
	// Split document up into words on non-word boundaries.
	for _, s := range strings.FieldsFunc(value, func(r rune) bool {
		if r == '\n' {
			return true
		}
		return false
	}) {
		sep := strings.Split(s, "  ")
		res = append(res, KeyValue{sep[0], sep[1] + "  " + sep[2]})
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
func (s srtmService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Sort: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from sort service plugin: %s\n", args.InFile)

	doMap(args.JobName, args.InFile, args.TaskNum, args.NReduce)

	return nil
}

var Interface srtmService
