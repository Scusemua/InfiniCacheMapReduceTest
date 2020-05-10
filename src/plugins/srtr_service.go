//////////////////////////////////////////////////////////////////////
//
// Performs reduce actions for sort.
//
//////////////////////////////////////////////////////////////////////

package main

import (
	"bytes"
	"cs675-spring20-labs/lab2/serverless"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"
)

// To compile the map plugin: run:
// go build --buildmode=plugin -o srtr_service.so srtr_service.go
// go build --buildmode=plugin -o srtm_service.so srtm_service.go

// Define Inverted Indexing's reduce service
type srtrService string

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
	Value []string
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const debugEnabled = true

func Debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func mergeSort(arr []string) []string {
	var arr_length = len(arr)

	if arr_length == 1 {
		return arr
	}

	midpoint := int(arr_length / 2)

	return merge(mergeSort(arr[:midpoint]), mergeSort(arr[midpoint:]))
}

func merge(left []string, right []string) (merged []string) {
	// Store merged values in here.
	merged = make([]string, len(left)+len(right))

	Debug("Merging... (len(left) = %d, len(right) = %d)\n", len(left), len(right))

	i := 0

	for len(left) > 0 && len(right) > 0 {
		// The values are the rows from gensort, which are "<actual string>  <string index>  <checksum>"
		left_split := strings.Split(left[0], "  ")
		right_split := strings.Split(right[0], "  ")

		if left_split[0] < right_split[0] {
			merged[i] = left[0]
			left = left[1:]
		} else {
			merged[i] = right[0]
			right = right[1:]
		}
		i++
	}

	// Add remaining elements from left/right to the merged array.
	for j := 0; j < len(left); j++ {
		merged[i] = left[j]
		i++
	}
	for j := 0; j < len(right); j++ {
		merged[i] = right[j]
		i++
	}

	return merged
}

// The reduce function is called once for each key generated by Map,
// with a list of that key's string value (merged across all inputs).
// The return value should be a single output value for that key.
func reduceF(key string, values [][]string) []string {
	// Just sort and count the documents. Output as value.
	//sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	Debug("Unsorted values:\n")
	for i, s := range values[0] {
		Debug("%d: %s\n", i, s)
	}

	// Perform merge sort.
	sorted := mergeSort(values[0])

	Debug("Merge-sort completed. Sorted values:\n")
	for _, s := range sorted {
		Debug("%s\n", s)
	}
	return sorted //fmt.Sprintf("%s", strings.Join(sorted, ","))
}

// doReduce does the job of a reduce worker: it reads the
// intermediate key/value pairs (produced by the map phase) for this
// task, sorts the intermediate key/value pairs by key, calls the
// user-defined reduce function (reduceF) for each key, and writes
// the output to disk. Each reduce generates an output file named
// using serverless.MergeName(jobName, reduceTask).
func doReduce(
	jobName string,
	reduceTaskNum int,
	nMap int,
) {
	inputs := make([]*KeyValue, 0)
	for i := 0; i < nMap; i++ {
		fileName := serverless.ReduceName(jobName, i, reduceTaskNum)
		Debug("fileName = %s\n", fileName)
		f, err := os.Open(fileName)
		checkError(err)
		defer f.Close()
		dec := json.NewDecoder(f)

		for {
			kv := new(KeyValue)
			if err := dec.Decode(&kv); err == io.EOF {
				break
			}
			checkError(err)
			inputs = append(inputs, kv)
		}
	}
	sort.Slice(inputs, func(i, j int) bool { return inputs[i].Key < inputs[j].Key })

	fileName := serverless.MergeName(jobName, reduceTaskNum)
	Debug("Creating %s\n", fileName)
	f, err := os.Create(fileName)
	checkError(err)
	defer f.Close()
	enc := json.NewEncoder(f)

	doReduce := func(enc *json.Encoder, k string, v [][]string) {
		output := reduceF(k, v)
		new_kv := new(KeyValue)
		new_kv.Key = k
		new_kv.Value = output
		Debug("Output:\n%s\n", output)
		err = enc.Encode(&new_kv)
		checkError(err)
	}

	var lastKey string
	values := make([][]string, 0)
	for i, kv := range inputs {
		if kv.Key != lastKey && i > 0 {
			doReduce(enc, lastKey, values)
			values = make([][]string, 0)
		}
		lastKey = kv.Key
		values = append(values, kv.Value)
	}
	doReduce(enc, lastKey, values)
}

// DON'T MODIFY THIS FUNCTION
func (s srtrService) DoService(raw []byte) error {
	var args MapReduceArgs
	buf := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&args)
	if err != nil {
		fmt.Printf("Sort: Failed to decode!\n")
		return err
	}
	fmt.Printf("Hello from sort plugin: %s\n", args.InFile)

	doReduce(args.JobName, args.TaskNum, args.NOthers)

	return nil
}

var Interface srtrService
