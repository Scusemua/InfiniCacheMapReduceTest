package serverless

import (
	//"log"
	//"fmt"
	"encoding/gob"
	"fmt"
	"os"
	"os/exec"
)

const FILEPATH = "intermediate_data/"
const FILE_EXTENSION = ".gob"

func RegisterJob(jobName string, numLambdas int, capacityGB int, peakMbps int, latencySensitive int) string {
	python_command := fmt.Sprintf("import pocket; print pocket.register_job(%s, num_lambdas=%d, capacityGB=%d, peakMbps=%d, latency_sensitive=%d",
		jobName, numLambdas, capacityGB, peakMbps, latency_sensitive)
	cmd := exec.Command("python3", "-c", python_command)
	fmt.Println(cmd.Args)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(string(out))

	// TODO: Adjust this, as out will be the entire output of the Pocket
	// function call rather than JUST the jobId, which is what we want.
	return string(out)
}

// Steps:
// Create local file.
// Write data to local file.
// Use Pocket's PutFile API to store the file.
func Set(key string, value []byte, jobid string) {
	filename := FILEPATH + key + FILE_EXTENSION

	// Create local file!
	file, _ := os.Create(filename)
	// Create gob encoder!
	encoder := gob.NewEncoder(file)
	// Encode data to file!
	encoder.Encode(value)
	// Close the file explicitly so we can hand it over to Pocket!
	file.Close()

	//python_command := fmt.Sprintf("import pocket; print pocket.put(jobName, numLambdas, capacityGB, peakMbps, latency_sensitive)
	//cmd := exec.Command("python3", "-c", python_command)
}
