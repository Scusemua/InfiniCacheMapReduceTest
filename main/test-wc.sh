#!/bin/bash
DATE=`date "+%Y%m%d%H%M"`

START=`date +"%Y-%m-%d %H:%M:%S"`

GET_IP_CMD="curl http://169.254.169.254/latest/meta-data/local-ipv4"

IP=$(eval $GET_IP_CMD)

echo "========== GREP =========="
echo "Launching MapReduce client."

# Commandline Arguments 
# (1) The S3 key file (OPTIONAL, defaults to 1MB problem size).

# A POSIX variable
OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our variables:
KEY_FILE=/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt # S3 key of input data.
NUM_WORKERS=5

while getopts "h?fn:" opt; do
    case "$opt" in
    h|\?)
        echo "-f <key_file> for s3 key file (input data stored in AWS S3)"
        echo "-n <num_workers> number of workers"
        exit 0
        ;;
    f)  KEY_FILE=$OPTARG
        ;;
    n)  NUM_WORKERS=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

echo "Command-line Arguments: KEY_FILE='$KEY_FILE', NUM_WORKERS=$NUM_WORKERS"

go run client.go -driverHostname 127.0.0.1:1234 -jobName wc -nReduce 10 -sampleDataKey sample_data.dat -s3KeyFile "$KEY_FILE" -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps "127.0.0.1:6378" & 
pids[0]=$!

echo "Client launched."

port=1235

echo "Launching $NUM_WORKERS workers..."

for ((i = 1;i<=$NUM_WORKERS;i++)); do
    echo "Launching worker #$i @ $IP:$port, driver @ 127.0.0.1:1234"
    /usr/local/go/bin/go run worker.go $IP:$port "127.0.0.1:1234" 999999 & 
    pids[$i]=$!
    port=$((port + 1))
done 

echo "[Test]: waiting for client and worker to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

# clean up generated intermediate and output files
rm mrtmp.* mr.srt-res* #mr-final.srt.out

END=`date +"%Y-%m-%d %H:%M:%S"`

echo "Start time: $START"
echo "End time: $END"