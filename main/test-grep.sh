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
KEY_FILE=/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt # S3 key of input data.
PATTERN="[a-zA-Z]+" # Regular expression pattern for grep.
NUM_WORKERS=1

while getopts "h?vf:" opt; do
    case "$opt" in
    h|\?)
        echo "-p <pattern> for regex pattern, -f <key_file> for s3 key file (input data stored in AWS S3)"
        exit 0
        ;;
    p)  PATTERN=$OPTARG
        ;;
    f)  KEY_FILE=$OPTARG
        ;;
    n)  NUM_WORKERS=$OPTARG
        ;;
    esac
done

shift $((OPTIND-1))

[ "${1:-}" = "--" ] && shift

echo "Command-line Arguments:\nPATTERN=$PATTERN, KEY_FILE='$KEY_FILE', NUM_WORKERS=$NUM_WORKERS"

go run client.go -driverHostname 127.0.0.1:1234 -jobName grep -nReduce 10 -sampleDataKey sample_data.dat -s3KeyFile "$KEY_FILE" -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps "127.0.0.1:6378" & 
#go run client.go localhost:1234 srt 10 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt 10 2 32 &
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

# echo "Launching worker #1."

# go run worker.go localhost:1235 localhost:1234 999999 & 
# pids[1]=$!

# echo "Launching worker #2."

# go run worker.go localhost:1236 localhost:1234 999999 & 
# pids[2]=$!

# echo "Launching worker #3."

# go run worker.go localhost:1237 localhost:1234 999999 & 
# pids[3]=$!

# echo "Launching worker #4."

# go run worker.go localhost:1238 localhost:1234 999999 & 
# pids[4]=$!

# echo "Launching worker #5."

# go run worker.go localhost:1239 localhost:1234 999999 & 
# pids[5]=$!vals

echo "[Test]: waiting for client and worker to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

#echo "[Test]: assert(mr-final.sort.out)" > /dev/stderr

#sort -n -k2 mr-final.sort.out | tail -10 | diff - mr-testout.txt > diff.out
#if [ -s diff.out ]
#then
#echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
#  cat diff.out
#else
#  echo "Passed test!" > /dev/stderr
#fi

# clean up generated intermediate and output files
rm mrtmp.* mr.srt-res* #mr-final.srt.out

END=`date +"%Y-%m-%d %H:%M:%S"`

echo "Start time: $START"
echo "End time: $END"