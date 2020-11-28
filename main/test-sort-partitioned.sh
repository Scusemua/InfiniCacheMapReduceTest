#!/bin/bash
DATE=`date "+%Y%m%d%H%M"`

START=`date +"%Y-%m-%d %H:%M:%S"`

echo "Launching MapReduce client."

go run client.go -driverHostname 127.0.0.1:1234 -jobName srt -nReduce 10 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps "127.0.0.1:6378"
#go run client.go localhost:1234 srt 10 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt 10 2 32 &
pids[0]=$!

echo "Client launched."
echo "Launching worker #1."

go run worker.go localhost:1235 localhost:1234 999999 & 
pids[1]=$!

echo "Launching worker #2."

go run worker.go localhost:1236 localhost:1234 999999 & 
pids[2]=$!

echo "Launching worker #3."

go run worker.go localhost:1237 localhost:1234 999999 & 
pids[3]=$!

echo "Launching worker #4."

go run worker.go localhost:1238 localhost:1234 999999 & 
pids[4]=$!

echo "Launching worker #5."

go run worker.go localhost:1239 localhost:1234 999999 & 
pids[5]=$!vals

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