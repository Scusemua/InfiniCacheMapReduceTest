#!/bin/bash
DRIVER_HOST="ec2-18-212-213-199.compute-1.amazonaws.com:1234"

# The first command-line argument should be the file of S3 keys. 
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt

go run client.go $DRIVER_HOST srt 10 sample_data.dat $1 &
pids[0]=$!

echo "[Test]: waiting for client and workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

rm mrtmp.* mr.srt-res* 