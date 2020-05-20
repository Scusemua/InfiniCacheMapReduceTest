#!/bin/bash
DRIVER_HOST="ec2-18-212-213-199.compute-1.amazonaws.com:1234"

go run client.go $DRIVER_HOST srt 10 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt &
pids[0]=$!

echo "[Test]: waiting for client and workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

rm mrtmp.* mr.srt-res* 