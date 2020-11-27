#!/bin/bash
#DRIVER_HOST="ec2-18-212-213-199.compute-1.amazonaws.com:1234"

GET_IP_CMD="curl http://169.254.169.254/latest/meta-data/local-ipv4"

IP=$(eval $GET_IP_CMD)

echo "Got IP: $IP"

DRIVER_HOST="$IP:1234"

# The first command-line argument is nReducers.
# The second command-line argument should be the file of S3 keys. 

# Examples for second argument:
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt

go run client.go $DRIVER_HOST:1234 srt $1 sample_data.dat $2 10 2 32 &
#go run client.go $DRIVER_HOST:1234 srt 10 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt 10 2 32 &
#go run client.go $DRIVER_HOST srt $1 sample_data.dat $2 &
pids[0]=$!

echo "[Test]: waiting for client and workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

rm mrtmp.* mr.srt-res* 