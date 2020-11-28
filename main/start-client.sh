#!/bin/bash
#DRIVER_HOST="ec2-18-212-213-199.compute-1.amazonaws.com:1234"

GET_IP_CMD="curl http://169.254.169.254/latest/meta-data/local-ipv4"

IP=$(eval $GET_IP_CMD)

echo "Got IP: $IP"

DRIVER_HOST="$IP:1234"

# Command Line Arguments
# (1) The name of the MapReduce job.
# (2) Number of MapReduce reducers.
# (3) The S3 key of the sample data to use for generating sample keys and building the trie.
# (4) File which contains the S3 keys of the input data partitions.
# (5) Data shards (InfiniStore-related)
# (6) Parity shards (InfiniStore-related)
# (7) Max goroutines (InfiniStore-related)

# Examples for second argument:
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
# /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt

go run client.go -drv $DRIVER_HOST -jobName $1 -nReduce $2 -sampleDataKey $3 -s3KeyFile $4 -dataShards $5 -parityShards $6 -maxGoRoutines $7

go run client.go $DRIVER_HOST srt $1 sample_data.dat $2 10 2 32 &
#go run client.go $DRIVER_HOST:1234 srt 10 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt 10 2 32 &
#go run client.go $DRIVER_HOST srt $1 sample_data.dat $2 &
pids[0]=$!

echo "[Test]: waiting for client and workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

rm mrtmp.* mr.srt-res* 