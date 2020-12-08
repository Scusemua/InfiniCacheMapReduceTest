#!/bin/bash

# Launch two independent jobs -- grep and sort -- on the MapReduce framework.

INTERVAL=60
FIRST_JOB="grep"

while getopts "h?if:" opt; do
    case "$opt" in
    h|\?)
        echo "-i <integer>, the time in betweet starting the first job and the second job."
        echo "-f <string>, where <string> is either \"grep\" or \"sort\". This specifies which job goes first."
        exit 0
        ;;
    i)  INTERVAL=$OPTARG
        ;;
    f)  FIRST_JOB=$OPTARG
        ;;
    esac
done

echo "Interval between two jobs: $INTERVAL seconds."
echo "First job = $FIRST_JOB"

echo "Starting Job #1"

if [ "$FIRST_JOB" = "grep" ]; then 
    echo "Launching GREP as first job."
    # Grep 100GB, runs in the background.
    go run client.go -driverHostname 10.0.116.159:1234 -jobName grep -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100GB_50Partitions_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -clientPoolCapacity 10 -storageIps 10.0.116.159:6378 -storageIps 10.0.81.136:6378 -storageIps 10.0.74.216:6378 -storageIps 10.0.70.136:6378 -storageIps 10.0.66.154:6378 -storageIps 10.0.72.93:6378 -storageIps 10.0.91.143:6378 &
else 
    echo "Launching SORT as first job."
    # TeraSort 50GB, runs in the background.
    go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/50GB_50Partitions_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -clientPoolCapacity 10 -storageIps 10.0.116.159:6378 -storageIps 10.0.81.136:6378 -storageIps 10.0.74.216:6378 -storageIps 10.0.70.136:6378 -storageIps 10.0.66.154:6378 -storageIps 10.0.72.93:6378 -storageIps 10.0.91.143:6378 &
fi 

echo "Sleeping for $INTERVAL seconds before launching second job..."

sleep $INTERVAL

echo "Starting Job #2"

if [ "$FIRST_JOB" = "sort" ]; then 
    echo "Launching GREP as second job."
    # Grep 100GB, runs in the background.
    go run client.go -driverHostname 10.0.116.159:1234 -jobName grep -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100GB_50Partitions_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -clientPoolCapacity 10 -storageIps 10.0.116.159:6378 -storageIps 10.0.81.136:6378 -storageIps 10.0.74.216:6378 -storageIps 10.0.70.136:6378 -storageIps 10.0.66.154:6378 -storageIps 10.0.72.93:6378 -storageIps 10.0.91.143:6378 &
else 
    echo "Launching SORT as second job."
    # TeraSort 50GB, runs in the background.
    go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/50GB_50Partitions_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -clientPoolCapacity 10 -storageIps 10.0.116.159:6378 -storageIps 10.0.81.136:6378 -storageIps 10.0.74.216:6378 -storageIps 10.0.70.136:6378 -storageIps 10.0.66.154:6378 -storageIps 10.0.72.93:6378 -storageIps 10.0.91.143:6378 &
fi 

echo "Both jobs have been launched."