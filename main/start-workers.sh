#!/bin/bash

#GET_IP_CMD="dig +short myip.opendns.com @resolver1.opendns.com"
GET_IP_CMD="curl http://169.254.169.254/latest/meta-data/local-ipv4"

IP=$(eval $GET_IP_CMD)

echo "Got IP: $IP"

DRIVER_HOST="ec2-18-212-213-199.compute-1.amazonaws.com:1234"

# Start the workers
go run worker.go $IP:1235 $DRIVER_HOST 999999 & 
pids[0]=$!

go run worker.go $IP:1236 $DRIVER_HOST 999999 & 
pids[2]=$!

go run worker.go $IP:1237 $DRIVER_HOST 999999 & 
pids[3]=$!

go run worker.go $IP:1238 $DRIVER_HOST 999999 & 
pids[4]=$!

go run worker.go $IP:1239 $DRIVER_HOST 999999 & 
pids[5]=$!

echo "[Test]: waiting for workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

# clean up generated intermediate and output files
rm mrtmp.* mr.srt-res*