#!/bin/bash

#GET_IP_CMD="dig +short myip.opendns.com @resolver1.opendns.com"
GET_IP_CMD="curl http://169.254.169.254/latest/meta-data/local-ipv4"

# -- Command Line Arguments --
# (1) The hostname of the Client in the form of a string "<IP>:<PORT>".
# (2) The number of workers to launch (integer).
# (3) The starting port for the worker (integer).

IP=$(eval $GET_IP_CMD)

echo "Got IP: $IP"

num_workers=$2
port=1235

echo "Launching $2 workers..."

for ((i = 1;i<=num_workers;i++)); do
    echo "Launching worker to listen @ $IP:$port, driver @ $1"
    /usr/local/go/bin/go run worker.go $IP:$port $1 999999 & 
    pids[$i]=$!
    port=$((port + 1))
done 

# $1 contains driver hostname (i.e., <ip>:<port>)

# Start the workers
# /usr/local/go/bin/go run worker.go $IP:1235 $1 999999 & 
# pids[0]=$!

# /usr/local/go/bin/go run worker.go $IP:1236 $1 999999 & 
# pids[2]=$!

# /usr/local/go/bin/go run worker.go $IP:1237 $1 999999 & 
# pids[3]=$!

# /usr/local/go/bin/go run worker.go $IP:1238 $1 999999 & 
# pids[4]=$!

# /usr/local/go/bin/go run worker.go $IP:1239 $1 999999 & 
# pids[5]=$!

echo "[Test]: waiting for workers to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

# clean up generated intermediate and output files
rm mrtmp.* mr.srt-res*