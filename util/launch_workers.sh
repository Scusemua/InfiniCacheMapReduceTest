#!/bin/bash
echo "========= WORKERS ========="
echo "Launching MapReduce workers."

# A POSIX variable
#OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our variables:
MAPREDUCE_ROOT_DIRECTORY=$1 
CLIENT_ADDR=$2 
NUM_WORKERS=$3 
KEYFILE_PATH=$4 
HOST_IP=$5
USERNAME=$6

echo "MAPREDUCE_ROOT_DIRECTORY = $MAPREDUCE_ROOT_DIRECTORY"
echo "CLIENT_ADDR = $CLIENT_ADDR"
echo "NUM_WORKERS = $NUM_WORKERS"
echo "KEYFILE_PATH = $KEYFILE_PATH"
echo "HOST_IP = $HOST_IP"
echo "USERNAME = $USERNAME"

SSH_COMMAND="cd $MAPREDUCE_ROOT_DIRECTORY/main/;export PATH=\$PATH:/usr/local/go/bin;./start-workers.sh $CLIENT_ADDR $NUM_WORKERS > /dev/null 2>&1 &"

echo "Preparing to execute the following command:"
echo "$SSH_COMMAND"

ssh -i $KEYFILE_PATH $USERNAME@$HOST_IP "$SSH_COMMAND"

#$SHELL # Uncomment this to prevent script from closing terminal after executing, at least on windows