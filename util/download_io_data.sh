#!/bin/bash

# Download IOData files from the VM at the given IP address.
# Command-line Arguments
# (1) Experiment prefix so we can put these in a folder locally
# (2) IP of VM
# (3) Keypath for SSH key

PREFIX=$1
IP=$2
KEYPATH=$3

mkdir iodata/$PREFIX
mkdir iodata/$PREFIX$IP

scp -i $KEYPATH -r ubuntu@$IP:/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main/IOData ./iodata/$PREFIX$IP/