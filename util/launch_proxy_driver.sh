#!/bin/bash
echo "============== PROXIES =============="
echo "Launching InfiniStore proxies client."

# A POSIX variable
#OPTIND=1         # Reset in case getopts has been used previously in the shell.

# Initialize our variables:
INFINISTORE_ROOT_DIRECTORY=$1 #"/home/ubuntu/project/src/github.com/mason-leap-lab/infinicache"
LAMBDA_PREFIX=$2 #"CacheNode0-"
EXPERIMENTAL_PREFIX=$3 #"202011291702/"
KEYFILE_PATH=$4 #"C:\Users\benrc\.ssh\CS484_Desktop.pem"
IP=$5 #"54.173.137.68"
USERNAME=$6 #"ubuntu"

#shift $((OPTIND-1))

#[ "${1:-}" = "--" ] && shift

echo "INFINISTORE_ROOT_DIRECTORY = $INFINISTORE_ROOT_DIRECTORY"
echo "LAMBDA_PREFIX = $LAMBDA_PREFIX"
echo "EXPERIMENTAL_PREFIX = $EXPERIMENTAL_PREFIX"
echo "KEYFILE_PATH = $KEYFILE_PATH"
echo "IP = $IP"
echo "USERNAME = $USERNAME"

SSH_COMMAND="cd $INFINISTORE_ROOT_DIRECTORY/evaluation; export PATH=\$PATH:/usr/local/go/bin;go run \$PWD/../proxy/proxy.go -debug=true -prefix=$EXPERIMENTAL_PREFIX -lambda-prefix=$LAMBDA_PREFIX -disable-color >./log 2>&1 &"

echo "Preparing to execute the following command:"
echo "$SSH_COMMAND"

ssh -i $KEYFILE_PATH $USERNAME@$IP "$SSH_COMMAND"

#$SHELL # Uncomment this to prevent script from closing terminal after executing, at least on windows