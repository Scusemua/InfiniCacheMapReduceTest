# I recommend copying-and-pasting these imports into your Python terminal session.
import boto3   
from datetime import datetime
import mapreduce_driver as mrd
import paramiko
try:
    from pssh.clients import ParallelSSHClient
    parallel_ssh_enabled = True 
except:
    parallel_ssh_enabled = False 

import random
import redis 
import subprocess
import time 

import importlib
rl = importlib.reload

"""
This file contains scripts that I use to orchestrate workloads on remote clusters/virtual machines.

You could write scripts to automatically deploy and run everything by running a single function, but
I typically write out the function calls with the parameters I intend to use, and then just copy-and-
paste them into a terminal.

At the very least, you need to modify the variable declarations at the top of the file to match
your setup. 

-----------------------------------------------------------------------------------------------
If you make changes to this file AFTER starting a Python interactive session, you can re-import
the module and obtain the changes. You need to use the importlib module:

import importlib
importlib.reload(mrd) # or importlib.reload(mapreduce_driver)
-----------------------------------------------------------------------------------------------

INFINISTORE_DIRECTORY is the path to the root of the InfiniStore GitHub repo. Note that this should
be under your $GOPATH environment variable. (My GOPATH is /home/ubuntu/project, I believe). Use Google
to learn about GOPATH.

KEYFILE_PATH is the path to your SSH key so Paramiko can SSH onto your VM's to execute commands.
"""

# This is the $GOPATH environment variable on your virtual machine(s).
GOPATH = "/home/ubuntu/project"

# This is the path to the root of the InfiniStore local GitHub repository on the VM.
# It should be under the GOPATH directory, which is why the GOPATH variable is used automatically.
INFINISTORE_DIRECTORY = "{}/src/github.com/mason-leap-lab/infinicache".format(GOPATH)

# This is the path to the root of the MapReduce framework GitHub repository on the VM. (So this
# repo). It should be under the GOPATH directory, which is why the GOPATH variable is used automatically.
MAPREDUCE_DIRECTORY = "{}/src/github.com/Scusemua/InfiniCacheMapReduceTest".format(GOPATH)

# The path to your keyfile.
KEYFILE_PATH = "C:\\Users\\benrc\\.ssh\\CS484_Desktop.pem"

# This is the branch to use for InfiniStore.
INFINISTORE_BRANCH = "origin/config_ben"

# This is the branch to use for the MapReduce framework.
MAPREDUCE_BRANCH = "origin/pocket"

def get_private_ips(region_name="us-east-1"):
    """
    Get the private IPv4 addresses of EC2 instances.

    Arguments:
        - The AWS region in which your VMs are running.
    Returns:
        A list of strings, where each string is a private IPv4 of an EC2 VM.
    """
    print("Getting public IPs now...")
    ec2client = boto3.client('ec2', region_name = region_name)
    response = ec2client.describe_instances()
    private_ips = list()
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            if instance["State"]["Name"] == "running":
                private_ips.append(instance["PrivateIpAddress"])
    print("Retrieved the following private IP addresses:")
    for ip in private_ips:
        print(ip)
    print("Retrieved {} IPs in total.".format(len(private_ips)))
    return private_ips

def get_ips(region_name="us-east-1"):
    """
    Get the public AND private IPv4 addresses of EC2 instances.

    Arguments:
        - The AWS region in which your VMs are running.
    Returns:
        A tuple of two lists, where the first list is the list of
        public IPs and the second list is the list of private IPs.
    """
    print("Getting public and private IPs now...")
    ec2client = boto3.client('ec2', region_name = region_name)
    response = ec2client.describe_instances()
    public_ips = list()
    private_ips = list()
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            if instance["State"]["Name"] == "running":
                public_ips.append(instance["PublicIpAddress"])
                private_ips.append(instance["PrivateIpAddress"])
    print("Retrieved the following public IP addresses:")
    for ip in public_ips:
        print(ip)
    print("Retrieved the following private IP addresses:")
    for ip in private_ips:
        print(ip)        
    print("Retrieved {} IPs in total.".format(len(public_ips) + len(private_ips)))
    return public_ips, private_ips

def get_public_ips(region_name="us-east-1"):
    """
    Get the public IPv4 addresses of EC2 instances.

    Arguments:
        - The AWS region in which your VMs are running.
    Returns:
        A list of strings, where each string is a public IPv4 of an EC2 VM.
    """    
    print("Getting public IPs now...")
    ec2client = boto3.client('ec2', region_name = region_name)
    response = ec2client.describe_instances()
    public_ips = list()
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            if instance["State"]["Name"] == "running":
                public_ips.append(instance["PublicIpAddress"])
    print("Retrieved the following public IP addresses:")
    for ip in public_ips:
        print(ip)
    print("Retrieved {} IPs in total.".format(len(public_ips)))
    return public_ips

def execute_command(
    command = None,
    count_limit = 3,
    get_pty = True,
    ips = None,
    key_path = KEYFILE_PATH
):
    """
    Execute a command-line/terminal command on a remote VM.

    Arguments:
        command (str): The full command that you wish to execute.

        count_limit (int): This is the maximum number of attempts we make to retrieve output
                           from the VM after executing our command. Each attempt takes time, and
                           sometimes you don't really care about the output, so you don't bother
                           spending time trying to retrieve it (meaning you pass, like, 1 for this).

        get_pty (bool): This corresponds to the Paramiko (SSH library) function argument of the
                        same name. When this is true, it means that you are requesting a pseudo-
                        terminal from the server. This is usually used right after creating a client
                        channel, to ask the server to provide some basic terminal semantics for a shell
                        invoked with invoke_shell. 

                        Basically, this allows us to get output from the commands we execute. This 
                        should generally be True.

                        https://www.kite.com/python/docs/paramiko.Channel.get_pty

        ips (list of strings): The IPs of all the VM's you wish to execute your command on.
                               These should be public IP's, unless you're running this script
                               from an EC2 VM, in which case you may be able to use private IP's.

        key_path: The full path to your private key-file for SSH-ing into the VM.
    """
    keyfile = paramiko.RSAKey.from_private_key_file(key_path)
    ssh_clients = list()
    print(" ")
    for ip in ips:
        ssh_client = paramiko.SSHClient()
        ssh_clients.append((ip, ssh_client))
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname = ip, username="ubuntu", pkey = keyfile) 
    counter = 1
    for IP, ssh_redis_client in ssh_clients:
        print("Executing command for instance @ {}... ({}/{})".format(IP, counter, len(ips)))
        print("get_pty:", get_pty)
        ssh_stdin, ssh_stdout, ssh_stderr = ssh_redis_client.exec_command(command, get_pty = get_pty)
        print("Executed command \"{}\"...".format(command))
        count = 0
        while not ssh_stdout.channel.eof_received:
            print("Waiting for EOF received...")
            time.sleep(1)
            count += 1
            if count >= count_limit:
                break
        if ssh_stdout.channel.eof_received:
            print("Calling ssh_stdout.channel.recv_exit_status()...")
            ssh_stdout.channel.recv_exit_status()
            print("Reading lines from stdout...")
            lines = ssh_stdout.readlines()
            print("STDOUT Lines = {}".format(lines))      
            lines2 = ssh_stderr.readlines()
            print("STDErr Lines = {}".format(lines2))  
            #if len(lines) > 0:
            #    print("Successfully launched Redis server.")
        else:
            print("ssh_stdout.channel.eof_received is still False... skipping...")
        counter += 1
    for IP, ssh_client in ssh_clients:
        print("Closing SSH client connected to IP %s" % IP)
        ssh_client.close()

def launch_redis_servers(
    connect_and_ping = True,
    count_limit = 3,
    ips = None,
    key_path = KEYFILE_PATH,
    kill_first = False ,
    shards_per_vm = 1,
    starting_port = 6379
):
    """
    Launch Redis virtual machines on VM's. This assumes that the VM has been configured a certain
    way so that redis-server is installed (I think you can install it by running either
    "apt-get install redis-server" or "yum install redis-server" on your VM if it isn't already installed.)
    
    Arguments:
        connect_and_ping (bool): If True, then this will attempt to connect to each Redis instance and
        ping it (via the Redis API) after creating the instance to ensure it is working.

        count_limit (int): Number of attempts to make in retrieving output of executing SSH commands on VM.

        ips (list of string): The IPs of the VM's on which we want to launch Redis instances.

        key_path (str): The path to your SSH key so Paramiko can execute SSH commands on the EC2 VMs.

        kill_first(bool): Attempt to kill any existing/currently-running Redis instances on each VM before
        launching the new ones.

        shards_per_vm (int): The number of Redis instances to launch on each VM. 

        starting_port (int): The first port to use when launching a Redis instance on the VM. If 
        'shards_per_vm' is greater than zero, then the second Redis instance will have port equal to
        'starting_port + 1', and so on and so forth.
    
    Returns: 
        list of str: hostnames of instances that were launched.
    """
    hostnames = []
    if kill_first:
        print("Killing existing servers first...")
        kill_command = "sudo pkill -9 redis-server"
        execute_command(
            command = kill_command,
            count_limit = 1,
            ips = ips,
            key_path = key_path
        )
    print("Shards per VM: {}".format(shards_per_vm))
    for i in range(0, shards_per_vm):
        port = starting_port + i
        redis_command = "sudo redis-server --protected-mode no --bind 0.0.0.0 --port {} --appendonly no --save \"\"".format(port)
        current_batch = ["{}:{}".format(ip, port) for ip in ips]
        hostnames.extend(current_batch)
        print("Launching Redis instances for port {}".format(port))
        execute_command(
            command = redis_command,
            count_limit = 1,
            ips = ips,
            key_path = key_path
        )
    # Connect to the servers and ping them to verify that they were actually setup properly.
    if connect_and_ping:
        count = 1
        for hostname in hostnames:
            _split = hostname.split(":")
            redis_client = redis.Redis(host = _split[0], port = _split[1], db = 0)
            print("Pinging Redis instance @ {} now... ({}/{})".format(hostname, count, len(hostnames)))
            res = redis_client.ping()
            if res is False:
                raise Exception("ERROR: Redis instance @ {} did not start correctly!".format(hostname))
            else:
                print("True")   
            count += 1
    return hostnames

def launch_client(
    client_ip = None,
    key_path = KEYFILE_PATH,
    nReducers = 10,
    #s3_key_file = "%s/util/1MB_S3Keys.txt" % MAPREDUCE_DIRECTORY
    s3_key_file = "%s/util/1MB_S3Keys.txt" % MAPREDUCE_DIRECTORY
    # %s/util/5GB_S3Keys.txt % MAPREDUCE_DIRECTORY
):
    """
    Launch a MapReduce client on the given VM.

    DOES NOT CURRENTLY WORK. I just manually launch the VM so I can see the output reliably and in
    real time. This could be easily modified to work by editing the command passed to execute_command(),
    as currently this command corresponds to an older version of the MapReduce framework.

    Arguments:
        client_ip (string): The IP address of the VM on which you wish to run a client.
        key_path (string): The path to your keyfile for SSH-ing into the VM.
        nReducers (int): This is a MapReduce argument needed by the client. This controls how many
                         reducers there are. (In MapReduce, there are Mappers and Reducers. Use Google
                         for more information on this topic.)
        s3_key_file (String): The path to the text file containing the list of S3 keys, where each
                              S3 key is for a chunk of input data. This file should be on the VM, and 
                              thus this path should be for the client VM (not the computer on which
                              you're running this script, unless they're the same VM, I guess).
    """
    pre_command = """
    . ~/.profile;
    . ~/.bashrc;
    """

    # TODO: THIS DOES NOT CURRENTLY WORK. COMMAND IS NOT FORMATTED PROPERLY.

    #post_command = "cd /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main;./start-client.sh {} {}".format(MAPREDUCE_DIRECTORY, nReducers, s3_key_file)

    # command = pre_command + post_command

    # execute_command(
    #     command = command,
    #     ips = [client_ip],
    #     key_path = key_path
    # )    
    raise NotImplementedError("Not implemented.")

def wondershape(
    ips = None,
    upload_bytes = 1024000,
    download_bytes = 1024000
):
    """
    This assumes you have wondershaper installed on the target VM. Wondershaper is used to artificially
    restrict the upload/download (Internet) speed of your VM for testing purposes.

    Arguments:
        ips (list of strings): The IPs of the VM's on which you want to run this function. 
                               Should be public IPv4's unless you are running this script on an EC2 VM. 
        upload_bytes (int): The desired upload speed in bytes.
        download_bytes (int): The desired download speed in bytes.
    """
    command = "sudo wondershaper -a eth0 -u {} -d {}".format(upload_bytes, download_bytes)
    execute_command(command = command, count_limit = 1, get_pty = True, ips = ips)

def ping_redis(
    ips = None,
    hostnames = None
):
    """
    This creates a Redis client for each of the specified VMs and executes the ping() function. Basically
    used to check if they're running.

    Arguments:
    Only pass ONE of the two arguments to this function.
        ips (list of str): These are the IPs of the VM with no ports. This assumes each VM only has one
                           Redis instance listening on port 6379.
        hostnames (list of str): List of strings of the form "<IP>:<PORT>" where each string corresponds
                                 to a Redis instance running on the VM with IP "<IP>", listening on port
                                 "<PORT>".
    """
    if hostnames is not None:
        count = 1
        for hostname in hostnames:
            _split = hostname.split(":")
            redis_client = redis.Redis(host = _split[0], port = _split[1], db = 0)
            print("Pinging Redis instance @ {} now... ({}/{})".format(hostname, count, len(hostnames)))
            res = redis_client.ping()
            if res is False:
                raise Exception("ERROR: Redis instance @ {} did not start correctly!".format(hostname))
            else:
                print("True")   
            count += 1
    else:
        count = 1
        for ip in ips:
            redis_client = redis.Redis(host = ip, port = 6379, db = 0)
            print("Pinging Redis instance @ {}:6379 now... ({}/{})".format(ip, count, len(ips)))
            res = redis_client.ping()
            if res is False:
                raise Exception("ERROR: Redis instance @ {} did not start correctly!".format(ip))
            else:
                print("True")
            count += 1

def update_redis_hosts(
    hostnames = None,
    ips = None,
    redis_ips = None,
    key_path = KEYFILE_PATH
):
    """
    Update the redis_hosts.txt file on the given VMs. Previously this file was used by the MapReduce
    framework, as the framework would read in all the hostnames from the file and use this to create
    Redis clients for each host. This feature is not currently used.

    Arguments:
        ips (list of str): The IPv4's of the VM's on which you want to update the redis host file.
        redis_ips (list of str): These are the IPs of the VM with no ports. This assumes each VM only 
                                 has one Redis instance listening on port 6379.
        hostnames (list of str): List of strings of the form "<IP>:<PORT>" where each string corresponds
                                 to a Redis instance running on the VM with IP "<IP>", listening on port
                                 "<PORT>".
        key_path (string): The path to your keyfile for SSH-ing into the VM.

        You only need to pass one of redis_ips and hostnames, not both.
    """
    print("Key path = {}".format(key_path))
    create_redis_file_command = "printf \""

    if hostnames is not None:
        for i in range(0, len(hostnames)):
            hostname = hostnames[i]
            if i == len(hostnames) - 1:
                create_redis_file_command = create_redis_file_command + hostname + "\"" 
            else:
                create_redis_file_command = create_redis_file_command + hostname + "\n"
    else:
        for i in range(0, len(redis_ips)):
            redis_ip = redis_ips[i]
            if i == len(redis_ips) - 1:
                create_redis_file_command = create_redis_file_command + redis_ip + ":6379" + "\""
            else:
                create_redis_file_command = create_redis_file_command + redis_ip + ":6379" + "\n"
    
    #create_redis_file_command = create_redis_file_command + " > /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/redis_hosts.txt"
    create_redis_file_command = create_redis_file_command + " > %s/main/redis_hosts.txt" % MAPREDUCE_DIRECTORY
    
    print("create_redis_file_command = \"{}\"".format(create_redis_file_command))

    execute_command(
        command = create_redis_file_command,
        ips = ips,
        key_path = key_path
    )

def git_status(ips, key_path = KEYFILE_PATH):
    """
    This runs 'git status' in the InfiniCache directory of 
    """
    print("Checking status now...")
    command = "cd %s; git status" % INFINISTORE_DIRECTORY
    execute_command(command, 3, get_pty = True, ips = ips, key_path = key_path)

def build_infinistore(ips, count_limit = 1, key_path = KEYFILE_PATH):
    """
    Execute the ./build.sh script in the /plugins directory of the MapReduce library on each of the
    given VMs, as specified by their IP addresses in the ips parameter.

    Arguments:
        ips (list of string): The IPs of the VM's you want to build the MapReduce framework on.

        key_path (str): Path to your SSH key.
    """
    print("Executing /plugins/build.sh for the MapReduce framework on %d VMs." % len(ips))
    command = "cd %s/evaluation; export PATH=$PATH:/usr/local/go/bin; make build" % INFINISTORE_DIRECTORY

    if parallel_ssh_enabled:
        client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")
        client.run_command(command)
        del client
    else:
        execute_command(command, count_limit, get_pty = True, ips = ips, key_path = key_path)   
    
def build_mapreduce(ips, count_limit = 1, key_path = KEYFILE_PATH):
    """
    Execute the ./build.sh script in the /plugins directory of the MapReduce library on each of the
    given VMs, as specified by their IP addresses in the ips parameter.

    Arguments:
        ips (list of string): The IPs of the VM's you want to build the MapReduce framework on.

        key_path (str): Path to your SSH key.
    """
    print("Executing /plugins/build.sh for the MapReduce framework on %d VMs." % len(ips))
    command = "cd %s/plugins; export PATH=$PATH:/usr/local/go/bin; ./build.sh" % MAPREDUCE_DIRECTORY

    if parallel_ssh_enabled:
        client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")
        client.run_command(command)
        del client
    else:
        execute_command(command, count_limit, get_pty = True, ips = ips, key_path = key_path)   

def pull_from_github_mapreduce(ips, reset_first = False, key_path = KEYFILE_PATH):
    """
    Execute 'git pull' on the MapReduce repo on the given VM's.

    Arguments:
        ips (list of string): The IPs of the VM's you want to execute 'git pull' on.

        reset_first (bool): This will just perform 'git reset --hard <BRANCH>', effectively overwriting
        any local changes. This is useful if you made local changes to the code for debugging and now
        want to reset yourself to the state of the remote branch (like the state of the branch as it
        exists on GitHub).

        key_path (str): Path to your SSH key.
    """
    #if parallel_ssh_enabled:
    #    client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")
    
    if reset_first:
        print("Resetting the MapReduce repo first, before pulling.")
        command_reset = "cd %s; git reset --hard %s" % (MAPREDUCE_DIRECTORY, MAPREDUCE_BRANCH)

    #    if parallel_ssh_enabled:
    #         client.run_command(command_reset)
            # Do not delete client here, wait until after second half of this command.
    #    else:
        execute_command(command_reset, 2, get_pty = True, ips = ips, key_path = key_path)
    
    print("Now pulling latest code from GitHub for MapReduce repo.")
    command = "cd %s; git pull" % MAPREDUCE_DIRECTORY
    #if parallel_ssh_enabled:
    #    client.run_command(command)
    #    del client
    #else:
    execute_command(command, 2, get_pty = True, ips = ips, key_path = key_path)    

def pull_from_github_infinistore(ips, reset_first = False, key_path = KEYFILE_PATH):
    """
    Execute 'git pull' on the InfiniStore repo on the given VM's.

    Arguments:
        ips (list of string): The IPs of the VM's you want to execute 'git pull' on.

        reset_first (bool): This will just perform 'git reset --hard <BRANCH>', effectively overwriting
        any local changes. This is useful if you made local changes to the code for debugging and now
        want to reset yourself to the state of the remote branch (like the state of the branch as it
        exists on GitHub).

        key_path (str): Path to your SSH key.
    """
    #if parallel_ssh_enabled:
    #    client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")

    if reset_first:
        print("Resetting the InfiniStore repo first, before pulling.")
        command_reset = "cd %s/evaluation; git reset --hard %s" % (INFINISTORE_DIRECTORY, INFINISTORE_BRANCH)

        #if parallel_ssh_enabled:
        #    client.run_command(command_reset)
            # Do not delete client here, wait until after second half of this command.
        #else:
        execute_command(command_reset, 2, get_pty = True, ips = ips, key_path = key_path)
    
    print("Now pulling latest code from GitHub for InfiniStore repo.")
    command = "cd %s/evaluation; git pull" % INFINISTORE_DIRECTORY

    #if parallel_ssh_enabled:
    #    client.run_command(command)
    #    del client
    #else:
    execute_command(command, 2, get_pty = True, ips = ips, key_path = key_path)

def launch_infinistore_proxies(ips, key_path = KEYFILE_PATH):
    """
    Launch the InfiniStore proxies on the VMs.

    Arguments:
        ips (list of string): IPs of VM's on which you'll launch proxies.

        key_path (str): Path to your SSH key.
    """
    prefix = datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M') + "/"

    # Each proxy needs a slightly different command as each proxy uses different Lambdas.
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = "CacheNode%d-" % i 
        print("Assigning lambda prefix \"%s\" to proxy at ip %s." % (lambda_prefix, ip))
        #command = "cd {}/evaluation; export PATH=$PATH:/usr/local/go/bin;go run $PWD/../proxy/proxy.go -debug=true -prefix={} -lambda-prefix={} -disable-color >./log 2>&1 &".format(INFINISTORE_DIRECTORY, prefix, lambda_prefix)
        #command = "nohup go run {}/proxy/proxy.go -debug=true -prefix={} -lambda-prefix={} -disable-color </dev/null >{}/evaluation/log 2>&1 &".format(INFINISTORE_DIRECTORY, prefix, lambda_prefix, INFINISTORE_DIRECTORY)
        #execute_command(command, 1, get_pty = True, ips = [ip], key_path = key_path)

        #command = "launch_proxy_driver.sh -i \"%s\" -l \"%s\" -e \"%s\" -p \"%s\" -u \"%s\"" % (INFINISTORE_DIRECTORY, lambda_prefix, prefix, ip, "ubuntu")
        command = "launch_proxy_driver.sh \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\"" % (INFINISTORE_DIRECTORY, lambda_prefix, prefix, key_path, ip, "ubuntu")
        print("About to execute command:\n %s" % command)
        subprocess.run(command, shell=True)
    return prefix

def export_cloudwatch_logs(ips, prefix, start, end, key_path = KEYFILE_PATH):
    """
    This executes the 'export_ubuntu.sh' script on each VM. This script exports the AWS Lambda logs of
    InfiniStore to S3.

    Arguments:
        ips (list of string): The IPv4's of the VM's on which you wish to execute 'export_ubuntu.sh'.

        prefix (string): The experimental prefix as passed to the InfiniStore proxies.

        start (string): The start time of the experiment formatted as a date '%Y-%m-%d %H:%M:%S'. Used
                        to identify which Lambda logs to export.
        
        end (string): The end time of the experiment formatted as a date '%Y-%m-%d %H:%M:%S'. Also used
                      to identify which Lambda logs to export.
    """
    command = "cd %s/evaluation/cloudwatch; ./export_ubuntu.sh %s %s %s;" % (INFINISTORE_DIRECTORY, prefix, str(start), str(end))
    execute_command(command, 3, get_pty = True, ips = ips, key_path = key_path)

def stop_infinistore_proxies(ips, key_path = KEYFILE_PATH):
    """
    Stop the InfiniStore proxies running on the given VMs (specified via their IPv4 addresses).
    """
    command = "cd %s/evaluation; make stop-server" % INFINISTORE_DIRECTORY
    execute_command(command, 1, get_pty = True, ips = ips, key_path = key_path)

# mrd.kill_go_processes(ips = worker_ips)
# mrd.kill_go_processes(ips = worker_ips + [client_ip])
# mrd.kill_go_processes(ips = [client_ip])
def kill_go_processes(
    ips = None,
    key_path = KEYFILE_PATH
):
    """
    Kills all GO processes running on the VMs (specified via their IPv4 addresses).
    """  
    kill_command = "sudo ps aux | grep go | awk '{print $2}' | xargs kill -9 $1"

    if parallel_ssh_enabled:
        client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")
        client.run_command(kill_command)
        del client
    else:
        execute_command(kill_command, 0, get_pty = True, ips = ips, key_path = key_path)

def kill_proxies(ips, key_path = KEYFILE_PATH):
    """
    Kills all InfiniStore proxies running on the VMs (specified via their IPv4 addresses).

    Similar to 'kill_proxies' but more forceful, I guess? I think they essentially do the exact
    same thing so it doesn't really matter. I usually use 'kill_go_processes' to just kill
    everything, though (proxies, MapReduce clients and workers, etc.).
    """      
    kill_command = "sudo ps aux | grep proxy | awk '{print $2}' | xargs kill -9 $1"
    if parallel_ssh_enabled:
        client = ParallelSSHClient(ips, pkey = key_path, user = "ubuntu")
        client.run_command(kill_command)
        del client
    else:
        execute_command(kill_command, 0, get_pty = True, ips = ips, key_path = key_path)

# mrd.clear_redis_instances(flushall = True, hostnames = hostnames)
def clear_redis_instances(
    flushall = False,
    hostnames = None
):
    """
    Execute the "flushall" or "flushdb" commands on the Redis VMs.

    Arguments:
        flushall (bool): If True, executes 'flushall()'. Otherwise executes 'flushdb()'.

        hostnames (list of string): List of Redis hostnames in the form of "<IP>:<PORT>"
    """
    for hostname in hostnames:
        _split = hostname.split(":")
        rc = redis.Redis(host = _split[0], port = int(_split[1]), db = 0)
        if flushall:
            print("Flushing all on Redis @ {}".format(hostname))
            rc.flushall()
        else:
            print("Flushing DB on Redis @ {}".format(hostname))
            rc.flushdb()

# mrd.clean_workers(worker_ips = worker_ips)
def clean_workers(
    key_path = KEYFILE_PATH,
    worker_ips = None
):
    """
    Removes intermediate data generated by MapReduce from the workers. Isn't really necessary to run.

    Arguments:
        key_path: Path to SSH key.

        worker_ips: IPv4 addresses of worker VMs.
    """
    command = "cd %s/main/;sudo rm WorkerLog*; sudo rm *.dat; sudo rm IOData/*" % MAPREDUCE_DIRECTORY

    if parallel_ssh_enabled:
        client = ParallelSSHClient(worker_ips, pkey = key_path, user = "ubuntu")
        client.run_command(command)
        del client
    else:
        execute_command(
            command = command,
            count_limit = 2,
            ips = worker_ips,
            key_path = key_path,
            get_pty = True 
        )     

def launch_workers(
    client_ip = None,
    count_limit = 5,
    key_path = KEYFILE_PATH,
    workers_per_vm = 5,
    worker_ips = None
):
    """
    Commonly-used function. This launches the MapReduce workers.

    Arguments:
        client_ip (str): String of the form "<IP>:<PORT>". The IP is the IPv4 (usually private) of 
        the MapReduce client VM, and port is the port that the client is listening on (usually 1234).

        count_limit (int): Number of attempts to obtain output of executing SSH commands on the VM.

        key_path (string): Path to your SSH key (on your local computer/wherever you're running this script).

        workers_per_vm (int): Number of MapReduce workers to create on each VM.

        worker_ips (list of str): IPv4 addresses of the VMs on which you want to create the workers.
    """
    print("Key path = {}".format(key_path))

    # pre_command = """
    # . ~/.profile;
    # . ~/.bashrc;
    # """

    #post_command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;./start-workers.sh {}:1234 {}".format(client_ip, workers_per_vm)
    post_command = "cd {}/main/;export PATH=$PATH:/usr/local/go/bin;./start-workers.sh {}:1234 {} > /dev/null".format(MAPREDUCE_DIRECTORY, client_ip, workers_per_vm)

    # if parallel_ssh_enabled:
    #     print("Full command: {}".format(post_command))
    #     client = ParallelSSHClient(worker_ips, pkey = key_path, user = "ubuntu")
    #     client.run_command(post_command)
    #     del client
    # else:
    for ip in worker_ips:
        command = "launch_workers.sh \"%s\" \"%s\" \"%s\" \"%s\" \"%s\" \"%s\"" % (MAPREDUCE_DIRECTORY, client_ip, workers_per_vm, key_path, ip, "ubuntu")
        print("About to execute command:\n %s" % command)
        subprocess.run(command, shell=True)

    # execute_command(
    #     command = command,
    #     count_limit = count_limit,
    #     ips = worker_ips,
    #     key_path = key_path,
    #     get_pty = True 
    # )

def update_lambdas_prefixed(ips : list, prefix = "CacheNode", first_number = 0, key_path = KEYFILE_PATH):
    """
    This executes the ./update_function.sh InfiniStore script on each of the VMs specified by their
    IP address.

    This is used for prefixed deployments. For example, you may have created several InfiniStore
    AWS Lambda deployments. The first had prefix "CacheNode0-", the next had prefix "CacheNode1-", etc.
    The "base" prefix for each deployment is "CacheNode". 

    So the first IP in the ips parameter (which is a list of strings) will execute 'update_function' 
    for CacheNode0- prefix. The second IP will do the same for CacheNode1- prefix.

    So the 'prefix' argument is the base prefix. This assumes each prefix is of the form
    "<BASE_PREFIX><NUMBER>-". 

    This passes a random integer (currently between 60 and 120) for the timeout for the Lambda functions.
    """
    num = first_number
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = prefix + "{}-".format(num)
        command = "cd {}/deploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {} {}".format(INFINISTORE_DIRECTORY, random.randint(60, 120), lambda_prefix)
        print("Full command: {}".format(command))
        execute_command(
            command = command,
            count_limit = 1,
            ips = [ip],
            key_path = key_path,
            get_pty = True 
        )
        num += 1

def retrieve_remote_log(public_ip : str, private_ip : str, port : int, key_path = KEYFILE_PATH):
    """
    Retrieve the logfile for the worker identified by the ip/port combination.

    Arguments:
        ip (string): The IPv4 of the VM on which the worker is/was running.
        
        port (int): The port on which the worker is/was listening.

        key_path: Path to your SSH key.
    
    Writes WorkerLog-<private_ip>:<port>.out to /log/WorkerLog-<private_ip>:<port>.out.
    """
    keyfile = paramiko.RSAKey.from_private_key_file(key_path)

    print("Establishing SSH connection...")
    ssh_clients = list()
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(hostname = public_ip, username="ubuntu", pkey = keyfile) 

    print("Connection established. Creating SFTP client...")
    filename = "WorkerLog-{}:{}.out".format(private_ip, port)
    remote_path = "{}/main/{}".format(MAPREDUCE_DIRECTORY, filename)
    local_path = "logs/{}".format(filename)

    print("Remote path = %s\nLocal path = %s" % (remote_path, local_path))

    sftp_client = ssh_client.open_sftp()

    print("SFTP Client created. Opening file and reading...")
    #remote_file = sftp_client.open()
    sftp_client.get(remote_path, local_path)
    #local_file = open("logs/%s" % filename, "w+")
    # try:
    #     local_file.writelines(remote_file)
    #     #for line in remote_file:
    #     #    local_file.write(line)
    # finally:
    #     remote_file.close()
    #local_file.close()
    if sftp_client: sftp_client.close()
    if ssh_client: ssh_client.close()

    print("Successfully retrieved log \"%s\" from server @ %s (private ip = %s)" % (filename, public_ip, private_ip))

def update_lambdas(ips : list, key_path = KEYFILE_PATH):
    """
    This just executes the ./update_function.sh script on each VM specified by the ips list (list of str).

    So either you should only specify 1 IP and the update_function.sh script should be configured correctly
    on that VM (i.e., be configured to update Lambdas with the desired prefix), or each update_function.sh on the
    VMs specified by ips should be configured for different Lambdas (different prefix in each update_function.sh script).

    I usually just use update_lambdas_prefixed.
    """
    command = "cd {}/deploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {}".format(INFINISTORE_DIRECTORY, random.randint(600, 900))
    print("Full command: {}".format(command))
    execute_command(
        command = command,
        count_limit = 3,
        ips = ips,
        key_path = key_path,
        get_pty = True 
    )    

def format_proxy_config(proxy_ips : list) -> str:
    """
    When MapReduce framework used the Redis protocol for InfiniStore, we needed to update
    the file config.go, as in n proxy/config/config.go. This file needed a list of all the
    InfiniStore proxies. But we don't use the Redis protocol anymore so this function is not used.

    We would take the output of this function, replace the associated line in
    proxy/config/config.go (in the InfiniStore repo), add, commit, and push to GitHub, then
    use the 'pull_from_github_infinistore' function defined above to update all the VMs.
    """
    num_proxies = len(proxy_ips)
    #code_line = "var ProxyList [{}]string = [{}]string".format(num_proxies, num_proxies)
    code_line = "var ProxyList []string = []string{"
    
    for i in range(0, num_proxies):
        ip = proxy_ips[i]

        if i == (num_proxies - 1):
            code_line = code_line + "\"{}:6378\"".format(ip)
        else:
            code_line = code_line + "\"{}:6378\", ".format(ip)

    code_line = code_line + "}"
    return code_line

def format_parameter_storage_list(ips : list, port : int, parameter_name : str) -> str:
    """
    Users must pass to the MapReduce client a list of all the InfiniStore proxies. This parameter is formatted
    as "-storageIps <IP 1>:<PORT> -storageIps <IP 2>:<PORT> ...". This can be very tedious to write out
    by hand, so this function will basically generate the formatted parameter for you. Then you can
    copy-and-paste or otherwise append the string to the command for launching the MapReduce client.

    Arguments:
        ips (list of string): The IPv4s (usually private) of VMs that have an InfiniStore proxy running on them.

        port (int): The port that the InfiniStore proxies use (should just be 6378).

        parameter_name (str): The name of the parameter for the client. Currently it is 'storageIps'.
    
    For example, let's say you have InfiniStore proxies running on two VMs. The first VM has private IPv4
    10.0.116.159, and the second VM has private IPv4 10.0.255.255. The ips argument would be:
    ["10.0.116.159", "10.0.255.255"]. port would be 6378. parameter_name would be 'storageIps'. This 
    function would return the following:

    "-storageIps 10.0.116.159:6378 -storageIps 10.0.255.255:6378"

    You could then copy and paste this to the end of your command to start MapReduce client

    go run client.go -hostname 10.0.116.159:1234 ... ... ... -storageIps 10.0.116.159:6378 -storageIps 10.0.255.255:6378
    """
    param = ""
    for ip in ips:
        param = param + "-{} {}:{} ".format(parameter_name, ip, port)
    return param

def print_time():
    """
    Print the current time formatted in the way that the export_ubuntu.sh script, which is used for 
    exporting AWS Lambda CloudWatch logs to AWS S3, expects the start and end parameters to be.
    """
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(date_time)
    return date_time

def launch_proxies_and_record_metadata(worker_ips : list, client_ip : str) -> (str, str):
    """
    This just calls launch_infinistore_proxies and returns a start time and prefix.

    Good for when you want to run an experiment.
    """
    start_time = print_time()
    experiment_prefix = launch_infinistore_proxies(worker_ips + [client_ip])
    print("experiment_prefix = " + str(experiment_prefix)) 
    return start_time, experiment_prefix   

def download_io_data(prefix, ips = [], key_path = KEYFILE_PATH):
    for ip in ips:
        command = "download_io_data.sh \"%s\" \"%s\" \"%s\"" % (prefix, ip, key_path)
        print("About to execute command:\n %s" % command)
        subprocess.run(command, shell=True)

"""
==== GENERAL RUNNING INSTRUCTIONS ===
I usually run these script from my personal laptop or desktop. I always run the scripts
from a Python3 interactive terminal. I start by opening the interactive session (e.g., type
python3 in terminal/command-line). I then copy-and-paste all of the import statements at the top of
this file into the interactive session.

Next, you need to collect all of the IP addresses of your VMs so you can execute SSH commands on them.
This is done automatically for you, provided you have the proper Python libraries installed (boto3, 
namely).

In the code below (after the "if __name__ == '__main__':" part), I have labeled the various 
blocks of code that you'll run as BLOCK 1, BLOCk 2, ..., BLOCK N. I will use these labels
when discussing them.

Block 1 will gather all of the IPs of your VMs. Of the VMs you use for MapReduce, one will serve
as the so-called Client/Scheduler while the others are workers. Identify the VM you will use as
your client, as these scripts treat that VM differently, and the VM is identified via its IP address.

When gathering the IPs by executing the code in BLOCK 1, you will see a list of IPs. Basically,
the client_ip variables need to take on the value of the client VM while the rest are workers.
I usually pick the first or last VM in the list to be my client, as this makes it easy to
assign client_ip = ip[0] (make client the first vm).

So execute one of the code blocks in BLOCK 2, whichever makes sense. You may need to modify them
if the client IP is in the middle of the list. You also may need to manually remove the IPs of VMs
used by Pocket (since you presumably don't want to run workers on those VMs).

Block 4 prints out all of the data you just collected so you can view it. The "param" variable won't
be used by Pocket, so you can ignore that.

Once you've gotten your list of worker IPs, I would SSH onto the Client, go into the MapReduce project
root directory, then go into the main folder. It is here that you will run the client. There are a
bunch of pre-configured commands for running the client below so you can copy-and-paste them. They
alll have a bunch of -storageParameter command-line parameters. You can either include these or 
remove them. They're for InfiniStore.

After starting the client, I'd just execute the line of code in Block 7. That'll start the workers,
and thus start the job. That should be it. Watch the client output for errors or to see when it
finishes. Timing information for the job phases (map, reduce, merge) will be printed out.

You can download the IO metadata by executing the command:

mrd.download_io_data(experiment_prefix, ips = worker_ips + [client_ip])

This will download all of the metadata to a folder MapReduceProjectRoot/util/IOData/experiment_prefix/.
"""

# =========================================================
# Quick reference, copy-and-paste these commands as needed.
# =========================================================
#
# They are commented out bc otherwise they would be executed when we import 
# mapreduce_driver.py into our Python terminal session.

# ====================
# Post-Experiment:
# ====================
# mrd.download_io_data(experiment_prefix, ips = worker_ips + [client_ip])

# =======================
# Clean-up (after jobs):
# =======================
# mrd.clear_redis_instances(flushall = True, hostnames = hostnames)
# mrd.clean_workers(worker_ips = worker_ips)
# mrd.kill_proxies(ips = worker_ips + [client_ip])
# mrd.kill_go_processes(ips = worker_ips + [client_ip])

# ====================
# Pulling from Github:
# ====================
# mrd.pull_from_github_infinistore(worker_ips)
# mrd.pull_from_github_infinistore(worker_ips + [client_ip], reset_first = True)
# mrd.pull_from_github_mapreduce(worker_ips + [client_ip], reset_first = True)

# ====================
# Updating the Lambdas
# ====================
# mrd.build_mapreduce(worker_ips + [client_ip])
# mrd.build_infinistore(worker_ips + [client_ip])
# mrd.update_lambdas(worker_ips + [client_ip])
# mrd.update_lambdas_prefixed(worker_ips + [client_ip])
# mrd.update_lambdas(worker_ips) # Workers only, so we can do manually on client to see progress.
# mrd.update_lambdas_prefixed(worker_ips) # Workers only, so we can do manually on client to see progress.

# ====================
# Launching Workloads
# ====================
# experiment_prefix = mrd.launch_infinistore_proxies(worker_ips + [client_ip])
# experiment_prefix = mrd.launch_infinistore_proxies([client_ip])
# print("experiment_prefix = " + str(experiment_prefix))
if __name__ == "__main__":
    # ================
    # ====BLOCK #1====
    # ================
    # I usually copy-and-paste this entire block of text into a terminal. Make sure you
    # un-indent them before copying-and-pasting. They are indented now bc otherwise 
    # they cause errors when importing mapreduce_driver.py into your Python terminal session.
    # In most code editors, you can highlight the whole block and press SHIFT+TAB to unindent all at once.
    public_ips, private_ips = mrd.get_ips() # Get the public & private IPv4 addresses of all running VMs.
    all_ips = public_ips + private_ips     # Creates a list of all the IPv4 addresses.
    workers_per_vm = 7                     # Number of MapReduce workers per VM.
    NUM_CORES_PER_WORKER = 2               # Number of cores that each worker gets.

    # Generally my InfiniStore client VM is the first or last VM in the list public_ips. 
    # So depending on which it is, I then copy and paste this block or the next block to 
    # populate the values accordingly.

    # ================
    # ====BLOCK #2====
    # ================
    # If the Client IP appears first in the list. 
    client_ip = public_ips[0]   # Create variable to hold the MapReduce client's IP public address.
    client_ip_private = private_ips[0] # Create variable to hold the MapReduce client's IP private address. 
    worker_ips = public_ips[1:] # The remaining IPs are public worker IPs.
    worker_private_ips = private_ips[1:] # The remaining IPs are private worker IPs.

    # If the Client IP appears last in the list. Only copy-and-paste this block if you didn't
    # use the previous block.
    client_ip = public_ips[-1] # Create variable to hold the MapReduce client's IP public address.
    client_ip_private = private_ips[-1] # Create variable to hold the MapReduce client's IP private address.
    worker_ips = public_ips[:-1] # The remaining IPs are public worker IPs. 
    worker_private_ips = private_ips[:-1] # The remaining IPs are private worker IPs.

    # This function isn't used anymore; this was only for when we used the Redis protocol with
    # InfiniStore. See the documentation for 'format_proxy_config'.
    # code_line = mrd.format_proxy_config([client_ip_private] + worker_private_ips)

    # ================
    # ====BLOCK #4====
    # ================
    # We DO use this. Usually I'll execute this, print out the value of 'param', then update the
    # command I use to launch the MapReduce worker.
    param = mrd.format_parameter_storage_list([client_ip_private] + worker_private_ips, 6378, "storageIps")
    # Print the IPs.
    print("Client IP: {}".format(client_ip))
    print("Client private IP: {}".format(client_ip_private))
    print("Worker IP's ({}): {}".format(len(worker_ips), worker_ips))
    print("Worker private IP's ({}): {}".format(len(worker_private_ips), worker_private_ips))
    print(param)

    # Only do this if we're purposefully modifying the upload/download speeds of our VMs.
    # wondershape(ips = [client_ip] + worker_ips)

    # ================
    # ====BLOCK #5====
    # ================    
    # Number of Reducers = NUM_WORKERS_PER_VM * NUM_VMs * NUM_CORES_PER_WORKER
    # This value gets passed to the MapReduce client. You need to update the command yourself
    # with whatever this calculates.
    nReducers = workers_per_vm * len(worker_ips) * NUM_CORES_PER_WORKER
    print("nReducers = {}".format(nReducers))

    # ================
    # ====BLOCK #6====
    # ================
    # ==================================================================================================
    # We use this block to start all the InfiniStore proxies.
    # 
    # First, we get the start time of the experiment. We'll need to pass this value to export_ubuntu.sh
    # if we want to export the AWS Lambda CloudWatch logs after running the workload.
    #
    # Next, we launch the InfiniStore proxies. We launch one on each worker VM and one on the client VM.
    # We store the returned experiment_prefix in a variable (and print it) as we also need that to pass
    # to export_ubuntu.sh. Basically, we use the prefix when creating a folder in our AWS S3 bucket for
    # the logs corresponding to this specific experiment/job.
    # start_time = mrd.print_time()
    # experiment_prefix = mrd.launch_infinistore_proxies(worker_ips + [client_ip])
    # print("experiment_prefix = " + str(experiment_prefix))
    # This function does the same thing as the previous three lines. Just use this.
    start_time, experiment_prefix = mrd.launch_proxies_and_record_metadata(worker_ips, client_ip)

    # ===============================================================================================
    # NOTE: I generally copy-and-paste these into a terminal session, so I leave the FULL paths here
    # without using the MAPREDUCE_DIRECTORY and GOPATH variables defined in mapreduce_driver.py. I 
    # recommend you replace these with your own hard-coded paths if you intend to copy-and-paste them.

    # The input data for TeraSort/grep is stored in S3. The MapReduce framework needs the keys of the
    # input data. Usually there are 20+ shards, so passing that as a command-line argument would be
    # obnoxious. Instead, I created a bunch of text files, each containing the S3 keys of the input
    # data partitions for various problem sizes. 

    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/20GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/20GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt    

    # This function does NOT work anymore. I am leaving it here in case I ever decide to update it, though. That way,
    # I don't have to rewrite this function call.
    # mrd.launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt")

    # ================
    # ====BLOCK #7====
    # ================    
    # This function IS used. This is a pre-formatted function call to start the workers based on all the code we executed above.
    mrd.launch_workers(client_ip = client_ip_private, worker_ips = worker_ips, workers_per_vm = workers_per_vm, count_limit = 1)
    # mrd.launch_workers(client_ip = client_ip_private, worker_ips = [client_ip], workers_per_vm = workers_per_vm, count_limit = 1)

    # Make sure to print this at the end so you know when the job stopped (in the correct format).
    # This gets passed to export_ubuntu.sh if you export the AWS Lambda CloudWatch logs for this job.
    end_time = mrd.print_time()

# ======================================================================================================
# This is a pre-formatted command to run the MapReduce client. I created a long-running EC2 VM (like one
# that I stop and start but never terminate) so its private IPv4 is basically static. You use the VM's
# private IPv4 for the 'driverHostname' parameter, along with port 1234.

# Instance1 and Instance2, this is for Ben's debugging.
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps 10.0.116.159:6378 -storageIps 10.0.81.136:6378 -storageIps 10.0.74.216:6378 -storageIps 10.0.70.136:6378 -storageIps 10.0.66.154:6378 -storageIps 10.0.72.93:6378 -storageIps 10.0.91.143:6378

# Temp
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 15 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps 10.0.116.159:6378 -storageIps 10.0.84.102:6378 -chunkThreshold 512000000

# Six-node SORT Commands ** REMEMBER TO CHANGE -nReduce PARAMETER **
# 1 MB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378
# 100 MB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378
# 10 GB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/10GB_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378
# 20 GB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/10GB_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378
# 50 GB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/50GB_50Partitions_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378
# 100 GB
# go run client.go -driverHostname 10.0.116.159:1234 -jobName srt -nReduce 90 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100GB_50Partitions_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378

# go run client.go -driverHostname 10.0.116.159:1234 -jobName wc -nReduce 84 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/50GB_50Partitions_S3Keys.txt -storageIps 10.0.116.159:6378 -storageIps 10.0.78.140:6378 -storageIps 10.0.82.103:6378 -storageIps 10.0.73.57:6378 -storageIps 10.0.95.241:6378 -storageIps 10.0.77.246:6378 -storageIps 10.0.77.240:6378

# Change the 'jobName' parameter depending on what job you want to run. For TeraSort, it is 'srt'.
# For grep, it is 'grep'. For Word Count, it is 'wc'. Basically, it is the prefix of the two service
# files which implement the job. For example, TeraSort is implemented in srtm_service.go and srtr_service.go.
# These files are named as <JOB_NAME>m_service.go for the Map service/function, and similarly
# <JOB_NAME>r_service.go for the Reduce service/function.

# Make sure to update the nReduce argument based on what you calculated above.

# You should change the s3KeyFile parameter based on the problem size you wish to use. 

# Finally, make sure to append the updated -storageIps parameters to the end of the command. Replace the
# existing 'storageIps' parameter.

# ==========================================================================================
# These are two hard-coded functions to start InfiniStore proxies. The first uses CacheNode0
# while the second uses CacheNode1. This is just for debugging, as the prefix values are old/outdated.

#go run $PWD/../proxy/proxy.go -debug=true -prefix=202011291702/ -lambda-prefix=CacheNode0- -disable-color >./log 2>&1
#go run $PWD/../proxy/proxy.go -debug=true -prefix=202011291702/ -lambda-prefix=CacheNode1- -disable-color >./log 2>&1