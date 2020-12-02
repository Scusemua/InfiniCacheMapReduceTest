import boto3   
import time 
import paramiko
import random
from datetime import datetime
import redis 
import launch_cluster as lc

"""
This file contains scripts that I use to orchestrate workloads on remote clusters/virtual machines.

You could write scripts to automatically deploy and run everything by running a single function, but
I typically write out the function calls with the parameters I intend to use, and then just copy-and-
paste them into a terminal.

At the very least, you need to modify the variable declarations at the top of the file to match
your setup. 

INFINISTORE_DIRECTORY is the path to the root of the InfiniStore GitHub repo. Note that this should
be under your $GOPATH environment variable. (My GOPATH is /home/ubuntu/project, I believe). Use Google
to learn about GOPATH.

KEYFILE_PATH is the path to your SSH key so Paramiko can SSH onto your VM's to execute commands.
"""

# This is the path to the root of the InfiniStore local GitHub repository on the VM.
INFINISTORE_DIRECTORY = "/home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/"

# The path to your keyfile.
KEYFILE_PATH = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"

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
        ssh_redis = paramiko.SSHClient()
        ssh_clients.append((ip, ssh_redis))
        ssh_redis.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_redis.connect(hostname = ip, username="ubuntu", pkey = keyfile) 
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
    #s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt"
    s3_key_file = "/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt"
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
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

    #post_command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;./start-client.sh {} {}".format(nReducers, s3_key_file)
    post_command = "cd /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main;./start-client.sh {} {}".format(nReducers, s3_key_file)

    command = pre_command + post_command

    execute_command(
        command = command,
        ips = [client_ip],
        key_path = key_path
    )    

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
    create_redis_file_command = create_redis_file_command + " > /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main/redis_hosts.txt"
    
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

def pull_from_github(ips, reset_first = False, key_path = KEYFILE_PATH):
    if reset_first:
        print("Resetting first...")
        command_reset = "cd %sevaluation; git reset --hard origin/config_ben" % INFINISTORE_DIRECTORY
        execute_command(command_reset, 2, get_pty = True, ips = ips, key_path = key_path)
    
    print("Now pulling...")
    command = "cd %sevaluation; git pull" % INFINISTORE_DIRECTORY
    execute_command(command, 2, get_pty = True, ips = ips, key_path = key_path)

def launch_infinistore_proxies(ips, key_path = KEYFILE_PATH):
    # command = "cd %sevaluation; export PATH=$PATH:/usr/local/go/bin; export GOPATH=/home/ubuntu/project; make start-server" % INFINISTORE_DIRECTORY
    # command = "cd %sevaluation; export GOPATH=/home/ubuntu/project; make start-server" % INFINISTORE_DIRECTORY
    #command = "cd %sevaluation; export PATH=$PATH:/usr/local/go/bin; export GOPATH=/home/ubuntu/project; ./server.sh >./log 2>&1 &" % INFINISTORE_DIRECTORY
    prefix = datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M') + "/"
    
    # This starts the proxies successfully, but I get failures almost immediately during workload. May be entirely unrelated.

    # Each proxy needs a slightly different command as each proxy uses different Lambdas.
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = "CacheNode%d-" % i 
        print("Assigning lambda prefix \"%s\" to proxy at ip %s." % (lambda_prefix, ip))
        command = "cd {}evaluation; export PATH=$PATH:/usr/local/go/bin; go run $PWD/../proxy/proxy.go -debug=true -prefix={} -lambda-prefix={} -disable-color >./log 2>&1".format(INFINISTORE_DIRECTORY, prefix, lambda_prefix)
        execute_command(command, 3, get_pty = True, ips = [ip], key_path = key_path)

    # This does NOT work.
    #command = "cd %sevaluation; export PATH=$PATH:/usr/local/go/bin; make start-server; cat log; cat log" % INFINISTORE_DIRECTORY

    return prefix

def export_cloudwatch_logs(ips, prefix, start, end, key_path = KEYFILE_PATH):
    command = "cd %sevaluation/cloudwatch; ./export_ubuntu.sh %s %s %s;" % (INFINISTORE_DIRECTORY, prefix, str(start), str(end))
    execute_command(command, 3, get_pty = True, ips = ips, key_path = key_path)

def stop_infinistore_proxies(ips, key_path = KEYFILE_PATH):
    command = "cd %sevaluation; make stop-server" % INFINISTORE_DIRECTORY
    execute_command(command, 1, get_pty = True, ips = ips, key_path = key_path)

# lc.kill_go_processes(ips = worker_ips)
# lc.kill_go_processes(ips = worker_ips + [client_ip])
# lc.kill_go_processes(ips = [client_ip])
def kill_go_processes(
    ips = None,
    key_path = KEYFILE_PATH
):
    kill_command = "sudo ps aux | grep go | awk '{print $2}' | xargs kill -9 $1"
    execute_command(kill_command, 0, get_pty = True, ips = ips, key_path = key_path)

def kill_proxies(ips, key_path = KEYFILE_PATH):
    kill_command = "sudo ps aux | grep proxy | awk '{print $2}' | xargs kill -9 $1"
    execute_command(kill_command, 0, get_pty = True, ips = ips, key_path = key_path)

# lc.clear_redis_instances(flushall = True, hostnames = hostnames)
def clear_redis_instances(
    flushall = False,
    hostnames = None
):
    for hostname in hostnames:
        _split = hostname.split(":")
        rc = redis.Redis(host = _split[0], port = int(_split[1]), db = 0)
        if flushall:
            print("Flushing all on Redis @ {}".format(hostname))
            rc.flushall()
        else:
            print("Flushing DB on Redis @ {}".format(hostname))
            rc.flushdb()

# lc.clean_workers(worker_ips = worker_ips)
def clean_workers(
    key_path = KEYFILE_PATH,
    worker_ips = None
):
    #command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;sudo rm WorkerLog*; sudo rm *.dat"
    command = "cd /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main/;sudo rm WorkerLog*; sudo rm *.dat"
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
    print("Key path = {}".format(key_path))

    pre_command = """
    . ~/.profile;
    . ~/.bashrc;
    """

    #post_command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;./start-workers.sh {}:1234 {}".format(client_ip, workers_per_vm)
    post_command = "cd /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/main/;./start-workers.sh {}:1234 {}".format(client_ip, workers_per_vm)

    command = pre_command + post_command

    print("Full command: {}".format(command))

    execute_command(
        command = command,
        count_limit = count_limit,
        ips = worker_ips,
        key_path = key_path,
        get_pty = True 
    )

def update_lambdas_prefixed(ips, prefix = "CacheNode", key_path = KEYFILE_PATH):
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = prefix + "{}-".format(i)
        command = "cd %sdeploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {} {}".format(INFINISTORE_DIRECTORY, random.randint(60, 100), lambda_prefix)
        print("Full command: {}".format(command))
        execute_command(
            command = command,
            count_limit = 3,
            ips = [ip],
            key_path = key_path,
            get_pty = True 
        )    

def update_lambdas(ips, key_path = KEYFILE_PATH):
    command = "cd %sdeploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {}".format(INFINISTORE_DIRECTORY, random.randint(600, 900))
    print("Full command: {}".format(command))
    execute_command(
        command = command,
        count_limit = 3,
        ips = ips,
        key_path = key_path,
        get_pty = True 
    )    

def format_proxy_config(proxy_ips : list) -> str:
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
    param = ""
    for ip in ips:
        param = param + "-{} {}:{} ".format(parameter_name, ip, port)
    return param

def print_time():
    now = datetime.now()
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(date_time)
    return date_time

# 52.55.211.171, 3.84.164.176
# lc.clear_redis_instances(flushall = True, hostnames = hostnames)
# lc.clean_workers(worker_ips = worker_ips)
# lc.kill_go_processes(ips = worker_ips + [client_ip])
# lc.pull_from_github(worker_ips)
# lc.pull_from_github(worker_ips + [client_ip])
# experiment_prefix = lc.launch_infinistore_proxies(worker_ips + [client_ip])
# experiment_prefix = lc.launch_infinistore_proxies([client_ip])
# print("experiment_prefix = " + str(experiment_prefix))
# lc.update_lambdas(worker_ips + [client_ip])
# lc.update_lambdas_prefixed(worker_ips + [client_ip])
if __name__ == "__main__":
    get_private_ips = lc.get_private_ips
    get_public_ips = lc.get_public_ips
    get_ips = lc.get_ips
    public_ips, private_ips = get_ips()
    all_ips = public_ips + private_ips
    workers_per_vm = 3
    NUM_CORES_PER_WORKER = 5
    shards_per_vm = 1
    num_redis = 0

    # If the Client IP appears first in the list.
    client_ip = public_ips[0]
    client_ip_private = private_ips[0]
    worker_ips = public_ips[1:]
    worker_private_ips = private_ips[1:]

    # If the Client IP appears last in the list.
    client_ip = public_ips[-1]
    client_ip_private = private_ips[-1]
    worker_ips = public_ips[:-1]
    worker_private_ips = private_ips[:-1]

    subset_workers = worker_ips[0:2]
    code_line2 = lc.format_proxy_config([client_ip_private] + subset_workers)

    code_line = lc.format_proxy_config([client_ip_private] + worker_private_ips)

    param = lc.format_parameter_storage_list([client_ip_private] + worker_private_ips, 6378, "storageIps")

    print("Client IP: {}".format(client_ip))
    print("Client private IP: {}".format(client_ip_private))
    print("Worker IP's ({}): {}".format(len(worker_ips), worker_ips))
    print("Worker private IP's ({}): {}".format(len(worker_private_ips), worker_private_ips))
    print(param)

    wondershape(ips = [client_ip] + worker_ips)

    # NUM_WORKERS_PER_VM * NUM_VMs * NUM_CORES_PER_WORKER
    nReducers = workers_per_vm * len(worker_ips) * NUM_CORES_PER_WORKER
    print("nReducers = {}".format(nReducers))

    lc.pull_from_github([client_ip] + worker_ips, reset_first = True)
    start_time = lc.print_time()
    experiment_prefix = lc.launch_infinistore_proxies(worker_ips + [client_ip])
    experiment_prefix += "/" #TODO: Remove this next time, as the launch_infinistore_proxies will be updated.
    print("experiment_prefix = " + str(experiment_prefix))

    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/20GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/20GB_S3Keys.txt
    # /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt    
    #launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt")
    # ./start-client srt 36 sample_data.dat /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt 10 2 32 
    # go run client.go -driverHostname 10.0.109.88:1234 -jobName srt -nReduce 36 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps 10.0.109.88:6378 -storageIps 10.0.121.202:6378
    lc.launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt")

    lc.launch_workers(client_ip = client_ip, worker_ips = worker_ips, workers_per_vm = workers_per_vm, count_limit = 1)

    #lc.launch_workers(client_ip = client_ip, worker_ips = worker_ips[0:2], workers_per_vm = workers_per_vm, count_limit = 1)
    end_time = lc.print_time()

# make stop-server

# cd ../evaluation
# make start-server 
# tail -f log

# vim ../proxy/config/config.go

# vim ../deploy/update_function.sh

# cd ../deploy
# ./update_function.sh 607

#   
#go run $PWD/../proxy/proxy.go -debug=true -prefix=202011291702 -lambda-prefix=CacheNode0- -disable-color >./log 2>&1
#go run $PWD/../proxy/proxy.go -debug=true -prefix=202011291702 -lambda-prefix=CacheNode1- -disable-color >./log 2>&1