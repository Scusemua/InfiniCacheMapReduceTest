import boto3   
import time 
import paramiko
import random
from datetime import datetime
import redis 
import launch_cluster as lc

def get_private_ips(region_name="us-east-1"):
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"
):
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
    kill_first = False ,
    shards_per_vm = 1,
    starting_port = 6379
):
    """
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
    nReducers = 10,
    #s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt"
    s3_key_file = "/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt"
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
):
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
    command = "sudo wondershaper -a eth0 -u {} -d {}".format(upload_bytes, download_bytes)
    execute_command(command = command, count_limit = 1, get_pty = True, ips = ips)

def ping_redis(
    ips = None,
    hostnames = None
):
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"
):
    """
    Update the redis_hosts.txt file on the given VMs.
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

def git_status(ips, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    print("Checking status now...")
    command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/; git status"
    execute_command(command, 3, get_pty = True, ips = ips, key_path = key_path)

def pull_from_github(ips, reset_first = False, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    if reset_first:
        print("Resetting first...")
        command_reset = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; git reset --hard origin/config_ben"
        execute_command(command_reset, 2, get_pty = True, ips = ips, key_path = key_path)
    
    print("Now pulling...")
    command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; git pull"
    execute_command(command, 2, get_pty = True, ips = ips, key_path = key_path)

def launch_infinistore_proxies(ips, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    # command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; export PATH=$PATH:/usr/local/go/bin; export GOPATH=/home/ubuntu/project; make start-server"
    # command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; export GOPATH=/home/ubuntu/project; make start-server"
    #command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; export PATH=$PATH:/usr/local/go/bin; export GOPATH=/home/ubuntu/project; ./server.sh >./log 2>&1 &"
    prefix = datetime.fromtimestamp(time.time()).strftime('%Y%m%d%H%M')
    
    # This starts the proxies successfully, but I get failures almost immediately during workload. May be entirely unrelated.

    # Each proxy needs a slightly different command as each proxy uses different Lambdas.
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = "CacheNode%d-" % i 
        print("Assigning lambda prefix \"%s\" to proxy at ip %s." % (lambda_prefix, ip))
        command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; export PATH=$PATH:/usr/local/go/bin; go run $PWD/../proxy/proxy.go -debug=true -prefix={} -lambda-prefix={} -disable-color >./log 2>&1".format(prefix, lambda_prefix)
        execute_command(command, 3, get_pty = True, ips = [ip], key_path = key_path)

    # This does NOT work.
    #command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; export PATH=$PATH:/usr/local/go/bin; make start-server; cat log; cat log"

    return prefix

def export_cloudwatch_logs(ips, prefix, start, end, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation/cloudwatch; ./export_ubuntu.sh %s %s %s;" % (prefix, str(start), str(end))
    execute_command(command, 3, get_pty = True, ips = ips, key_path = key_path)

def stop_infinistore_proxies(ips, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/evaluation; make stop-server"
    execute_command(command, 1, get_pty = True, ips = ips, key_path = key_path)

# lc.kill_go_processes(ips = worker_ips)
# lc.kill_go_processes(ips = worker_ips + [client_ip])
# lc.kill_go_processes(ips = [client_ip])
def kill_go_processes(
    ips = None,
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"
):
    kill_command = "sudo ps aux | grep go | awk '{print $2}' | xargs kill -9 $1"
    execute_command(kill_command, 0, get_pty = True, ips = ips, key_path = key_path)

def kill_proxies(ips, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
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
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
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

def update_lambdas_prefixed(ips, prefix = "CacheNode", key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    for i in range(0, len(ips)):
        ip = ips[i]
        lambda_prefix = prefix + "{}-".format(i)
        command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/deploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {} {}".format(random.randint(60, 80), lambda_prefix)
        print("Full command: {}".format(command))
        execute_command(
            command = command,
            count_limit = 3,
            ips = [ip],
            key_path = key_path,
            get_pty = True 
        )    

def update_lambdas(ips, key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"):
    command = "cd /home/ubuntu/project/src/github.com/mason-leap-lab/infinicache/deploy; export PATH=$PATH:/usr/local/go/bin; ./update_function.sh {}".format(random.randint(600, 900))
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
    NUM_CORES_PER_WORKER = 4
    shards_per_vm = 1
    num_redis = 0

    client_ip = public_ips[0]
    client_ip_private = private_ips[0]
    worker_ips = public_ips[1:]
    worker_private_ips = private_ips[1:]

    subset_workers = worker_ips[0:2]
    code_line2 = lc.format_proxy_config([client_ip_private] + subset_workers)

    code_line = lc.format_proxy_config([client_ip_private] + worker_private_ips)

    param = format_parameter_storage_list([client_ip_private] + worker_private_ips, 6378, "storageIps")

    print("Client IP: {}".format(client_ip))
    print("Client private IP: {}".format(client_ip_private))
    print("Worker IP's ({}): {}".format(len(worker_ips), worker_ips))
    print("Worker private IP's ({}): {}".format(len(worker_private_ips), worker_private_ips))
    print(code_line)

    wondershape(ips = [client_ip] + worker_ips)

    # NUM_WORKERS_PER_VM * NUM_VMs * NUM_CORES_PER_WORKER
    nReducers = workers_per_vm * len(worker_ips) * NUM_CORES_PER_WORKER
    print("nReducers = {}".format(nReducers))

    lc.pull_from_github([client_ip] + worker_ips, reset_first = True)
    start_time = lc.print_time()
    experiment_prefix = lc.launch_infinistore_proxies(worker_ips + [client_ip])
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
    # go run client.go -driverHostname 10.0.109.88:1234 -jobName srt -nReduce 36 -sampleDataKey sample_data.dat -s3KeyFile /home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/100MB_S3Keys.txt -dataShards 10 -parityShards 2 -maxGoRoutines 32 -storageIps 10.0.109.88:6378 -storageIps 10.0.64.237:6378 -storageIps 10.0.69.5:6378 -storageIps 10.0.82.164:6378
    # -storageIps 10.0.109.88 -storageIps 10.0.82.164 
    lc.launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/github.com/Scusemua/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt")

    lc.launch_workers(client_ip = client_ip, worker_ips = worker_ips, workers_per_vm = workers_per_vm, count_limit = 1)

    #lc.launch_workers(client_ip = client_ip, worker_ips = worker_ips[0:2], workers_per_vm = workers_per_vm, count_limit = 1)
    end_time = print_time()

# make stop-server

# cd ../evaluation
# make start-server 
# tail -f log

# vim ../proxy/config/config.go

# vim ../deploy/update_function.sh

# cd ../deploy
# ./update_function.sh 607