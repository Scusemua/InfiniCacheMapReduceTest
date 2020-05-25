import boto3   
import time 
import paramiko
import redis 

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
    get_pty = False,
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
    s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt"
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
):
    pre_command = """
    . ~/.profile;
    . ~/.bashrc;
    """

    post_command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;./start-client.sh {} {}".format(nReducers, s3_key_file)

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
    
    create_redis_file_command = create_redis_file_command + " > /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/redis_hosts.txt"
    
    print("create_redis_file_command = \"{}\"".format(create_redis_file_command))

    execute_command(
        command = create_redis_file_command,
        ips = ips,
        key_path = key_path
    )

# lc.kill_go_processes(ips = worker_ips)
# lc.kill_go_processes(ips = worker_ips + [client_ip])
# lc.kill_go_processes(ips = [client_ip])
def kill_go_processes(
    ips = None,
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"
):
    kill_command = "sudo ps aux | grep go | awk '{print $2}' | xargs kill -9 $1"
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
    command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;sudo rm WorkerLog*; sudo rm *.dat"
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
    redis_ips = None,
    workers_per_vm = 5,
    worker_ips = None
):
    print("Key path = {}".format(key_path))
    #update_redis_hosts(ips = [client_ip] + worker_ips, redis_ips = redis_ips)

    pre_command = """
    . ~/.profile;
    . ~/.bashrc;
    """

    post_command = "cd /home/ubuntu/project/src/InfiniCacheMapReduceTest/main/;pwd;./start-workers.sh {}:1234 {}".format(client_ip, workers_per_vm)

    command = pre_command + post_command

    print("Full command: {}".format(command))

    execute_command(
        command = command,
        count_limit = count_limit,
        ips = worker_ips,
        key_path = key_path,
        get_pty = True 
    )    

# lc.clear_redis_instances(flushall = True, hostnames = hostnames)
# lc.clean_workers(worker_ips = worker_ips)
# lc.kill_go_processes(ips = worker_ips + [client_ip])
if __name__ == "__main__":
    ips = get_public_ips()
    workers_per_vm = 5
    shards_per_vm = 5
    num_redis = 5

    redis_ips = ips[0:num_redis]
    client_ip = ips[num_redis]
    worker_ips = ips[num_redis + 1:] #worker_ips = ips[num_redis:]

    print("Redis IP's ({}): {}".format(len(redis_ips), redis_ips))
    print("Client IP: {}".format(client_ip))
    print("Worker IP's ({}): {}".format(len(worker_ips), worker_ips))

    # hostnames = lc.launch_redis_servers(ips = redis_ips, count_limit = 1, kill_first = False, connect_and_ping = True, shards_per_vm = shards_per_vm)
    # hostnames = lc.launch_redis_servers(ips = redis_ips, count_limit = 1, kill_first = True, connect_and_ping = True, shards_per_vm = shards_per_vm)
    hostnames = launch_redis_servers(ips = redis_ips, kill_first = True, connect_and_ping = True, shards_per_vm = shards_per_vm)

    ping_redis(hostnames = hostnames)

    update_redis_hosts(ips = [client_ip], redis_ips = redis_ips, hostnames = hostnames)

    wondershape(ips = [client_ip] + worker_ips + redis_ips)

    nReducers = workers_per_vm * len(worker_ips) * 9
    print("nReducers = {}".format(nReducers))

    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/1MB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/20GB_S3Keys.txt
    # /home/ubuntu/project/src/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt
    launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/100GB_S3Keys.txt")

    launch_workers(client_ip = client_ip, redis_ips = redis_ips, worker_ips = worker_ips, workers_per_vm = workers_per_vm, count_limit = 1)
