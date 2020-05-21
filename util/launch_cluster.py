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
    
    count = 1
    for IP, ssh_redis_client in ssh_clients:
        print("Executing command for instance @ {}... ({}/{})".format(IP, count, len(ips)))
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
        count += 1

def launch_redis_servers(
    connect_and_ping = True,
    ips = None,
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
    kill_first = False 
):
    redis_command = "sudo redis-server redis.conf"
    
    if kill_first:
        print("Killing existing servers first...")
        kill_command = "sudo pkill -9 redis-server"
        execute_command(
            command = kill_command,
            ips = ips,
            key_path = key_path
        )

    execute_command(
        command = redis_command,
        ips = ips,
        key_path = key_path
    )

    # Connect to the servers and ping them to verify that they were actually setup properly.
    if connect_and_ping:
        for ip in ips:
            redis_client = redis.Redis(host = ip, port = 6379, db = 0)
            print("Pinging Redis instance @ {}:6379 now...".format(ip))
            res = redis_client.ping()
            if res is False:
                raise Exception("ERROR: Redis instance @ {} did not start correctly!".format(ip))
            else:
                print("True")        

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
    command = "sudo wondershaper eth0 {} {}".format(upload_bytes, download_bytes)
    execute_command(command = command, count_limit = 1, get_pty = True, ips = ips)

def update_redis_hosts(
    ips = None,
    redis_ips = None,
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem"
):
    """
    Update the redis_hosts.txt file on the given VMs.
    """
    print("Key path = {}".format(key_path))
    create_redis_file_command = "printf \""

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

def launch_workers(
    client_ip = None,
    count_limit = 5,
    key_path = "G:\\Documents\\School\\College\\Junior Year\\CS 484_\\HW1\\CS484_Desktop.pem",
    redis_ips = None,
    workers_per_vm = 5,
    worker_ips = None
):
    print("Key path = {}".format(key_path))
    update_redis_hosts(ips = [client_ip] + worker_ips, redis_ips = redis_ips)

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

if __name__ == "__main__":
    ips = get_public_ips()
    workers_per_vm = 3
    redis_ips = ips[0:6]
    print("Redis IP's: {}".format(redis_ips))

    client_ip = ips[6]
    print("Client IP: {}".format(client_ip))

    worker_ips = ips[7:]
    print("Worker IP's: {}".format(worker_ips))

    launch_redis_servers(ips = redis_ips, kill_first = False, connect_and_ping = True)

    # for ip in redis_ips:
    #     redis_client = redis.Redis(host = ip, port = 6379, db = 0)
    #     print("Pinging Redis instance @ {}:6379 now...".format(ip))
    #     res = redis_client.ping()
    #     if res is False:
    #         raise Exception("ERROR: Redis instance @ {} did not start correctly!".format(ip))
    #     else:
    #         print("True")

    update_redis_hosts(ips = [client_ip], redis_ips = redis_ips)

    wondershape(ips = [client_ip] + worker_ips + redis_ips)

    nReducers = workers_per_vm * len(worker_ips) * 3
    print("nReducers = {}".format(nReducers))
    launch_client(client_ip = client_ip, nReducers = nReducers, s3_key_file = "/home/ubuntu/project/src/InfiniCacheMapReduceTest/util/5GB_S3Keys.txt")

    launch_workers(client_ip = client_ip, redis_ips = redis_ips, worker_ips = worker_ips, workers_per_vm = workers_per_vm)
