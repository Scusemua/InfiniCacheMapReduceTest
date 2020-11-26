ips=$(eval "aws ec2 describe-instances --query "Reservations[*].Instances[*].PublicIpAddress" --output=text")

for host in "${redisHostnames[@]}"
do
   echo "Starting Redis instance on EC2 instance at $host"
   if ! ssh host "sudo redis-server redis.conf"
   then
      echo "Failed to start Redis server..."
   fi
done

