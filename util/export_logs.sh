#!/bin/bash
LAMBDA="/aws/lambda/"
FILE="log/"
LOG_PREFIX="CacheNode"

PREFIX=$1
start=$2
end=$3 

# Convert date into seconds (Format is %s)
startTime=$(date -d "$start" +%s)000
endTime=$(date -d "$end" +%s)000

FROM=0
TO=399
if [ "$4" != "" ] ; then
  FROM=$4
  TO=$4
fi
if [ "$5" != "" ] ; then
  TO=$5
fi

# Get the number of individual Lambda deployments for which we need to export logs.
NUM_DEPLOYMENTS=$6

echo ("Waiting for the last task to end.")
# Wait for the end the last task
for j in {0..15}
do
  RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
  echo "RUNNING = $RUNNING"
  if [ "$RUNNING" != "" ]; then
    sleep 2s
  else
    break
  fi
done

# Abandon
if [ "$RUNNING" != "" ]; then
  echo "Detect running task and wait timeout, killing task \"$RUNNING\"..."
  aws logs cancel-export-task --task-id \"$RUNNING\"

fi

echo "Waiting another 30 seconds for the abandon procedure"
# Wait another 30 seconds for the abandon procedure
for j in {0..15}
do
  RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
  echo "RUNNING = $RUNNING"
  if [ "$RUNNING" != "" ]; then
    sleep 2s
  else
    break
    echo "Done"
  fi
done

echo "Time to export the logs."
for (( x=0; x <$NUM_DEPLOYMENTS; x++))
do
    echo "Exporting logs for deployment #$x"
    for (( i=$FROM; i<=$TO; i++ ))
    do
        # Try 3 times.
        for k in {0..2}
        do
            echo "Exporting $LAMBDA$LOG_PREFIX$i"
            aws logs create-export-task --log-group-name $LAMBDA$LOG_PREFIX$x-$i --from ${startTime} --to ${endTime} --destination "tianium.default" --destination-prefix $FILE$PREFIX$LOG_PREFIX$x-$i
            sleep 2s

            # Wait for the end the last task
            for j in {0..15}
            do
                RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
                if [ "$RUNNING" != "" ]; then
                    sleep 2s
                else
                    break
                fi
            done

            # Abandon
            if [ "$RUNNING" != "" ]; then
                echo "Detect running task and wait timeout, killing task \"$RUNNING\"..."
                aws logs cancel-export-task --task-id \"$RUNNING\"
            else
                break
            fi

            # Wait another 30 seconds for the abandon procedure
            for j in {0..15}
            do
                RUNNING=`aws logs describe-export-tasks --status-code "RUNNING" | grep taskId | awk -F \" '{ print $4 }'`
                if [ "$RUNNING" != "" ]; then
                    sleep 2s
                else
                    break
                    echo "Done"
                fi
            done
        done
    done
done