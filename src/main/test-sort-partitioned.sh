#!/bin/bash
go run client.go localhost:1234 srt 5 data500.dat part*.dat &
pids[0]=$!

go run worker.go localhost:1235 localhost:1234 100 & 
pids[1]=$!

echo "[Test]: waiting for client and worker to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

#echo "[Test]: assert(mr-final.sort.out)" > /dev/stderr

#sort -n -k2 mr-final.sort.out | tail -10 | diff - mr-testout.txt > diff.out
#if [ -s diff.out ]
#then
#echo "Failed test. Output should be as in mr-testout.txt. Your output differs as follows (from diff.out):" > /dev/stderr
#  cat diff.out
#else
#  echo "Passed test!" > /dev/stderr
#fi

# clean up generated intermediate and output files
rm mrtmp.* mr.srt-res* #mr-final.srt.out
