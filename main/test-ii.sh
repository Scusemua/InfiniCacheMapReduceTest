#!/bin/bash
go run client.go localhost:1234 ii 16 pg-*.txt &
pids[0]=$!

go run worker.go localhost:1235 localhost:1234 100 &
pids[1]=$!

echo "[Test]: waiting for client and worker to finish..." > /dev/stderr
for pid in ${pids[*]}; do
    wait $pid
done

echo "[Test]: assert(mr-final.ii.out)" > /dev/stderr

sort -k1,1 mr-final.ii.out | sort -snk2,2 | grep -v '16' | tail -10 | diff - mr-challenge.txt > diff.out
if [ -s diff.out ]
then
echo "Failed test. Output should be as in mr-challenge.txt. Your output differs as follows (from diff.out):" > /dev/stderr
  cat diff.out
else
  echo "Passed test!" > /dev/stderr
fi

# clean up generated intermediate and output files
rm mrtmp.* mr.ii-res*
