# MapReduce Framework

This is a MapReduce framework. The scripts for remotely orchestrating benchmarks are in util/launch_cluster.py. 

In this branch, the benchmarks use ECClient InfiniStore instead of the Redis protocol.

## Old README below:

# CS 675 Lab 2: Distributed and Fault-tolerant MapReduce

## Important Dates and Stuff

**Due** Monday, 03/02, midnight.

This is an **individual** lab.

Using a Linux environment for development and testing is **highly
recommanded**. By default you get [750 hours of free tier
usage](https://aws.amazon.com/ec2/?did=ft_card&trk=ft_card) for
[Amazon EC2 virtual
machines](https://aws.amazon.com/ec2/?did=ft_card&trk=ft_card). Why
not use that for development and testing? If you want to do moderately-scaled
testing, spare a few cents (out of your $100 AWS credits) to spin up a few powerful VMs (e.g., 
the `m5.2xlarge` with 8 CPUs and 32GB memory) and run your tests. 
See [EC2 on demand](https://aws.amazon.com/ec2/pricing/on-demand/) for
more detail. But make sure to **stop** your VMs when they are not being used.

## Introduction

This part of the assignment continues from Lab 1 -- building a MapReduce
framework as a way to learn the Go programming language and as a way to learn
about fault tolerance in distributed systems. You will
tackle a distributed MapReduce library, writing code for a driver
(i.e., the master)
that hands out tasks to multiple workers and handles failures in workers.
The interface to the library and the approach to fault tolerance is similar to the one described in
the original [MapReduce paper](https://tddg.github.io/cs675-spring20/public/papers/mapreduce_osdi04.pdf).
In this lab, you will complete a sample MapReduce application.

## Software

You will reuse most of the serverless package inherited from Lab 1,
focusing this time on implementing a working MapReduce framework
prototype. 

Over the course of this lab, you will have to modify `schedule` in
`schedule.go`, as well as `doMap` (and `mapF`) and `doReduce` (and `reduceF`) in
`plugins/wcm_service.go` and `plugins/wcr_service.go` (for word count
named with a prefix `wc`) respectively.

Recall that in Lab 1, you were asked to finish the implementation of 
`worker.go` in `main/` and `driver.go` in `serverless/`. In this lab,
you can reuse the code you've implemented in Lab 1. Those places are
marked with the following comments:

```go
   //
   // TODO: Lab 2 will reuse the code you've implemented in Lab 1
   //
```

As with the previous part of this assignment, you should not modify
any other files, but reading them might be useful in order to
understand how the other methods fit into the overall architecture of
the system.

To get started, fork this repository from GitLab into your account
and change its visibility to **Private**. 

## Implementing a Distributed MapReduce Framework

### Overall MapReduce Flow

The MapReduce package (located at `serverless/`) provides a simple 
MapReduce library. Applications would normally call `Driver.Run()`
located in `main/client.go` to start a job. 

The flow of the MapReduce implementation is as follows:

1. The application provides a number of input files, a map go plugin
   Lambda service module, a reduce go plugin Lambda service module,
and the number of reduce tasks (`nReduce`). Typically, the number of
input files is the number of map tasks.

2. A driver is created with this knowledge. It spins up an RPC server
   (see `serverless/driver.go`), and waits for workers to register
(using the RPC call `Register()` defined in `serverless/driver.go`).
As tasks become available (via explicit service registration RPC call
`Worker.RegisterService` issued by the driver to each worker),
`schedule()` -- located in `serverless/schedule.go` -- decides how
to assign those tasks to workers, and how to handle worker failures.

3. The driver considers each input file one map task, and makes a
   call to `DoService()` (the generic interface provided by a Lambda
plugin service module), which further calls `doMap()` in
`plugins/wcm_service.go` at least once for each task. It does so
by issuing the `Worker.InvokeService` RPC -- implemented in
`main/worker.go` -- on a worker. Each call to `doMap()` reads the
appropriate file, calls the map function on that file's contents, and
produces `nReduce` files for each map file. Thus, after all map tasks
are done, the total number of files will be the product of the number
of files given to map (`numInFiles` or `len(inFiles)`) and `nReduce`.

```
f0-0, ..., f0-[nReduce-1],
...
f[numInFiles-1]-0, ..., f[numInFiles-1]-[nReduce-1].
```

4. The driver next makes a call to `doReduce()` in
   `plugins/wcr_service.go` at least once for each reduce task. As
with `doMap()`, it does so through a worker. `doReduce()` collects
corresponding files from each map result (e.g., `f0-i, f1-i, ...,
f[numInFiles-1]-i`), and runs the reduce function on each collection.
This process produces `nReduce` result files.

5. The driver calls `drv.merge()` in `serverless/mr_misc.go`, which
   merges all the `nReduce` files produced by the previous step into
a single output. 

6. The driver sends a `Shutdown` RPC to each of its workers, and then 
shuts down its own RPC server. 


### Part A: MapReduce Task Scheduling

One of MapReduce's biggest selling points is that the developer
should not need to be aware that their code is running in parallel on
many machines. In theory, we should be able to take the word count
code you will write as part of this lab, and automatically
parallelize it!

Our current serverless framework runs simple `helloworld` plugin
Lambdas concurrently on the worker processes. In this lab, you will
complete a version of MapReduce that splits the work up over a set of
worker threads (i.e., goroutines) in order to exploit multiple cores.
Computing the map tasks in parallel and then the reduce tasks can
result in much faster completion, but is also harder to implement and
debug.  Note that for this lab, the work is not distributed across
multiple physical machines as in **"real"** MapReduce deployments;
however, you are more than welcome to setup a truly distributed
testbed deployment, and test your implementation.

To coordinate the parallel execution of tasks, we will use a special
client process, which runs a driver to hand out work to one or
multiple worker processes and waits for them to finish. To make the
assignment more realistic, the driver should only communicate with
the workers via RPC. We give you the worker code (`main/worker.go`),
the code that starts the workers, and code to deal with RPC messages
(`serverless/common_rpc.go`).

Your job is to complete `schedule.go` in the `serverless` package. In
particular, you should modify `schedule()` in `schedule.go` to hand
out the map and reduce tasks to workers, and return only when all the
tasks have finished.

Look at `run()` in `driver.go`. It calls your `schedule()` to run the
map and reduce tasks, then calls `merge()` (defined in
`serverless/mr_misc.go`) to assemble the per-reduce-task outputs into
a single output file. `schedule` only needs to tell the workers the
name of the original input file (`mr.files[task]`) and the task
`task`; each worker knows from which files to read its input and to
which files to write its output. The driver tells the worker about a
new task by sending it the RPC call `Worker.InvokeService`, giving a
`RPCArgs` object as the RPC argument (that further encapsulates the
per-task `MapReduceArgs` argument). The `RPCArgs` RPC argument should
use data marshalling/unmarshalling library such as `encoding/gob`
that you used in Lab 1.

When a worker starts, it sends a Register RPC to the driver.
`driver.go` already implements the driver's `Driver.Register` RPC
handler for you. Before starting a certain phase of tasks, the driver
will first register the corresponding plugin Lambdas (namely
`wcm_service.so` and `wcr_service.so` for the word count example) at
each worker, and passes the worker's information to
`drv.registerChan`. Your `schedule` should grab workers from this
channel to assign tasks.

Information about the currently running job is in the `Driver
struct`, defined in `driver.go`. Note that the driver does not need
to know which Map or Reduce functions are being used for the job; the
workers will take care of executing the right code for Map or Reduce
(the correct functions are registered at each worker and dispatched
when `Driver.run()` are called in `serverless/driver.go`).


### Part B: MapReduce Input and Output

> **NOTE:** Both Part B and C deal with the plugin implementations
located at `plugins/wcm_service.go` and `plugins/wcr_service.go` --
your `doMap()` and `doReduce()` function will handle the MapReduce
input and output, while your `mapF()` and `reduceF()` will carry 
out the specific data processing. 

The MapReduce implementation you are given is missing some pieces.
Before you can write your first MapReduce function pair (i.e., the go
plugin module pair), you will need to fix the input & output part. In
particular, the code we give you is missing two crucial pieces: the
function that divides up the output of a map task, and the function
that gathers all the inputs for a reduce task. These tasks are
carried out by the `doMap()` function in `plugin/wcm_service.go`, and
the `doReduce()` function in `serverless/wcr_service.go`
respectively. The comments in those files should point you in the
right direction. 

### Part C: Word Count

To put your implementation in context, now is time to implement some
interesting MapReduce operations. For this lab, we will be
implementing word count -- a simple and classic MapReduce example.
Specifically, your task is to extend `wcm_service.go` and
`wcr_service.go` located at `plugins` to implement `mapF()` and
`reduceF()`, so that the application
reports the number of occurrences of each word. A word is any
contiguous sequence of letters, as determined by `unicode.IsLetter`.

There are some input files with pathnames of the form `pg-*.txt` in
the `main` directory, downloaded from [Project
Gutenberg](https://www.gutenberg.org/ebooks/search/%3Fsort_order%3Ddownloads).
Remember to set `GOPATH` before continuing. The following are the
commands when you try to run a word count MapReduce job: 

```bash
$ cd "$GOPATH/src/cs675-spring20-labs/lab2/main"
$ go run client.go localhost:1234 wc pg-*.txt
localhost:1234: Starting driver RPC server
2020/02/09 21:23:17 rpc.Register: method "Lock" has 1 input parameters; needs exactly three
2020/02/09 21:23:17 rpc.Register: method "Run" has 4 input parameters; needs exactly three
2020/02/09 21:23:17 rpc.Register: method "Unlock" has 1 input parameters; needs exactly three
2020/02/09 21:23:17 rpc.Register: method "Wait" has 1 input parameters; needs exactly three
localhost:1234: Starting MapReduce job: wc
localhost:1234: To start the Map phase...
Driver: enter the worker registration service loop...
...
```

```bash
// In a separate terminal session
$ cd "$GOPATH/src/cs675-spring20-labs/lab2/main"
$ go run worker.go localhost:1235 localhost:1234 100
2020/02/09 21:24:41 rpc.Register: method "Lock" has 1 input parameters; needs exactly three
2020/02/09 21:24:41 rpc.Register: method "Unlock" has 1 input parameters; needs exactly three
Successfully registered worker localhost:1235
Worker: localhost:1235 To start the RPC server...
Worker: To register new service wcm_service
Successfully registered new service wcm_service
Worker: InvokeService: parsed service name: wcm_service
...
```

Before you start coding read Section 2 of the [MapReduce
paper](https://tddg.github.io/cs675-spring20/public/papers/mapreduce_osdi04.pdf).
Your `mapF()` and `reduceF()` functions will differ a bit from those
in the paper's Section 2.1. Your `mapF()` will be passed the name of
a file, as well as that file's contents; it should split it into
words, and return a Go slice of key/value pairs, of type
`serverless.KeyValue`.  Your `reduceF()` will be called once for each
key, with a slice of all the values generated by `mapF()` for that
key; it should return a single output value. 

You can test your solution using the above two commands. The output
will be in the file mr-final.wc.out. We will test your
implementation's correctness with the following command, which should
produce the following top 10 words:

```bash
$ sort -n -k2 mr-final.wc.out | tail -10
he: 34077
was: 37044
that: 37495
I: 44502
in: 46092
a: 60558
to: 74357
of: 79727
and: 93990
the: 154024
```

(this sample result is also found in `main/mr-testout.txt`)

You can remove the output file and all intermediate files with: 

```bash
$ rm mrtmp.* mr.wc-res-* mr-final.wc.out
```

To make testing easy for you, from the `$GOPATH/src/cs675-spring20-labs/lab2/main` directory, run: 

```bash
$ ./test-wc.sh
```

and it will report if your solution is correct or not. 


### Part D: Handling Worker Failures

In this part you will make the driver handle failed workers.
MapReduce makes this relatively easy because workers don't have
persistent state. If a worker fails, any RPCs that the driver issued
to that worker will fail (e.g., due to a timeout). Thus, if the
driver's RPC to the worker fails, the master should re-assign the
task given to the failed worker to another worker.

An RPC failure doesn't necessarily mean that the worker failed; the
worker may just be unreachable but still computing. Thus, it may
happen that two workers receive the same task and compute it.
However, because tasks are idempotent, it doesn't matter if the same
task is computed twice -- both times it will generate the same output.
So, you don't have to do anything special for this case. (Our test
never fail workers in the middle of task, so you don't even have to
worry about several workers writing to the same output file.)

You don't have to handle failures of the driver; we will assume it
won't fail. Making the driver fault-tolerant is more difficult
because it keeps persistent state that would have to be recovered in
order to resume operations after a driver failure. 

Your implementation must pass the next few test cases defined in
`test-single-failure.sh` and `test-many-failures.sh`. These testing scripts test handling of
one or multiple
failures of workers (i.e., multiple goroutines).  The test case
starts new workers that the driver can use to make forward progress,
but these workers fail after handling a few tasks (less than 10 as
specified by the last command-line argument passed to `go run
worker.go`). For the `test-many-failures.sh`, the new workers fail
again and another new workers (finally with enough `nRPC`s) are
started after a 5-second sleep. Run the test as follows. Remember
again to set your `GOPATH` first. 

```bash
// to test robustness against a single failure
$ ./test-single-failure.sh
// to test robustness against two consecutive failures
$ ./test-many-failures.sh
```

Driver crash or hang is treated as **failed to pass the test**. To
pass the test, the script must print out `Passed test!`. 


### Part E: Inverted Index Generation

Word count (we call it `wc` when passing the job name as a command-line
argument) is a classical example of a MapReduce application, but
it is not an application that many large consumers of MapReduce use.
It is simply not very often you need to count the words in a really
large dataset. For this application exercise, we will instead have
you build Map and Reduce functions for generating an inverted index.
We call inverted index `ii` when passing in the job name to run
the client code.

Inverted indices are widely used in computer science, and are
particularly useful in document searching. Broadly speaking, an
inverted index is a map from interesting facts about the underlying
data, to the original location of that data. For example, in the
context of search, it might be a map from keywords to documents that
contain those words. 

We have created a second set of mapper and reducer Go plugin modules
in `plugins/`: `iim_service.go` and `iir_service.go`, which are very
similar to the Word Count modules you built earlier. You should
modify `mapF()` and `reduceF()` in these two files so that they
together produce an inverted index.  You can pretty much reuse your
implementation of the `doMap()` function as it is mainly responsible
for handling MapReduce's I/Os (input and output), and should be
generic across these two representative applications.

```bash
$ ./test-ii.sh
// ... all outputs omitted here ...
$ head -n5 mr-final.ii.out
A: 16 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
ABC: 2 pg-les_miserables.txt,pg-war_and_peace.txt
ABOUT: 2 pg-moby_dick.txt,pg-tom_sawyer.txt
ABRAHAM: 1 pg-dracula.txt
ABSOLUTE: 1 pg-les_miserables.txt
```


If it is not clear from the listing above, the format is:

```
word: #documents documents,sorted,and,separated,by,commas
```

I will test your implementation's correctness with the following
command, which should produce these resulting last 10 items in the
index: 

```bash
$ sort -k1,1 mr-final.ii.out | sort -snk2,2 | grep -v '16' | tail -10 
women: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
won: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
wonderful: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
words: 15 pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
worked: 15 pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
worse: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
wounded: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
yes: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-metamorphosis.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
younger: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
yours: 15 pg-being_ernest.txt,pg-dorian_gray.txt,pg-dracula.txt,pg-emma.txt,pg-frankenstein.txt,pg-great_expectations.txt,pg-grimm.txt,pg-huckleberry_finn.txt,pg-les_miserables.txt,pg-moby_dick.txt,pg-sherlock_holmes.txt,pg-tale_of_two_cities.txt,pg-tom_sawyer.txt,pg-ulysses.txt,pg-war_and_peace.txt
```

(this sample result is also found in `main/mr-challenge.txt`) 

Once again, to make testing easy for you, from `main/` directory, run:

```bash
$ ./test-ii.sh
```

and it will report if your solution is correct or not.



## Resources and Advice

* The driver should send RPCs to the workers in parallel so that the
  workers can work on tasks concurrently. You will find the go
statement useful for this purpose and the [Go RPC
documentation](https://golang.org/pkg/net/rpc/). 

* The driver may have to wait for a worker to finish before it can
  hand out more tasks. You may find channels useful to synchronize
threads that are waiting for reply with the driver once the reply
arrives. Channels are explained in the document on [Concurrency in
Go](https://golang.org/doc/effective_go.html#concurrency). 

* The code we give you runs the workers and client processes within a single
machine, and can exploit multiple cores on that machine. To test a
truly distributed setup, there would need to be a way to start worker
processes on all the machines; and all the machines would have to
share storage through some kind of network file system (e.g., [Linux
NFS](http://nfs.sourceforge.net/)).

* The easiest way to track down bugs is to insert `Debug()` (or `serverless.Debug()`) statements,
 set `debugEngabled = true` in `serverless/common.go`, collect the output
in a file with, e.g., 

	```bash
	./test-wc.sh > out 2>&1
	```
	 
	and then think about whether the output matches your understanding of
	how your code should behave. The last step (thinking) is the most
	important.  

* When your code finishes, you may receive errors like ``accept
error: accept tcp 127.0.0.1:1234: use of closed network
connection``. You can ignore these as the framework intentionally closes
the connection to shut down the RPC when everything is finished.
 



## Point Distribution

<table>
<tr><th>Component</th><th>Points</th></tr>
<tr><td>Scheduler</td><td>15</td></tr>
<tr><td>Word Count Plugins</td><td>15</td></tr>
<tr><td>Inverted Index Plugins</td><td>10</td></tr>
<tr><td>test-wc.sh</td><td>15</td></tr>
<tr><td>test-ii.sh</td><td>15</td></tr>
<tr><td>test-single-failure.sh</td><td>10</td></tr>
<tr><td>test-many-failures.sh</td><td>10</td></tr>
</table>


## Submitting Lab 2

1. **Submit the electronic version**

You hand in your lab assignment exactly as you've been letting me know your progress:

```bash
$ git commit -am "[you fill me in]"
$ git tag -a -m "i finished lab 2" lab2-handin
$ git push origin master lab2-handin
```

You should verify that you are able to see your final commit and your
`lab2-handin` tag on the GitLab page in your repository for this lab.

We will use the timestamp of your **last** tag for the
purpose of calculating late days, and we will only grade that version of the
code. (We'll also know if you backdate the tag, don't do that.)

You will need to share your private repository with me (the instructor)
(my GitLab ID is the same as my mason email ID: `yuecheng`). 

> **NOTE:** Granting me a **Maintainer** access role would be helpful so that I can check in my
comments if needed.

2. **Schedule a meeting and discuss**

As a second part of the submission, you'll meet with me and explain what you
did for Lab 2. Hopefully we will use the office hour for this
after the due of Lab 2. We will also have a signup sheet as the date 
approaches, and I'll also give a little more detail in class.


## Acknowledgements

Part of this lab is adapted from MIT's 6.824 course. Thanks to
Frans Kaashoek, Robert Morris, and Nickolai Zeldovich for their
support.

