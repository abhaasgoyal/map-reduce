# Overview

A distributed MapReduce System, consisting of two programs, the master and the worker. 

## Architecture

- There is one master process, and one or more worker processes executing in parallel. 
- The workers talk to the master via RPC. 
- Each worker process asks the master for a task, read the task's input from one or more files, execute the task, and write the task's output to one or more files. 
- The master notices if a worker hasn't completed its task in a reasonable amount of time (10 seconds), and give the same task to a different worker.

## Building plugins

1. Build the respective map-reduce application (located under `mrapps/`)

```sh
$ go build -buildmode=plugin mrapps/${plugin}.go
```

## Running the main programs

In the `main` directory, run the master and one/more workers

```sh
# Running the master process
$ go run mrmaster.go pg-*.txt
# Running a sample worker process
$ go run mrworker.go wc.so
```

There is also a sequential program `mrsequential.go` providing the serial implementation.

## Testing

A test script is provided in `main/test-mr.sh`. The tests check that the `wc` and `indexer` MapReduce applications produce the correct output when given the `pg-xxx.txt` files as input. The tests also check that your implementation runs the Map and Reduce tasks in parallel, and that your implementation recovers from workers that crash while running tasks.

```sh
# Running tests a single time
$ sh tests/test-mr.sh
# Running test suite multiple times to have higher degree of confidence (particularly w.r.t crashing/sleeping processes)
$ sh tests/test-mr-many.sh
```

## Credits

This work is a modification of a lab of MIT's course in Distributed System (6.824) [link](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html)
