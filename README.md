# MIT 6.5840 MapReduce — Distributed MapReduce Implementation (IMPORTANT - Readme file created with the help of CHATGPT, so it may contain mistakes)

This repository contains my implementation of the **MapReduce lab from MIT 6.5840 (Distributed Systems)**.

The implementation consists of:

```text
coordinator.go
worker.go
rpc.go
```

These files implement the distributed MapReduce Coordinator, Workers, and RPC communication.

The code is intended to be copied into the official MIT 6.5840 lab repository and tested using the provided MapReduce test suite.

---

## 1. MIT 6.5840 MapReduce Lab

The MIT lab asks us to implement a distributed MapReduce system consisting of:

* **One Coordinator**
* **One or more Workers**
* RPC communication between Workers and the Coordinator
* Map task scheduling
* Reduce task scheduling
* Worker failure detection and task re-assignment

The official lab specifies that the implementation should be placed in:

```text
src/mr/coordinator.go
src/mr/worker.go
src/mr/rpc.go
```

The provided files:

```text
src/main/mrcoordinator.go
src/main/mrworker.go
```

contain the main programs used to start the Coordinator and Workers and should not be modified.

---

# 2. Clone the MIT Repository

Clone the official MIT 6.5840 2026 lab repository:

```bash
git clone git://g.csail.mit.edu/6.5840-golabs-2026 6.5840
cd 6.5840
```

The repository has the following basic structure:

```text
6.5840/
├── Makefile
└── src/
    ├── main/
    ├── mr/
    └── mrapps/
```

This is the repository structure specified by the MIT lab.

---

# 3. Add This Implementation

This repository contains:

```text
coordinator.go
worker.go
rpc.go
```

Copy these three files into the MIT repository's `src/mr/` directory.

For example, if this repository is cloned alongside the MIT repository:

```bash
cd 6.5840/src/mr

cp /path/to/this/repository/coordinator.go .
cp /path/to/this/repository/worker.go .
cp /path/to/this/repository/rpc.go .
```

Alternatively, clone this repository and copy the files directly:

```bash
cp /path/to/mapreduce-implementation/coordinator.go 6.5840/src/mr/
cp /path/to/mapreduce-implementation/worker.go 6.5840/src/mr/
cp /path/to/mapreduce-implementation/rpc.go 6.5840/src/mr/
```

After copying, the relevant directory should look like:

```text
6.5840/
└── src/
    ├── main/
    │   ├── mrcoordinator.go
    │   ├── mrworker.go
    │   ├── mrsequential.go
    │   └── ...
    │
    ├── mr/
    │   ├── coordinator.go    <-- implementation
    │   ├── worker.go        <-- implementation
    │   ├── rpc.go           <-- implementation
    │   ├── mr_test.go
    │   └── ...
    │
    └── mrapps/
        ├── wc.go
        ├── indexer.go
        ├── crash.go
        └── ...
```

**Do not replace or modify `src/main/mrcoordinator.go` or `src/main/mrworker.go`.**

---

# 4. Build the Word Count Plugin

The MIT lab provides a Word Count MapReduce application in:

```text
src/mrapps/wc.go
```

Build it as a Go plugin:

```bash
cd 6.5840/src/main

go build -buildmode=plugin ../mrapps/wc.go
```

This generates:

```text
wc.so
```

The official lab uses the same command to build the Word Count plugin.

---

# 5. Run the MapReduce Implementation

## Start the Coordinator

From:

```bash
cd 6.5840/src/main
```

remove any previous output files:

```bash
rm mr-out*
```

Then start the Coordinator:

```bash
go run mrcoordinator.go sock123 pg-*.txt
```

Here:

* `sock123` is the Unix socket used for Coordinator/Worker RPC.
* `pg-*.txt` are the input files.
* Each input file corresponds to one Map task.

This is the execution flow specified by the MIT lab.

---

# 6. Start Workers

Open one or more additional terminals.

From:

```bash
cd 6.5840/src/main
```

start a Worker:

```bash
go run mrworker.go wc.so sock123
```

You can run multiple workers simultaneously.

For example:

### Terminal 1 — Coordinator

```bash
cd 6.5840/src/main
go run mrcoordinator.go sock123 pg-*.txt
```

### Terminal 2 — Worker

```bash
cd 6.5840/src/main
go run mrworker.go wc.so sock123
```

### Terminal 3 — Worker

```bash
cd 6.5840/src/main
go run mrworker.go wc.so sock123
```

### Terminal 4 — Worker

```bash
cd 6.5840/src/main
go run mrworker.go wc.so sock123
```

The MIT lab specifies that one or more Workers can run in parallel and communicate with the Coordinator through RPC.

---

# 7. Check the Output

After the Coordinator and Workers finish, the Reduce tasks produce:

```text
mr-out-0
mr-out-1
mr-out-2
...
```

Check the generated files:

```bash
ls mr-out-*
```

To inspect the complete result:

```bash
cat mr-out-* | sort | more
```

The output should contain Word Count results such as:

```text
A 509
ABOUT 2
ACT 8
ACTRESS 1
...
```

The official lab uses the sorted union of the `mr-out-*` files to verify the result.

---

# 8. Run the MIT Test Suite

The most important way to verify the implementation is to run the official tests.

From the `src` directory:

```bash
cd 6.5840/src
make mr
```

The MIT test suite is located in:

```text
src/mr/mr_test.go
```

and tests:

* Word Count
* Indexer
* Map parallelism
* Reduce parallelism
* Job completion
* Early exit
* Worker crash recovery

These are the tests specified by the MIT lab.

---

# 9. Expected Test Output

A successful implementation should produce output similar to:

```text
$ make mr
...
=== RUN   TestWc
--- PASS: TestWc (...)
=== RUN   TestIndexer
--- PASS: TestIndexer (...)
=== RUN   TestMapParallel
--- PASS: TestMapParallel (...)
=== RUN   TestReduceParallel
--- PASS: TestReduceParallel (...)
=== RUN   TestJobCount
--- PASS: TestJobCount (...)
=== RUN   TestEarlyExit
--- PASS: TestEarlyExit (...)
=== RUN   TestCrashWorker
--- PASS: TestCrashWorker (...)
PASS
ok      6.5840/mr    ...
```

The exact execution times and crash-recovery messages will vary between runs. The MIT-provided example shows all of these tests passing, including `TestCrashWorker`.

---

# 10. Test Individual Components

You can run an individual test using:

```bash
make RUN="-run Wc" mr
```

For example:

```bash
make RUN="-run TestWc" mr
```

This is useful when debugging a particular part of the implementation.

The MIT lab also notes that modifying files in `src/mr/` may require rebuilding the MapReduce plugins.

---

# 11. MapReduce Execution Flow

The overall execution is:

```text
                    +----------------+
                    |  Coordinator   |
                    +-------+--------+
                            |
                         RPC calls
                            |
              +-------------+-------------+
              |             |             |
              v             v             v
          +-------+     +-------+     +-------+
          |Worker1|     |Worker2|     |Worker3|
          +---+---+     +---+---+     +---+---+
              |             |             |
              v             v             v
           Map Tasks      Map Tasks      Map Tasks
              |             |             |
              +-------------+-------------+
                            |
                     Intermediate Files
                       mr-X-Y
                            |
                            v
                    +---------------+
                    | Reduce Tasks  |
                    +-------+-------+
                            |
                            v
                       mr-out-X
```

The Coordinator first schedules Map tasks. Reduce tasks can start after the Map phase has completed.

---

# 12. Task Failure Recovery

The Coordinator is expected to detect Workers that do not complete their tasks within a reasonable amount of time.

For this lab, the timeout is:

```text
10 seconds
```

If a Worker fails while processing a task, the Coordinator should make that task available to another Worker.

The official tests specifically include:

```text
TestCrashWorker
```

to verify this behavior.

The MIT lab also provides:

```text
mrapps/crash.go
```

which can be used to test Worker crash recovery.

---

# 13. Intermediate and Final Files

Map Workers create intermediate files using the convention:

```text
mr-X-Y
```

where:

* `X` = Map task number
* `Y` = Reduce task number

For example:

```text
mr-0-0
mr-0-1
mr-1-0
mr-1-1
```

The Reduce Worker reads these intermediate files and produces:

```text
mr-out-0
mr-out-1
...
```

The MIT lab specifies these naming conventions and requires each Reduce task to produce its corresponding `mr-out-X` file.

---

# 14. Terminal Output



## Test Output

```text
% make mr
go build -race -o main/mrsequential main/mrsequential.go
go build -race -o main/mrcoordinator main/mrcoordinator.go
go build -race -o main/mrworker main/mrworker.go&
(cd mrapps && go build -race -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin nocrash.go) || exit 1
cd mr; go test -v -race 
=== RUN   TestWc
--- PASS: TestWc (6.97s)
=== RUN   TestIndexer
--- PASS: TestIndexer (5.57s)
=== RUN   TestMapParallel
--- PASS: TestMapParallel (8.09s)
=== RUN   TestReduceParallel
--- PASS: TestReduceParallel (9.09s)
=== RUN   TestJobCount
--- PASS: TestJobCount (11.11s)
=== RUN   TestEarlyExit
--- PASS: TestEarlyExit (11.27s)
=== RUN   TestCrashWorker
--- PASS: TestCrashWorker (27.32s)
PASS
ok      6.5840/mr       81.046s
```


# 15. Important Notes

### Implementation files

Only these three files contain the implementation from this repository:

```text
src/mr/coordinator.go
src/mr/worker.go
src/mr/rpc.go
```

### Provided MIT files

The MIT repository provides the following components around the implementation:

```text
src/main/mrcoordinator.go
src/main/mrworker.go
src/main/mrsequential.go
src/mrapps/wc.go
src/mrapps/indexer.go
src/mrapps/crash.go
src/mr/mr_test.go
```

The lab instructions specifically state that `mrcoordinator.go` and `mrworker.go` should not be changed.

### Shared filesystem

The lab runs all Workers on the same machine and relies on them sharing the filesystem for intermediate Map output.

---

# 16. Quick Start

For someone who just wants to reproduce the result:

```bash
# 1. Clone MIT repository
git clone git://g.csail.mit.edu/6.5840-golabs-2026 6.5840

# 2. Copy implementation
cp coordinator.go 6.5840/src/mr/
cp worker.go 6.5840/src/mr/
cp rpc.go 6.5840/src/mr/

# 3. Build Word Count plugin
cd 6.5840/src/main
go build -buildmode=plugin ../mrapps/wc.go

# 4. Run Coordinator
rm mr-out*
go run mrcoordinator.go sock123 pg-*.txt

# 5. In another terminal, run Worker
go run mrworker.go wc.so sock123

# 6. Run official tests
cd ..
make mr
```

For the complete evaluation, use:

```bash
cd 6.5840/src
make mr
```

---

# 17. Reference

This implementation follows the structure and requirements of the **MIT 6.5840 Distributed Systems MapReduce Lab**.

The lab specification covers the Coordinator/Worker architecture, RPC communication, task scheduling, output conventions, testing, parallelism, and crash recovery.
