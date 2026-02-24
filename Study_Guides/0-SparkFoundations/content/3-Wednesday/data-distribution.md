# Data Distribution Across Executors

## Learning Objectives
- Understand how data is spread across executors
- Explain the concept of data locality
- Define the "move compute to data" principle
- Recognize the cost of data movement in distributed systems

## Why This Matters

In a distributed system, where data lives determines how fast your job runs. Reading data from a local disk is orders of magnitude faster than pulling it across the network. Spark is designed around this reality—it tries to process data where it already exists rather than moving it around.

Understanding data distribution helps you:
- Write more efficient Spark applications
- Diagnose performance problems
- Make informed decisions about storage and partitioning

---

## How Data is Distributed

When you load data into Spark, it gets distributed across the cluster:

```
+------------------------------------------------------------------+
|                         DATA SOURCE                              |
|              (HDFS, S3, or other distributed storage)            |
|                                                                  |
|   Block 1 (128 MB) - stored on Node A                            |
|   Block 2 (128 MB) - stored on Node B                            |
|   Block 3 (128 MB) - stored on Node C                            |
|   Block 4 (128 MB) - stored on Node A                            |
+------------------------------------------------------------------+
                               |
                               v
                         SPARK READS
                               |
                               v
+------------------------------------------------------------------+
|                         EXECUTORS                                |
|                                                                  |
|   Executor on Node A:  [Partition 1] [Partition 4]               |
|   Executor on Node B:  [Partition 2]                             |
|   Executor on Node C:  [Partition 3]                             |
|                                                                  |
|   Spark tries to place partitions near their source data!        |
+------------------------------------------------------------------+
```

---

## Data Locality Levels

Spark tracks how "close" data is to the computation. There are five locality levels:

```
BEST                                                    WORST
  |                                                       |
  v                                                       v

PROCESS_LOCAL -> NODE_LOCAL -> NO_PREF -> RACK_LOCAL -> ANY

     Fast                                              Slow
```

| Level | Meaning | Speed |
|-------|---------|-------|
| **PROCESS_LOCAL** | Data is in the same JVM as the task | Fastest |
| **NODE_LOCAL** | Data is on the same physical machine | Fast |
| **NO_PREF** | Data has no preferred location | Variable |
| **RACK_LOCAL** | Data is in the same data center rack | Slower |
| **ANY** | Data is on a different rack | Slowest |

---

### What Each Level Means

```
PROCESS_LOCAL (Same JVM)
+-----------------------+
|       EXECUTOR        |
|  +-------+ +-------+  |
|  | Task  | | Data  |  |  <- Data cached in this executor's memory
|  +-------+ +-------+  |
|      Direct memory access
+-----------------------+

NODE_LOCAL (Same Machine)
+-----------------------+
|        MACHINE        |
| +--------+  +-------+ |
| |EXECUTOR|  | DISK  | |  <- Data on local disk
| | [Task] |  | [Data]| |
| +--------+  +-------+ |
|      Local disk read
+-----------------------+

RACK_LOCAL (Same Rack)
+----------+         +----------+
| Machine1 |         | Machine2 |
| [Task]   | <------ | [Data]   |  <- Data on different machine in same rack
+----------+  1 ms   +----------+
          Same rack network

ANY (Different Rack)
+----------+         +----------+
| Rack 1   |         | Rack 2   |
| [Task]   | <------ | [Data]   |  <- Data on different rack
+----------+ 10+ ms  +----------+
         Cross-rack network
```

---

## The Cost of Data Movement

Moving data across the network is expensive:

```
+----------------------------------------------------------+
|               DATA ACCESS SPEED COMPARISON               |
|                                                          |
|   Memory (same JVM):     |==|               100 GB/s     |
|                                                          |
|   Local SSD:             |====|              3-7 GB/s    |
|                                                          |
|   Network (same rack):   |===|              1-2 GB/s     |
|                                                          |
|   Network (cross-rack):  |=|               0.5-1 GB/s    |
|                                                          |
|   Network (cross-DC):    |.|               0.01-0.1 GB/s |
|                                                          |
+----------------------------------------------------------+
```

Moving 1 TB of data:
- From memory: ~10 seconds
- From local SSD: ~2-5 minutes
- Across same rack: ~10-15 minutes
- Across racks: ~30+ minutes

---

## Move Compute to Data

Spark's guiding principle: **move the computation to where the data lives, not vice versa**.

```
WRONG APPROACH: Move Data to Compute

+----------+                        +----------+
|  Node A  |   Move 100 GB data     |  Node B  |
|  [Data]  | =====================> | [Task]   |
+----------+    Slow! (minutes)     +----------+


RIGHT APPROACH: Move Compute to Data

+----------+                        +----------+
|  Node A  |   Send task code       |  Node B  |
|  [Data]  | <-- (a few KB) ------- | [Driver] |
|  [Task]  |                        +----------+
+----------+   Fast! (milliseconds)
```

Sending code (kilobytes) is vastly faster than sending data (gigabytes).

---

## How Spark Achieves Data Locality

### 1. Preferred Locations

When Spark reads from distributed storage (like HDFS), it knows where each block is stored:

```
Spark asks HDFS: "Where is block 1?"
HDFS responds: "Block 1 is on Node A and Node C (replicas)"

Spark scheduler: "I should run the task for partition 1 on Node A or C"
```

---

### 2. Delay Scheduling

Spark waits (briefly) for data-local slots:

```
Time 0: Need to run Task 1 (data on Node A)
        Node A is busy, Node B is free
        
        Wait a bit... (default: 3 seconds)
        
Time 1: Node A becomes free!
        Run Task 1 on Node A (data-local)

---

Alternative (if wait too long):

Time 0: Need to run Task 1 (data on Node A)
        Node A is busy
        
        Wait... wait... (timeout)
        
Time 3: Waited too long, just run on Node B
        Data will be fetched over network
```

---

### 3. Task Placement

The scheduler considers locality when assigning tasks:

```
Available Tasks:
- Task 1: Prefers Node A
- Task 2: Prefers Node B
- Task 3: Prefers Node A

Available Executors:
- Executor on Node A: 2 cores free
- Executor on Node B: 2 cores free

Scheduler assigns:
- Task 1 -> Node A (local)
- Task 2 -> Node B (local)
- Task 3 -> Node A (local)

All tasks run with data locality!
```

---

## When Data Must Move

Some operations **require** data movement (shuffles):

```
BEFORE: Data distributed by row

Executor 1: Row 1 (key=A), Row 4 (key=B), Row 7 (key=A)
Executor 2: Row 2 (key=B), Row 5 (key=A), Row 8 (key=B)
Executor 3: Row 3 (key=A), Row 6 (key=B), Row 9 (key=A)

AFTER groupBy(key): Data redistributed by key

Executor 1: All key=A rows (1, 5, 7, 3, 9)
Executor 2: All key=B rows (4, 2, 8, 6)
Executor 3: (empty or reassigned)

Data HAD to move to group by key!
```

We will explore shuffles in detail in the next document.

---

## Diagram: Data Distribution Flow

```
+------------------------------------------------------------------+
|                     DATA DISTRIBUTION FLOW                       |
|                                                                  |
|   STEP 1: Data at Rest (Distributed Storage)                    |
|   +------------------+                                            |
|   |  HDFS / S3       |                                            |
|   |  +-----+ +-----+ |                                            |
|   |  |Blk 1| |Blk 2| |  <- Data blocks on different nodes        |
|   |  +-----+ +-----+ |                                            |
|   +------------------+                                            |
|                                                                  |
|   STEP 2: Spark Reads with Locality                              |
|   +------------------+    +------------------+                    |
|   | Executor Node A  |    | Executor Node B  |                    |
|   |   +----------+   |    |   +----------+   |                    |
|   |   | Block 1  |   |    |   | Block 2  |   |                    |
|   |   | (local!) |   |    |   | (local!) |   |                    |
|   |   +----------+   |    |   +----------+   |                    |
|   +------------------+    +------------------+                    |
|                                                                  |
|   STEP 3: Process Locally                                        |
|   +------------------+    +------------------+                    |
|   | Task runs here   |    | Task runs here   |                    |
|   | No data movement |    | No data movement |                    |
|   +------------------+    +------------------+                    |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Key Takeaways

1. **Data is distributed across nodes:** Partitions live on different executors.

2. **Locality levels matter:** PROCESS_LOCAL is faster than ANY by orders of magnitude.

3. **Move compute to data:** Sending code is cheaper than sending data.

4. **Spark optimizes for locality:** Delay scheduling waits for local slots.

5. **Some operations force data movement:** Shuffles redistribute data (covered next).

6. **Network is the bottleneck:** Minimize cross-node data transfer.

---

## Additional Resources

- [Job Scheduling and Locality (Official Docs)](https://spark.apache.org/docs/latest/job-scheduling.html)
- [Data Locality in Spark (Databricks)](https://docs.databricks.com/en/optimizations/data-locality.html)
- [Understanding Spark Data Locality (Video)](https://www.youtube.com/watch?v=49Hr5xZyTEA)
