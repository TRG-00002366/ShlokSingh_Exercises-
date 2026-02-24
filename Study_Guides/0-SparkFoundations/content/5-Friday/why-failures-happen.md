# Why Failures Happen

## Learning Objectives
- Identify common failure scenarios in distributed systems
- Understand the probability and causes of node failures
- Recognize the types of failures Spark must handle
- Appreciate why fault tolerance is essential in distributed computing

## Why This Matters

In a distributed cluster, failures are not exceptional—they are expected. When you run a Spark job across 100 machines for several hours, the probability that at least one component will fail approaches certainty. Understanding failure modes helps you:
- Design robust data pipelines
- Configure appropriate retry and recovery settings
- Debug failed jobs effectively
- Appreciate Spark's fault tolerance mechanisms

---

## The Reality of Distributed Systems

### Single Machine vs Cluster Failure Probability

```
SINGLE MACHINE:
- Failure rate: ~1% per year
- Running one job: Almost certainly fine

CLUSTER WITH 100 NODES:
- Each node: ~1% failure rate per year
- Combined: 100 nodes = higher probability
- Long-running jobs: Probability compounds

If each node has 99% uptime:
- 1 node: 99% chance of success
- 10 nodes: 99%^10 = 90% chance ALL nodes stay up
- 100 nodes: 99%^100 = 37% chance ALL nodes stay up

At scale, failure is not if, but when.
```

---

### Industry Statistics

| Environment | Failure Rate |
|-------------|--------------|
| Hard drive failure | 1-3% per year |
| Server failure | 2-4% per year |
| Network switch failure | 1-2% per year |
| Data center rack failure | 1-2% per year |

**At Google scale (2006 study):**
- 1,000 machines: ~1,000 hard drive failures per year
- 1-5% of machines fail per year

---

## Types of Failures

### 1. Node Crashes

A machine becomes completely unresponsive.

```
CAUSES:
- Hardware failure (CPU, RAM, disk)
- Power outage
- Kernel panic / OS crash
- JVM crash (out of memory, etc.)

EFFECT ON SPARK:
+------------------------------------------+
|  Executor 3 on Node C                    |
|                                          |
|  Running tasks...                        |
|                                          |
|  [CRASH]                                 |
|                                          |
|  All tasks on this executor LOST         |
|  All cached data on this executor LOST   |
+------------------------------------------+
```

---

### 2. Network Partitions

Nodes cannot communicate with each other.

```
BEFORE PARTITION:
+--------+     +--------+     +--------+
| Node A |<--->| Node B |<--->| Node C |
+--------+     +--------+     +--------+

AFTER PARTITION:
+--------+     +--------+     +--------+
| Node A |  X  | Node B |<--->| Node C |
+--------+     +--------+     +--------+
    |                              ^
    |         NETWORK              |
    |         FAILURE              |
    +------------------------------+

A cannot reach B, but A can reach C, and B can reach C.
This is a partial network partition.
```

**Effect:** Executors may appear dead even though they are running.

---

### 3. Disk Failures

Storage becomes unavailable or corrupted.

```
CAUSES:
- Hard drive failure
- SSD wear-out
- File system corruption
- Disk full

EFFECT ON SPARK:
- Cannot read source data
- Cannot write shuffle files
- Cannot persist cached data to disk
- Cannot write output

+------------------------------------------+
|  EXECUTOR                                |
|                                          |
|  Task needs to read file.txt             |
|                                          |
|  Disk: [ERROR: Read failure]             |
|                                          |
|  Task FAILS                              |
+------------------------------------------+
```

---

### 4. Out-of-Memory Errors

Process runs out of heap memory.

```
CAUSES:
- Partition too large to fit in memory
- Data skew (one partition much larger than others)
- Too many cached datasets
- Memory leak in user code

EFFECT ON SPARK:
+------------------------------------------+
|  EXECUTOR - 8 GB RAM                     |
|                                          |
|  Processing partition (12 GB needed)     |
|                                          |
|  java.lang.OutOfMemoryError: Java heap   |
|                                          |
|  EXECUTOR CRASHES                        |
|  All tasks on executor LOST              |
+------------------------------------------+
```

---

### 5. Slow/Failing Tasks (Stragglers)

Some tasks take much longer than others.

```
TASK TIMING:
Task 1: [========] 10 seconds - Done
Task 2: [========] 12 seconds - Done
Task 3: [========] 11 seconds - Done
Task 4: [===================...] 2 minutes - Still running!

CAUSES:
- Data skew (one partition has 10x more data)
- Slow machine (degraded hardware)
- Garbage collection pauses
- Network congestion

EFFECT:
- Entire stage waits for slowest task
- Job takes 10x longer than expected
```

---

### 6. Driver Failures

The coordinator process dies.

```
IF DRIVER FAILS:
+------------------------------------------+
|  DRIVER                                  |
|                                          |
|  [CRASH]                                 |
|                                          |
|  All executors ORPHANED                  |
|  All job progress LOST                   |
|  All cached data LOST                    |
|  Application TERMINATES                  |
+------------------------------------------+

The Driver is a single point of failure!
```

---

## Failure Diagram

```
+------------------------------------------------------------------+
|                    FAILURE SCENARIOS                             |
|                                                                  |
|   +----------------+     +----------------+     +----------------+|
|   |    NODE        |     |   NETWORK      |     |     DISK       ||
|   |   CRASH        |     |  PARTITION     |     |    FAILURE     ||
|   +-------+--------+     +-------+--------+     +-------+--------+|
|           |                      |                      |        |
|           v                      v                      v        |
|   +----------------------------------------------------------+   |
|   |              SPARK MUST DETECT AND RECOVER               |   |
|   +----------------------------------------------------------+   |
|           ^                      ^                      ^        |
|           |                      |                      |        |
|   +-------+--------+     +-------+--------+     +-------+--------+|
|   |     OOM        |     |   STRAGGLER    |     |    DRIVER      ||
|   |    ERROR       |     |     TASK       |     |    FAILURE     ||
|   +----------------+     +----------------+     +----------------+|
|                                                                  |
+------------------------------------------------------------------+
```

---

## Why This Matters for Spark

Given that failures are inevitable, Spark must:

1. **Detect failures:** Know when an executor, task, or node has died
2. **Recover transparently:** Redo lost work without manual intervention
3. **Avoid restarting from scratch:** Only redo what was lost
4. **Handle partial failures:** Continue the job if possible

The next documents explain how Spark accomplishes this through:
- **Immutability:** Data cannot be corrupted since it is never modified
- **Lineage:** Spark remembers how to recreate any lost data
- **Checkpointing:** Periodic snapshots for very long computations

---

## Key Takeaways

1. **Failures are expected in distributed systems:** Not if, but when.

2. **Many failure types exist:** Node crashes, network partitions, disk failures, OOM, stragglers, driver failures.

3. **Each failure has different effects:** Task loss, data loss, job termination.

4. **Scale amplifies failure probability:** More nodes = higher chance of at least one failure.

5. **Spark is designed for failure:** The next documents explain how.

---

## Additional Resources

- [The Google File System Paper (discusses failure rates)](https://research.google/pubs/pub51/)
- [Spark Fault Tolerance (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-fault-tolerance)
- [Designing Data-Intensive Applications - Chapter 8](https://dataintensive.net/)
