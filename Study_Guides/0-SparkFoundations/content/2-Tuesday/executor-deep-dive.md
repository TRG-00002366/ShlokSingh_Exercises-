# Executor Deep Dive

## Learning Objectives
- Understand the Executor as the workhorse of Spark computation
- Explain how Executors manage memory, cores, and tasks
- Visualize the Executor memory layout
- Identify common Executor-related issues and their causes

## Why This Matters

Executors are where the actual data processing happens. When you read about Spark performance, memory errors, or task failures, Executors are usually involved. Understanding Executors helps you:
- Configure Spark for optimal performance
- Debug out-of-memory errors
- Understand why some operations are slow

Think of Executors as the Line Cooks from our restaurant analogy: they have their own workstation (memory) and tools (CPU cores), and they execute the recipe (tasks) given to them by the Head Chef (Driver).

---

## What is an Executor?

An **Executor** is a distributed agent responsible for executing tasks. Each Executor:

- Runs as a separate **JVM process** on a Worker Node
- Has its own **memory allocation**
- Has its own **CPU cores**
- Runs for the **lifetime of the application**
- Can run **multiple tasks in parallel**

```
+----------------------------------------------------------+
|                       EXECUTOR                           |
|                                                          |
|   JVM Process on Worker Node                             |
|   +--------------------------------------------------+   |
|   |                                                  |   |
|   |   CPU CORES: [Core 1] [Core 2] [Core 3] [Core 4] |   |
|   |              Each core runs one task             |   |
|   |                                                  |   |
|   |   MEMORY: 16 GB                                  |   |
|   |   +------------------------------------------+   |   |
|   |   |  Execution Memory | Storage Memory      |   |   |
|   |   |  (Running tasks)  | (Cached data)       |   |   |
|   |   +------------------------------------------+   |   |
|   |                                                  |   |
|   +--------------------------------------------------+   |
|                                                          |
+----------------------------------------------------------+
```

---

## Executor Resources

Each Executor is configured with specific resources:

### CPU Cores

Cores determine how many tasks can run **simultaneously** on an Executor.

```
Executor with 4 cores:

   [Core 1]     [Core 2]     [Core 3]     [Core 4]
      |            |            |            |
      v            v            v            v
   Task 1       Task 2       Task 3       Task 4
   (running)    (running)    (running)    (running)

4 tasks running in parallel!
```

Key points:
- More cores = more parallelism within each Executor
- Each task processes one **partition** of data
- If you have 100 partitions and 4 cores, tasks queue up

---

### Memory

Memory is divided into regions for different purposes:

```
+----------------------------------------------------------+
|                   EXECUTOR MEMORY (16 GB)                |
|                                                          |
|  +------------------------+  +------------------------+  |
|  |                        |  |                        |  |
|  |   EXECUTION MEMORY     |  |    STORAGE MEMORY      |  |
|  |        (8 GB)          |  |        (8 GB)          |  |
|  |                        |  |                        |  |
|  |  - Running task data   |  |  - Cached RDDs         |  |
|  |  - Shuffle buffers     |  |  - Cached DataFrames   |  |
|  |  - Join/sort buffers   |  |  - Broadcast variables |  |
|  |                        |  |                        |  |
|  +------------------------+  +------------------------+  |
|                                                          |
|  +------------------------------------------------------+|
|  |              RESERVED MEMORY (300 MB)                ||
|  |  - Spark internal overhead                           ||
|  +------------------------------------------------------+|
|                                                          |
+----------------------------------------------------------+
```

---

## Executor Memory Model in Detail

Understanding memory allocation is crucial for tuning Spark:

### Unified Memory Manager

Spark uses a **unified memory manager** that allows flexible sharing between execution and storage:

```
UNIFIED MEMORY POOL (Total - Reserved)
+----------------------------------------------------------+
|                                                          |
|   [EXECUTION]<========================================>  |
|       |                    BOUNDARY                 |    |
|       |           (Can shift dynamically)           |    |
|   <===+=============================================>[STORAGE]
|                                                          |
+----------------------------------------------------------+

When execution needs more memory:
- It can borrow from storage (evicting cached data)

When storage needs more memory:
- It can borrow from execution (if execution is not using it)
```

### Memory Fractions

| Memory Region | Default Fraction | Purpose |
|---------------|------------------|---------|
| Execution | 50% of usable | Shuffles, joins, sorts, aggregations |
| Storage | 50% of usable | Caching RDDs/DataFrames |
| Reserved | 300 MB fixed | Spark internal overhead |

---

## What Executors Do

### 1. Execute Tasks

The primary job: run the tasks assigned by the Driver.

```
Driver sends Task:
"Apply filter(age > 21) to partition 5"

Executor receives:
+----------------+
| Task           |
| - Partition: 5 |
| - Operation:   |
|   filter(>21)  |
+----------------+

Executor executes:
1. Read partition 5 from storage
2. Apply filter
3. Return result to Driver
```

---

### 2. Cache Data

Executors store cached data in Storage Memory:

```
User requests: df.cache()

Executor stores:
+--------------------------------------------------+
|                 STORAGE MEMORY                   |
|                                                  |
|  Partition 1: [.......data.......]               |
|  Partition 2: [.......data.......]               |
|  Partition 3: [.......data.......]               |
|                                                  |
|  Cached for reuse - no re-reading from disk!     |
+--------------------------------------------------+
```

Benefits of caching:
- Avoid re-reading data from disk
- Avoid re-computing expensive transformations
- Data stays in memory for fast access

---

### 3. Shuffle Data

During shuffles, Executors exchange data with each other:

```
SHUFFLE WRITE (Executor 1)          SHUFFLE READ (Executor 3)
+------------------------+          +------------------------+
| Data for Executor 2    | -------> | Data from Executor 1   |
| Data for Executor 3    | -------> | Data from Executor 2   |
| Data for Executor 4    | -------> | Data from Executor 4   |
+------------------------+          +------------------------+

Executors communicate directly during shuffles!
```

---

### 4. Report Status

Executors continuously communicate with the Driver:

```
EXECUTOR                                DRIVER
   |                                      |
   | --- Heartbeat (every 10 seconds) --> |
   |                                      |
   | --- Task Complete: Success --------> |
   |                                      |
   | --- Metrics Update: Memory usage --> |
   |                                      |
   | <-- New Task Assignment ------------ |
   |                                      |
```

If heartbeats stop, the Driver assumes the Executor is dead.

---

## Common Executor Issues

### 1. Out of Memory Error

**Symptom:** `java.lang.OutOfMemoryError: Java heap space`

**Cause:** Task data exceeds available memory.

```
EXECUTOR MEMORY: 8 GB

Task trying to:
- Load partition: 4 GB
- Sort data: 6 GB
- Total needed: 10 GB > 8 GB available

Result: OOM Error
```

**Solutions:**
- Increase executor memory
- Increase number of partitions (smaller data per task)
- Reduce data size before expensive operations

---

### 2. Executor Lost

**Symptom:** "Executor lost" or "Executor removed" in logs

**Causes:**
- Executor ran out of memory and crashed
- Network issues disconnected the Executor
- Worker Node failed

**Result:** Tasks are rescheduled on other Executors.

---

### 3. Straggler Tasks

**Symptom:** One task takes much longer than others.

```
Task times:
Task 1: 10 seconds - Done
Task 2: 12 seconds - Done
Task 3: 11 seconds - Done
Task 4: 5 MINUTES  - Still running (straggler!)
```

**Causes:**
- Data skew (one partition has much more data)
- Reading from a slow disk or node
- Garbage collection pauses

---

## Executor Lifecycle

```
+------------------------------------------------------------------+
|                    EXECUTOR LIFECYCLE                            |
|                                                                  |
|  1. APPLICATION START                                            |
|     Driver requests executors from Cluster Manager               |
|                                                                  |
|  2. EXECUTOR LAUNCH                                              |
|     Cluster Manager starts executor processes on Workers         |
|                                                                  |
|  3. EXECUTOR REGISTERS                                           |
|     Executors connect to Driver, report resources                |
|                                                                  |
|  4. TASK EXECUTION                                               |
|     Executors receive and run tasks (this is most of the time)   |
|                                                                  |
|  5. APPLICATION END                                              |
|     Driver signals completion, executors shut down               |
|                                                                  |
+------------------------------------------------------------------+
```

Key point: Executors live for the **entire application duration**, not per-job.

---

## Executor Configuration Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `spark.executor.memory` | Memory per executor | 8g |
| `spark.executor.cores` | Cores per executor | 4 |
| `spark.executor.instances` | Number of executors | 10 |
| `spark.executor.memoryOverhead` | Off-heap memory buffer | 1g |

---

## Key Takeaways

1. **Executors are the workers:** They execute tasks and store cached data.

2. **Each Executor is a JVM:** Runs on a Worker Node with dedicated memory and cores.

3. **Memory is shared:** Between execution (tasks) and storage (caching).

4. **More cores = more parallel tasks:** Within each Executor.

5. **Executors live for the application:** They start when your app starts and stop when it ends.

6. **Common issues:** Out of memory, executor lost, straggler tasks.

---

## Additional Resources

- [Spark Memory Management (Official Docs)](https://spark.apache.org/docs/latest/tuning.html#memory-management-overview)
- [Understanding Executor Memory (Databricks)](https://docs.databricks.com/en/spark/architecture.html)
- [Apache Spark Executor Deep Dive (Video)](https://www.youtube.com/watch?v=49Hr5xZyTEA)
