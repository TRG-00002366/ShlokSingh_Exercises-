# Spark Cluster Components

## Learning Objectives
- Identify the four main components of a Spark cluster
- Explain how Driver, Cluster Manager, Worker Nodes, and Executors interact
- Trace the lifecycle of a Spark application through these components
- Visualize the architecture using diagrams

## Why This Matters

When your Spark application runs, multiple components work together behind the scenes. If something goes wrong—a job is slow, a task fails, or memory runs out—you need to know which component is responsible. This document provides the complete mental map of Spark's architecture that you will build upon throughout the week.

Remember the restaurant kitchen analogy from Monday: the Head Chef (Driver), Sous Chef (Cluster Manager), and Line Cooks (Executors). Today, we explore what each actually does in Spark.

---

## The Four Main Components

```
+------------------------------------------------------------------+
|                        SPARK CLUSTER                             |
|                                                                  |
|  +------------------+                                             |
|  |     DRIVER       |  Your program runs here                    |
|  |  (SparkContext)  |  Creates the execution plan                |
|  +--------+---------+                                             |
|           |                                                       |
|           | Requests Resources                                    |
|           v                                                       |
|  +------------------+                                             |
|  | CLUSTER MANAGER  |  YARN, Mesos, Kubernetes, or Standalone    |
|  |                  |  Allocates executors across the cluster    |
|  +--------+---------+                                             |
|           |                                                       |
|           | Launches Executors on                                 |
|           v                                                       |
|  +------------------+------------------+------------------+        |
|  |   WORKER NODE    |   WORKER NODE    |   WORKER NODE    |       |
|  |  +------------+  |  +------------+  |  +------------+  |       |
|  |  |  EXECUTOR  |  |  |  EXECUTOR  |  |  |  EXECUTOR  |  |       |
|  |  |            |  |  |            |  |  |            |  |       |
|  |  |  [Tasks]   |  |  |  [Tasks]   |  |  |  [Tasks]   |  |       |
|  |  +------------+  |  +------------+  |  +------------+  |       |
|  +------------------+------------------+------------------+        |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Component 1: The Driver

The **Driver** is the process that runs your main program. It is the "brain" of your Spark application.

### What the Driver Does

1. **Creates SparkContext/SparkSession:** The entry point for your application
2. **Analyzes your code:** Converts transformations into a logical plan
3. **Creates the DAG:** Builds the Directed Acyclic Graph of operations
4. **Divides work into stages and tasks:** Determines what each executor will do
5. **Schedules tasks:** Sends work to executors
6. **Collects results:** Receives completed task results

### Driver Diagram

```
+------------------------------------------+
|                 DRIVER                   |
|                                          |
|  +--------------+    +--------------+    |
|  | Your Code    |    | SparkContext |    |
|  | main()       |--->| /Session     |    |
|  +--------------+    +------+-------+    |
|                             |            |
|                    +--------v--------+   |
|                    | DAG Scheduler   |   |
|                    | (Plans stages)  |   |
|                    +--------+--------+   |
|                             |            |
|                    +--------v--------+   |
|                    | Task Scheduler  |   |
|                    | (Assigns tasks) |   |
|                    +-----------------+   |
|                                          |
+------------------------------------------+
```

### Key Points About the Driver

- There is exactly **one Driver per application**
- The Driver runs on a machine with network access to all workers
- If the Driver fails, the entire application fails
- The Driver stores the final results of actions like `.collect()`

---

## Component 2: The Cluster Manager

The **Cluster Manager** is responsible for allocating resources across the cluster. It acts as the intermediary between your application and the physical machines.

### Cluster Manager Types

| Type | Description |
|------|-------------|
| **Standalone** | Spark's built-in cluster manager. Simple to set up. |
| **YARN** | Hadoop's resource manager. Common in Hadoop environments. |
| **Mesos** | Apache Mesos. General-purpose cluster manager. |
| **Kubernetes** | Container orchestration. Modern cloud-native option. |

### What the Cluster Manager Does

1. **Receives resource requests:** Driver asks for X executors with Y memory each
2. **Allocates resources:** Finds available machines with sufficient resources
3. **Launches executors:** Starts executor processes on worker nodes
4. **Monitors health:** Tracks which executors are alive
5. **Handles failures:** Restarts executors if they crash

### Cluster Manager Diagram

```
+------------------------------------------+
|           CLUSTER MANAGER                |
|                                          |
|    Driver Request:                       |
|    "I need 4 executors,                  |
|     each with 4 GB memory,               |
|     2 CPU cores"                         |
|                                          |
|              |                           |
|              v                           |
|    +-------------------+                 |
|    | Resource Tracker  |                 |
|    +-------------------+                 |
|              |                           |
|              v                           |
|    Available Workers:                    |
|    Worker 1: 16 GB, 8 cores (assign 1)   |
|    Worker 2: 16 GB, 8 cores (assign 1)   |
|    Worker 3: 16 GB, 8 cores (assign 2)   |
|                                          |
+------------------------------------------+
```

### Key Points About Cluster Managers

- The Cluster Manager is **external to Spark** (except Standalone mode)
- Your choice of Cluster Manager affects deployment, not application logic
- All Cluster Managers provide the same interface to Spark

---

## Component 3: Worker Nodes

**Worker Nodes** are the physical or virtual machines in your cluster that run executor processes.

### What Worker Nodes Provide

1. **CPU cores:** For parallel task execution
2. **Memory (RAM):** For caching data and running tasks
3. **Disk storage:** For shuffle files and spill-over
4. **Network connectivity:** For communicating with Driver and other workers

### Worker Node Diagram

```
+------------------------------------------+
|              WORKER NODE                 |
|                                          |
|    Physical Resources:                   |
|    - 16 CPU cores                        |
|    - 64 GB RAM                           |
|    - 1 TB SSD                            |
|    - 10 Gbps network                     |
|                                          |
|    +----------------+  +----------------+ |
|    |   EXECUTOR 1   |  |   EXECUTOR 2   | |
|    |                |  |                | |
|    |  4 cores       |  |  4 cores       | |
|    |  16 GB RAM     |  |  16 GB RAM     | |
|    +----------------+  +----------------+ |
|                                          |
|    Remaining: 8 cores, 32 GB RAM         |
|    (Available for more executors)        |
|                                          |
+------------------------------------------+
```

### Key Points About Worker Nodes

- A worker can run **multiple executors**
- Workers are managed by the Cluster Manager
- More workers = more parallelism (horizontal scaling)

---

## Component 4: Executors

**Executors** are the processes that actually run your code on worker nodes. They are where computation happens.

### What Executors Do

1. **Execute tasks:** Run the actual transformations on data
2. **Store data:** Cache RDDs and DataFrames in memory
3. **Return results:** Send task completion status and results to Driver
4. **Report status:** Heartbeat to Driver to confirm they are alive

### Executor Diagram

```
+------------------------------------------+
|                EXECUTOR                  |
|                                          |
|   Memory Layout:                         |
|   +----------------------------------+   |
|   |         Execution Memory          |   |
|   |   (Running tasks, shuffles)       |   |
|   +----------------------------------+   |
|   +----------------------------------+   |
|   |         Storage Memory            |   |
|   |   (Cached RDDs, DataFrames)       |   |
|   +----------------------------------+   |
|   +----------------------------------+   |
|   |         Reserved Memory           |   |
|   |   (Spark internal overhead)       |   |
|   +----------------------------------+   |
|                                          |
|   CPU Cores: 4                           |
|   (Can run 4 tasks in parallel)          |
|                                          |
+------------------------------------------+
```

### Key Points About Executors

- Executors are **launched at application start** and run for the entire duration
- Each executor is a **separate JVM process**
- More executor cores = more parallelism within each executor
- Executors do not share memory with each other

---

## How Components Interact

Here is the complete flow of a Spark application:

```
Step 1: Submit Application
+--------+                     +------------------+
| spark- |  -- submits to -->  | CLUSTER MANAGER |
| submit |                     +------------------+
+--------+

Step 2: Allocate Resources
+------------------+          +--------------+
| CLUSTER MANAGER  | -------> | WORKER NODES |
+------------------+  launch  +--------------+
                      executors

Step 3: Run Application
+---------+                   +------------+
| DRIVER  | <-- heartbeats -- | EXECUTORS  |
|         | -- tasks -------> |            |
|         | <-- results ----  |            |
+---------+                   +------------+

Step 4: Return Results
+---------+          +--------+
| DRIVER  | -------> | USER   |
+---------+  output  +--------+
```

### Detailed Lifecycle

1. **You run `spark-submit`:** Starts the Driver process
2. **Driver contacts Cluster Manager:** "I need X executors with Y resources"
3. **Cluster Manager allocates executors:** Launches them on Worker Nodes
4. **Driver builds execution plan:** Analyzes your code, creates DAG
5. **Driver schedules tasks:** Sends tasks to Executors
6. **Executors run tasks:** Process data partitions
7. **Executors return results:** Send results/status back to Driver
8. **Driver aggregates results:** Combines results, returns to user
9. **Application completes:** Executors shut down, resources released

---

## Communication Patterns

```
+------------------------------------------+
|                                          |
|   DRIVER <-------+--------> EXECUTOR 1   |
|      |           |              ^        |
|      |           |              |        |
|      |           |              v        |
|      +-----------|--------> EXECUTOR 2   |
|      |           |              ^        |
|      |           |              |        |
|      |           |              v        |
|      +-----------|--------> EXECUTOR 3   |
|                  |                       |
|                  |                       |
|          +-------v-------+               |
|          | CLUSTER       |               |
|          | MANAGER       |               |
|          +---------------+               |
|                                          |
+------------------------------------------+

Driver <-> Executors: Task assignment, result collection
Driver <-> Cluster Manager: Resource requests
Executor <-> Executor: Shuffle data (direct communication)
```

---

## Summary Table

| Component | Count | Location | Responsibility |
|-----------|-------|----------|----------------|
| Driver | 1 | Client or cluster | Coordination, planning, results |
| Cluster Manager | 1 | Dedicated node(s) | Resource allocation |
| Worker Node | Many | Cluster machines | Hosting executors |
| Executor | Many | Worker nodes | Task execution, caching |

---

## Key Takeaways

1. **Four components work together:** Driver, Cluster Manager, Worker Nodes, and Executors.

2. **Driver is the brain:** It plans the work but does not do the computation.

3. **Executors are the muscles:** They do the actual data processing.

4. **Cluster Manager is the allocator:** It decides where executors run.

5. **Worker Nodes are the hardware:** Physical machines that host executors.

6. **One Driver, many Executors:** This is the fundamental architecture pattern.

---

## Additional Resources

- [Spark Cluster Overview (Official Docs)](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Spark Architecture Video (Databricks)](https://www.youtube.com/watch?v=dmL0N3qfSc8)
- [Understanding Spark Cluster Mode (Spark Summit)](https://www.youtube.com/watch?v=7ooZ4S7Ay6Y)
