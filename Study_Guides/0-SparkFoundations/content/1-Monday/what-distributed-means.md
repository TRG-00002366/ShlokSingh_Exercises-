# What "Distributed" Actually Means

## Learning Objectives
- Define distributed computing in concrete terms
- Distinguish between data parallelism and task parallelism
- Identify the coordination challenges unique to distributed systems
- Understand why network becomes the new bottleneck

## Why This Matters

The term "distributed computing" appears constantly in data engineering job descriptions and technical discussions. But what does it actually mean? Before working with Apache Spark, you need a precise understanding of what happens when computation is spread across multiple machines. This mental model will help you understand why Spark is designed the way it is.

---

## Defining Distributed Computing

**Distributed computing** means using multiple interconnected computers to solve a problem together.

More precisely:
- **Multiple computers** (nodes) work simultaneously
- **Interconnected** via network communication
- **Solve a problem together** that would be too large or slow for one machine

```
+--------+       +--------+       +--------+
| Node 1 |<----->| Node 2 |<----->| Node 3 |
+--------+       +--------+       +--------+
     \               |               /
      \              |              /
       +-------------+-------------+
                     |
              NETWORK FABRIC
              (Communication)
```

Each node is a complete computer with its own CPU, memory, and disk. The magic—and complexity—lies in making them work as one.

---

## Two Types of Parallelism

Distributed systems achieve performance through parallelism, but there are two distinct types:

### Data Parallelism

**Data parallelism** means performing the same operation on different pieces of data simultaneously.

```
           SAME OPERATION
               |
       +-------+-------+
       |       |       |
       v       v       v
   +------+ +------+ +------+
   |Data A| |Data B| |Data C|
   |  +2  | |  +2  | |  +2  |
   +------+ +------+ +------+
   | Node1| | Node2| | Node3|
   +------+ +------+ +------+
```

**Example:** Adding 2 to every number in a dataset of 1 billion numbers.
- Node 1 processes numbers 1-333 million
- Node 2 processes numbers 334-666 million
- Node 3 processes numbers 667 million-1 billion

Apache Spark primarily uses **data parallelism**. Your dataset is split into partitions, and each executor applies the same transformations to its partition.

---

### Task Parallelism

**Task parallelism** means performing different operations simultaneously.

```
   DIFFERENT OPERATIONS
       |       |       |
       v       v       v
   +------+ +------+ +------+
   |Task A| |Task B| |Task C|
   | Sort | |Filter| | Join |
   +------+ +------+ +------+
   | Node1| | Node2| | Node3|
   +------+ +------+ +------+
```

**Example:** Three different analysis tasks running on different parts of the cluster:
- Node 1 sorts customer data
- Node 2 filters transactions
- Node 3 joins product catalogs

Spark uses task parallelism when your DAG (Directed Acyclic Graph) contains independent operations that can run simultaneously.

---

## Coordination Challenges

When multiple machines work together, new problems emerge that do not exist on a single machine:

### 1. Data Consistency

How do you ensure all nodes see the same data?

```
Scenario: Two nodes updating the same record

Node 1: account.balance = 100
Node 2: account.balance = 100

Node 1: account.balance += 50  --> balance = 150
Node 2: account.balance -= 30  --> balance = 70

What is the final balance?
- If Node 1's update wins: 150
- If Node 2's update wins: 70
- Correct answer should be: 120 (100 + 50 - 30)
```

Distributed systems must carefully coordinate updates to maintain consistency.

---

### 2. Partial Failure

What happens when some nodes fail but others continue?

```
Processing in progress...

+------+     +------+     +------+
|Node 1|     |Node 2|     |Node 3|
|  OK  |     | FAIL |     |  OK  |
+------+     +------+     +------+

Options:
1. Fail the entire job (safe but wasteful)
2. Retry only the failed work (efficient but complex)
3. Ignore the failure (fast but incorrect)
```

Spark chooses option 2: it tracks what each node was doing and can reassign failed work to healthy nodes.

---

### 3. Communication Overhead

Nodes must exchange data and synchronize, which takes time.

```
+------+     Network     +------+
|Node 1| -------------->  |Node 2|
+------+   1 ms latency   +------+

Within a single machine:
- Memory access: 100 nanoseconds
- Network access: 1,000,000 nanoseconds (1 ms)

Network is 10,000x slower than memory!
```

This is why distributed systems try to minimize data movement between nodes.

---

### 4. Synchronization Points

Sometimes all nodes must wait for each other.

```
Phase 1: All nodes process half of the data (parallel)
         Node 1: Done
         Node 2: Done
         Node 3: Still working... (slowest)
         
         Waiting... Waiting... Waiting...
         
         Node 3: Done!
         
Phase 2: Aggregate results (all nodes must have finished Phase 1)
```

The slowest node determines the overall speed. This is called the "straggler problem."

---

## Network: The New Bottleneck

In single-machine processing, disk I/O is often the bottleneck. In distributed systems, **network** becomes the critical constraint.

### Network Speed Comparison

| Transfer Type | Speed |
|---------------|-------|
| Memory to CPU | 100 GB/s |
| SSD to Memory | 3-7 GB/s |
| Network (10 Gbps) | 1.25 GB/s |
| Network (typical cloud) | 0.5-1 GB/s |

### What This Means for Design

Distributed systems are designed around one principle: **minimize data movement**.

```
GOOD: Move computation to data
+--------+     +--------+
| Node 1 |     | Node 2 |
| [DATA] |     | [DATA] |
| [CODE] |     | [CODE] |
+--------+     +--------+
    Process locally

BAD: Move data to computation
+--------+            +--------+
| Node 1 | =========> | Node 2 |
| [DATA] |   Slow!    | [CODE] |
+--------+            +--------+
    Network transfer
```

Spark sends your code to where the data lives, not the other way around. This is called "data locality."

---

## Diagram: Distributed System Overview

```
+------------------------------------------------------------------+
|                     DISTRIBUTED CLUSTER                          |
|                                                                  |
|   +----------+      +----------+      +----------+               |
|   | Worker 1 |      | Worker 2 |      | Worker 3 |               |
|   |          |      |          |      |          |               |
|   | CPU  RAM |      | CPU  RAM |      | CPU  RAM |               |
|   | Disk     |      | Disk     |      | Disk     |               |
|   +----+-----+      +----+-----+      +----+-----+               |
|        |                |                  |                     |
|        +----------------+------------------+                     |
|                         |                                        |
|                   NETWORK SWITCH                                 |
|                         |                                        |
|        +----------------+------------------+                     |
|        |                |                  |                     |
|   +----+-----+     +----+-----+      +-----+----+                |
|   |Coordinator|     | Storage |      | Storage  |                |
|   | (Master) |     | Node 1  |      | Node 2   |                |
|   +----------+     +----------+      +----------+                |
|                                                                  |
+------------------------------------------------------------------+

- Workers: Execute computation (this is where Spark Executors run)
- Coordinator: Manages job distribution (this is where Spark Driver runs)
- Storage: May be distributed (like HDFS) or centralized (like S3)
- Network: Connects everything (and is the bottleneck)
```

---

## Key Takeaways

1. **Distributed computing** uses multiple networked computers to solve problems too large for one machine.

2. **Data parallelism** (same operation, different data) is the primary model Spark uses.

3. **Coordination is hard:** Distributed systems must handle consistency, partial failure, communication overhead, and synchronization.

4. **Network is the bottleneck:** Moving data between machines is slow. Good distributed systems minimize data movement.

5. **"Move compute to data"** is the guiding principle: Send your code to where the data lives, not the reverse.

6. **Spark handles this complexity:** Understanding these challenges helps you appreciate what Spark does behind the scenes—and why certain operations (like shuffles) are expensive.

---

## Additional Resources

- [Fallacies of Distributed Computing](https://en.wikipedia.org/wiki/Fallacies_of_distributed_computing)
- [CAP Theorem Explained](https://www.ibm.com/topics/cap-theorem)
- [Designing Data-Intensive Applications (Book)](https://dataintensive.net/)
