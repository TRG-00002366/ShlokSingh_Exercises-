# Exercise: Identify Bottlenecks

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based analysis
- **Materials:** Pencil, this worksheet

## Learning Objective
Learn to identify which resource (CPU, memory, disk, network) becomes the bottleneck at different scales.

---

## Instructions

For each scenario, analyze the data pipeline diagram and answer:
1. Which resource is the bottleneck?
2. Why would that resource fail first?
3. What would be the symptom of this failure?

---

## Scenario 1: CSV Parsing Pipeline

**System Specs:**
- CPU: 8 cores, 3 GHz
- RAM: 16 GB
- Disk: 500 MB/s read
- Network: Not used (local processing)

**Pipeline:**

```
+------------+     +-------------+     +------------+     +---------+
| Read CSV   | --> | Parse Dates | --> | Filter     | --> | Output  |
| from disk  |     | (CPU heavy) |     | (light)    |     | to disk |
+------------+     +-------------+     +------------+     +---------+

Data: 50 GB CSV file
Each row requires date parsing (0.1ms CPU per row)
Rows: 500 million
```

**Question 1.1:** Calculate the time for each stage:

| Stage | Calculation | Time |
|-------|-------------|------|
| Read CSV | 50 GB / 500 MB/s | _____ seconds |
| Parse Dates | 500M rows * 0.1ms | _____ seconds |
| Filter | ~10% of parse time | _____ seconds |
| Write | 5 GB / 500 MB/s | _____ seconds |

**Question 1.2:** Which resource is the bottleneck?

```
[ ] CPU
[ ] Memory
[ ] Disk
[ ] Network
```

**Question 1.3:** Explain your answer:

```
_________________________________________________
_________________________________________________
```

---

## Scenario 2: In-Memory Aggregation

**System Specs:**
- CPU: 16 cores, 3 GHz
- RAM: 32 GB
- Disk: 1 GB/s SSD
- Network: Not used (local processing)

**Pipeline:**

```
+------------+     +----------------+     +---------+
| Read       | --> | Group by key   | --> | Output  |
| Parquet    |     | (needs all     |     | results |
| (columnar) |     |  data in RAM)  |     |         |
+------------+     +----------------+     +---------+

Data: 100 GB Parquet file
Aggregation requires entire dataset in memory
```

**Question 2.1:** What is the problem here?

```
_________________________________________________
```

**Question 2.2:** Which resource is the bottleneck?

```
[ ] CPU
[ ] Memory
[ ] Disk
[ ] Network
```

**Question 2.3:** What error message would you likely see?

```
_________________________________________________
```

---

## Scenario 3: Distributed Processing

**System Specs (per node, 4 nodes total):**
- CPU: 4 cores
- RAM: 8 GB
- Disk: 200 MB/s
- Network between nodes: 1 Gbps (125 MB/s)

**Pipeline:**

```
Node 1   Node 2   Node 3   Node 4
  |        |        |        |
  v        v        v        v
[Read]   [Read]   [Read]   [Read]   <- Each reads 25 GB locally
  |        |        |        |
  +--------+--------+--------+
           |
           v
      [SHUFFLE]                      <- All data must cross network
           |
           v
      [Aggregate]
```

Data: 100 GB total (25 GB per node)
Shuffle moves 50 GB across network

**Question 3.1:** Calculate time for each phase:

| Phase | Calculation | Time |
|-------|-------------|------|
| Read (parallel) | 25 GB / 200 MB/s | _____ seconds |
| Shuffle | 50 GB / 125 MB/s | _____ seconds |

**Question 3.2:** Which resource is the bottleneck?

```
[ ] CPU
[ ] Memory
[ ] Disk
[ ] Network
```

**Question 3.3:** How could you reduce the shuffle bottleneck?

```
_________________________________________________
_________________________________________________
```

---

## Scenario 4: The Mystery Slowdown

A job that used to run in 10 minutes now takes 2 hours. Analyze the metrics:

**Before (fast):**
- CPU utilization: 80%
- Memory usage: 12 GB / 32 GB
- Disk I/O: 400 MB/s
- Data size: 10 GB

**After (slow):**
- CPU utilization: 15%
- Memory usage: 31 GB / 32 GB
- Disk I/O: 50 MB/s (with lots of random access)
- Data size: 50 GB

**Question 4.1:** What resource became the bottleneck?

```
[ ] CPU
[ ] Memory
[ ] Disk
[ ] Network
```

**Question 4.2:** Explain what is happening:

```
_________________________________________________
_________________________________________________
```

**Question 4.3:** What is the solution?

```
_________________________________________________
```

---

## Summary Table

Fill in which resource most often bottlenecks each operation type:

| Operation Type | Typical Bottleneck |
|----------------|-------------------|
| Reading large files | _____________ |
| Complex calculations per row | _____________ |
| Sorting/grouping large datasets | _____________ |
| Shuffling between nodes | _____________ |
| Writing output files | _____________ |

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Scenario 1:**
- Read: 100 seconds
- Parse: 50,000 seconds (13.9 hours!)
- Filter: ~5,000 seconds
- Write: 10 seconds
- Bottleneck: CPU (parsing is extremely slow)

**Scenario 2:**
- Problem: 100 GB data cannot fit in 32 GB RAM
- Bottleneck: Memory
- Error: java.lang.OutOfMemoryError

**Scenario 3:**
- Read: 125 seconds
- Shuffle: 400 seconds
- Bottleneck: Network
- Solution: Pre-partition data, use reduceByKey instead of groupByKey

**Scenario 4:**
- Bottleneck: Memory (causing disk spill)
- Explanation: Data grew 5x but memory stayed same. Data is spilling to disk (random I/O is slow). CPU is idle waiting for I/O.
- Solution: Add more memory or use more partitions

**Summary Table:**
- Reading large files: Disk
- Complex calculations: CPU
- Sorting/grouping: Memory (may spill to disk)
- Shuffling: Network
- Writing output: Disk

</details>
