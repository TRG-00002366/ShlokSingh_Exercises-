# What is a Partition?

## Learning Objectives
- Define partitions as Spark's fundamental unit of parallelism
- Understand how data is divided into partitions
- Explain how partition count affects performance
- Identify default partitioning strategies

## Why This Matters

Partitions are the key to understanding Spark's parallelism. Every operation in Spark happens at the partition level—one task processes one partition. If you do not understand partitions, you cannot understand why some Spark jobs are fast and others are slow, or why the same code behaves differently with different data sizes.

In our postal facility analogy, partitions are like the bins of mail that each sorting table receives. The number of bins determines how many workers can sort simultaneously.

---

## What is a Partition?

A **partition** is a logical chunk of data that can be processed independently.

```
+----------------------------------------------------------+
|                    YOUR DATASET                          |
|                    (1 Million Rows)                      |
+----------------------------------------------------------+
                            |
              SPLIT INTO PARTITIONS
                            |
        +-------------------+-------------------+
        |                   |                   |
        v                   v                   v
+----------------+  +----------------+  +----------------+
|  Partition 1   |  |  Partition 2   |  |  Partition 3   |
|  Rows 1-333K   |  |  Rows 334K-666K|  |  Rows 667K-1M  |
+----------------+  +----------------+  +----------------+
        |                   |                   |
        v                   v                   v
    [Task 1]            [Task 2]            [Task 3]
    
    Each partition is processed by exactly one task
```

Key characteristics:
- **Logical division:** Partitions are how Spark thinks about data chunks
- **Parallel processing:** Multiple partitions = multiple tasks can run simultaneously
- **Independent:** Each partition can be processed without knowing about others (for narrow operations)

---

## Why Partitions Matter

### More Partitions = More Parallelism

```
4 Partitions, 4 Executor Cores:

    Time 0          Time 1
    ------          ------
    [P1] [P2] [P3] [P4]    -> All 4 partitions processed together
    
    Total: 1 time unit

---

2 Partitions, 4 Executor Cores:

    Time 0          Time 1
    ------          ------
    [P1] [P2] [--] [--]    -> Only 2 cores used, 2 idle
    
    Total: 1 time unit (but 50% resources wasted!)

---

8 Partitions, 4 Executor Cores:

    Time 0          Time 1
    ------          ------
    [P1] [P2] [P3] [P4]    -> First 4 partitions
    [P5] [P6] [P7] [P8]    -> Next 4 partitions
    
    Total: 2 time units
```

---

### The Goldilocks Problem

Too few partitions = underutilized resources
Too many partitions = excessive overhead

```
TOO FEW PARTITIONS                    TOO MANY PARTITIONS

  2 partitions, 100 cores               10,000 partitions, 100 cores
  
  [P1] [P2] [--] [--] [--]...           [P1][P2][P3]...[P10000]
  
  98 cores sitting idle!                Scheduling overhead dominates
  Large partitions may cause OOM        Each partition very small
```

**Rule of thumb:** 2-4 partitions per CPU core in your cluster.

---

## How Data is Partitioned

### When Reading Data

Spark automatically creates partitions when reading data:

```
READING FROM FILES:

File System:
  data/
    part-00000.csv  (100 MB)
    part-00001.csv  (100 MB)
    part-00002.csv  (100 MB)
    
Spark creates:
  Partition 0 <- part-00000.csv (or portion of it)
  Partition 1 <- part-00001.csv (or portion of it)
  Partition 2 <- part-00002.csv (or portion of it)
  
Default: ~128 MB per partition (configurable)
```

---

### When Creating RDDs from Collections

```
# Creating an RDD from a Python list
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Spark decides partition count based on cluster size
rdd = sc.parallelize(data)

# Or you can specify explicitly
rdd = sc.parallelize(data, numSlices=4)

# Result:
# Partition 0: [1, 2, 3]
# Partition 1: [4, 5]
# Partition 2: [6, 7, 8]
# Partition 3: [9, 10]
```

---

## Partition Characteristics

### Partitions are Immutable

When you transform data, Spark creates new partitions:

```
ORIGINAL         TRANSFORMED
+--------+       +--------+
| Part 0 | --->  | Part 0'|  (new partition after filter)
+--------+       +--------+
| Part 1 | --->  | Part 1'|  (new partition after filter)
+--------+       +--------+
| Part 2 | --->  | Part 2'|  (new partition after filter)
+--------+       +--------+

Original partitions are never modified!
```

---

### Partitions Have Size

Every partition holds some amount of data:

```
IDEAL: Even partition sizes

Partition 0: [======] 100 MB
Partition 1: [======] 100 MB
Partition 2: [======] 100 MB
Partition 3: [======] 100 MB

All tasks take similar time.

---

BAD: Uneven partition sizes (DATA SKEW)

Partition 0: [======] 100 MB
Partition 1: [==] 20 MB
Partition 2: [==========================] 500 MB  <- SKEW!
Partition 3: [=] 10 MB

Partition 2 takes 5x longer than others!
```

Data skew is a common performance problem. We will discuss this more on Thursday.

---

## Default Partitioning Strategies

### Hash Partitioning

Data is distributed based on the hash of a key:

```
Key: "apple"  -> hash("apple") % 4 = 2 -> Partition 2
Key: "banana" -> hash("banana") % 4 = 0 -> Partition 0
Key: "cherry" -> hash("cherry") % 4 = 1 -> Partition 1

All records with the same key go to the same partition.
This is crucial for operations like groupByKey and join.
```

---

### Range Partitioning

Data is distributed based on sorted order:

```
Keys: [1, 5, 10, 15, 20, 25, 30, 35]

Range partition into 4:
  Partition 0: keys 1-10   [1, 5, 10]
  Partition 1: keys 11-20  [15, 20]
  Partition 2: keys 21-30  [25, 30]
  Partition 3: keys 31-40  [35]

Useful when you want sorted output.
```

---

## Visualizing Partitions

```
+------------------------------------------------------------------+
|                        THE CLUSTER                               |
|                                                                  |
|   Executor 1                                                     |
|   +----------------------------------------------------------+   |
|   |  [Partition 0]  [Partition 4]  [Partition 8]             |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   Executor 2                                                     |
|   +----------------------------------------------------------+   |
|   |  [Partition 1]  [Partition 5]  [Partition 9]             |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   Executor 3                                                     |
|   +----------------------------------------------------------+   |
|   |  [Partition 2]  [Partition 6]  [Partition 10]            |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   Executor 4                                                     |
|   +----------------------------------------------------------+   |
|   |  [Partition 3]  [Partition 7]  [Partition 11]            |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   12 partitions distributed across 4 executors                   |
|   Each executor handles 3 partitions                             |
+------------------------------------------------------------------+
```

---

## Partition Count Recommendations

| Scenario | Recommended Partition Count |
|----------|----------------------------|
| Small data (< 1 GB) | 2-10 partitions |
| Medium data (1-100 GB) | 100-1000 partitions |
| Large data (100+ GB) | 1000+ partitions |
| Rule of thumb | 2-4 partitions per core |
| Another rule | ~128 MB per partition |

These are starting points—optimal counts depend on your specific workload.

---

## Key Takeaways

1. **Partitions are chunks of data:** Each partition is processed by one task.

2. **More partitions = more parallelism:** But too many creates overhead.

3. **Partitions are immutable:** Transformations create new partitions.

4. **Partition size matters:** Uneven sizes (skew) cause performance problems.

5. **Partitioning strategy matters:** Hash and range partitioning serve different purposes.

6. **You can control partitions:** Using `repartition()` and `coalesce()` (more on this Thursday).

---

## Additional Resources

- [Spark Partitioning (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations)
- [Understanding Spark Partitions (Databricks)](https://docs.databricks.com/en/spark/understanding-partitions.html)
- [Optimal Partition Count (Medium)](https://medium.com/@ashwin_kumar/how-to-decide-the-number-of-partitions-for-spark-1ef48c2b9bcd)
