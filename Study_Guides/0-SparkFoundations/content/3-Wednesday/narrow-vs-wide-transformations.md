# Narrow vs Wide Transformations

## Learning Objectives
- Distinguish between narrow and wide transformations
- Understand why this distinction matters for performance
- Identify common transformations in each category
- Recognize stage boundaries in Spark job execution

## Why This Matters

Not all Spark transformations are created equal. Some can run entirely within a single partition (narrow), while others require data from multiple partitions to be combined (wide). This distinction directly impacts:
- How Spark plans job execution
- Where stage boundaries are created
- How much network traffic your job generates
- How fast your job runs

Understanding narrow vs wide is essential for writing efficient Spark applications.

---

## Narrow Transformations

A **narrow transformation** is one where each output partition depends on at most one input partition.

```
NARROW TRANSFORMATION (e.g., map, filter)

Input Partitions          Output Partitions
+-------------+           +-------------+
| Partition 1 | --------> | Partition 1'|
+-------------+           +-------------+
+-------------+           +-------------+
| Partition 2 | --------> | Partition 2'|
+-------------+           +-------------+
+-------------+           +-------------+
| Partition 3 | --------> | Partition 3'|
+-------------+           +-------------+

Each output partition comes from exactly one input partition.
No data needs to move between executors!
```

### Characteristics of Narrow Transformations

- **No shuffle required:** Data stays on the same executor
- **Can be pipelined:** Multiple narrow transformations can run in sequence without writing to disk
- **Fast:** Only involves local computation
- **No network traffic:** Data does not cross executor boundaries

---

### Common Narrow Transformations

| Transformation | Description |
|----------------|-------------|
| `map()` | Apply function to each element |
| `flatMap()` | Apply function, flatten results |
| `filter()` | Keep elements matching condition |
| `mapPartitions()` | Apply function to entire partition |
| `union()` | Combine RDDs (no shuffle if partitions align) |
| `sample()` | Take a random sample |
| `coalesce()` | Reduce partition count (special case) |

---

## Wide Transformations

A **wide transformation** is one where output partitions may depend on multiple input partitions.

```
WIDE TRANSFORMATION (e.g., groupByKey, join)

Input Partitions          Output Partitions
+-------------+           
| Partition 1 | ----+     
+-------------+     |
                    +---> +-------------+
+-------------+     |     | Partition 1'|
| Partition 2 | ----+     +-------------+
+-------------+     |
                    +---> +-------------+
+-------------+     |     | Partition 2'|
| Partition 3 | ----+     +-------------+
+-------------+     

Output partitions receive data from multiple input partitions.
Data MUST move between executors - this is a SHUFFLE!
```

### Characteristics of Wide Transformations

- **Shuffle required:** Data must be redistributed across the cluster
- **Creates stage boundary:** Spark must complete all input partitions before continuing
- **Writes to disk:** Shuffle data is written to disk before transfer
- **Network intensive:** Data crosses executor boundaries
- **Slower:** The most expensive operation in Spark

---

### Common Wide Transformations

| Transformation | Description |
|----------------|-------------|
| `groupByKey()` | Group values by key |
| `reduceByKey()` | Combine values by key (still wide, but more efficient) |
| `aggregateByKey()` | Aggregate values by key |
| `join()` | Join two datasets by key |
| `cogroup()` | Group data from multiple RDDs by key |
| `repartition()` | Change partition count |
| `distinct()` | Remove duplicates |
| `sortByKey()` | Sort by key |

---

## Visual Comparison

```
+------------------------------------------------------------------+
|                     NARROW TRANSFORMATION                        |
|                          (filter)                                |
|                                                                  |
|   Executor 1        Executor 2        Executor 3                 |
|   +--------+        +--------+        +--------+                 |
|   | Part 1 |        | Part 2 |        | Part 3 |                 |
|   |   v    |        |   v    |        |   v    |                 |
|   | filter |        | filter |        | filter |                 |
|   |   v    |        |   v    |        |   v    |                 |
|   | Part 1'|        | Part 2'|        | Part 3'|                 |
|   +--------+        +--------+        +--------+                 |
|                                                                  |
|   No data crosses executor boundaries!                           |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
|                     WIDE TRANSFORMATION                          |
|                        (groupByKey)                              |
|                                                                  |
|   Executor 1        Executor 2        Executor 3                 |
|   +--------+        +--------+        +--------+                 |
|   | A,B,A  |        | B,C,A  |        | C,A,B  |                 |
|   +----+---+        +----+---+        +----+---+                 |
|        |                |                  |                     |
|        +------+---------+-------+----------+                     |
|               |                 |                                |
|         SHUFFLE (NETWORK)                                        |
|               |                 |                                |
|        +------+---------+-------+----------+                     |
|        |                |                  |                     |
|   +----v---+        +---v----+        +----v---+                 |
|   | A,A,A,A|        | B,B,B  |        | C,C    |                 |
|   +--------+        +--------+        +--------+                 |
|                                                                  |
|   Data with the same key ends up on the same executor            |
+------------------------------------------------------------------+
```

---

## Stage Boundaries

Spark creates a **new stage** at every wide transformation:

```
CODE:
rdd.filter(...)     # narrow
   .map(...)        # narrow
   .groupByKey()    # WIDE <-- Stage boundary
   .map(...)        # narrow
   .reduceByKey()   # WIDE <-- Stage boundary
   .collect()       # action

STAGES:
+------------------+
|     STAGE 1      |
|  filter -> map   |
+------------------+
         |
      SHUFFLE
         |
+------------------+
|     STAGE 2      |
|  groupByKey->map |
+------------------+
         |
      SHUFFLE
         |
+------------------+
|     STAGE 3      |
|   reduceByKey    |
+------------------+
         |
       RESULT
```

---

## Why This Matters for Performance

### Narrow Transformations: Pipelining

Multiple narrow transformations can be **pipelined** without intermediate writes:

```
PIPELINING (NARROW TRANSFORMATIONS)

For each row:
  1. Apply filter
  2. Apply map
  3. Apply flatMap
  
All in memory, one pass through the data!

+-------+    +--------+    +-----+    +----------+
| Input | -> | filter | -> | map | -> | flatMap  | -> Output
+-------+    +--------+    +-----+    +----------+

Efficient: No intermediate data materialization
```

---

### Wide Transformations: Materialization

Wide transformations require **materialization** to disk:

```
MATERIALIZATION (WIDE TRANSFORMATION)

Before shuffle:
1. Process all partitions until shuffle point
2. Write shuffle output to disk (spill)
3. Wait for all executors to complete

During shuffle:
4. Executors read shuffle files from each other
5. Merge data by key

After shuffle:
6. Continue with next stage

+-------+    +--------+    +---------+    +----------+
| Stage1| -> | SHUFFLE| -> | Disk I/O| -> |  Stage 2 |
+-------+    | (wait) |    | (slow)  |    +----------+
             +---------+    +---------+

Expensive: Disk writes, network transfer, synchronization
```

---

## Transformation Classification Table

| Transformation | Type | Causes Shuffle? |
|----------------|------|-----------------|
| `map` | Narrow | No |
| `filter` | Narrow | No |
| `flatMap` | Narrow | No |
| `mapPartitions` | Narrow | No |
| `union` | Narrow* | Usually no |
| `coalesce` | Narrow* | No (but may imbalance) |
| `groupByKey` | Wide | Yes |
| `reduceByKey` | Wide | Yes (but optimized) |
| `aggregateByKey` | Wide | Yes (but optimized) |
| `join` | Wide | Yes |
| `cogroup` | Wide | Yes |
| `repartition` | Wide | Yes |
| `distinct` | Wide | Yes |
| `sortByKey` | Wide | Yes |

*Note: `union` and `coalesce` can sometimes avoid shuffles in special cases.

---

## Key Takeaways

1. **Narrow transformations** depend on single input partitions—no shuffle needed.

2. **Wide transformations** combine data from multiple partitions—shuffle required.

3. **Shuffles are expensive:** They involve disk I/O, network transfer, and synchronization.

4. **Stage boundaries** are created at wide transformations.

5. **Pipelining** is possible for narrow transformations—efficient memory use.

6. **Minimize wide transformations:** Each shuffle is a performance cost.

---

## Additional Resources

- [RDD Transformations (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)
- [Understanding Spark Stages (Databricks)](https://docs.databricks.com/en/spark/stages.html)
- [Narrow vs Wide Dependencies (Video)](https://www.youtube.com/watch?v=49Hr5xZyTEA)
