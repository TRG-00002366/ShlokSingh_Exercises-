# Checkpointing Overview

## Learning Objectives
- Understand when lineage-based recovery is insufficient
- Explain what checkpointing does and how it works
- Identify scenarios that require checkpointing
- Recognize the trade-offs between lineage and checkpointing

## Why This Matters

While lineage enables elegant fault recovery, there are situations where recomputing from lineage is impractical:
- Very long transformation chains
- Iterative algorithms (machine learning, graph processing)
- When source data may become unavailable

Checkpointing breaks the lineage chain by saving data to reliable storage. Understanding when to checkpoint is essential for building robust Spark applications.

---

## The Problem with Long Lineages

Lineage recovery recomputes from the source:

```
SHORT LINEAGE (fast recovery):
source -> T1 -> T2 -> result
                        ^
                        LOST
                        
Recompute: T1 -> T2
Time: Seconds

---

LONG LINEAGE (slow recovery):
source -> T1 -> T2 -> T3 -> ... -> T99 -> T100 -> result
                                                    ^
                                                   LOST
                                                   
Recompute: T1 -> T2 -> T3 -> ... -> T99 -> T100
Time: Hours (entire computation repeated!)
```

---

## Iterative Algorithms: The Extreme Case

Machine learning algorithms often iterate many times:

```
ITERATIVE ALGORITHM (e.g., K-Means):

data -> iteration 1 -> iteration 2 -> ... -> iteration 100 -> model

Lineage grows with each iteration:
+----------+
| iter 1   | <- depends on data
+----+-----+
     |
+----v-----+
| iter 2   | <- depends on iter 1
+----+-----+
     |
    ...
     |
+----v-----+
| iter 100 | <- depends on iter 99
+----------+

If iter 100 partition is lost:
  -> Must recompute iter 99
  -> Which needs iter 98
  -> ... all the way back to data!

Recovery time = Original computation time
This is unacceptable!
```

---

## What is Checkpointing?

**Checkpointing** saves an RDD to reliable storage (like HDFS or S3), then **truncates the lineage**:

```
BEFORE CHECKPOINT:
data -> T1 -> T2 -> T3 -> T4 -> T5 -> T6 -> T7 -> T8 -> result
                                                          ^
                                                       LINEAGE
                                                       (8 steps)

AFTER CHECKPOINT (at T4):
data -> T1 -> T2 -> T3 -> T4 [SAVED TO HDFS] -> T5 -> T6 -> T7 -> T8 -> result
                              ^                                          ^
                         CHECKPOINT                                  LINEAGE
                         (on disk)                                   (4 steps)

If result is lost:
  -> Only need to recover T5 -> T6 -> T7 -> T8
  -> T4 is read from checkpoint (no recomputation)
```

---

## How Checkpointing Works

```
+------------------------------------------------------------------+
|                    CHECKPOINTING PROCESS                         |
|                                                                  |
|   STEP 1: Mark RDD for checkpoint                                |
|   +----------------------------------------------------------+   |
|   | rdd.checkpoint()   # Mark (does not execute yet)         |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   STEP 2: Trigger action (checkpoint happens during execution)   |
|   +----------------------------------------------------------+   |
|   | rdd.count()        # Action triggers checkpoint          |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   STEP 3: Data written to reliable storage                       |
|   +----------------------------------------------------------+   |
|   | HDFS / S3 / Azure Blob:                                  |   |
|   |   checkpoint-dir/rdd-123/                                |   |
|   |     part-00000                                           |   |
|   |     part-00001                                           |   |
|   |     part-00002                                           |   |
|   |     part-00003                                           |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   STEP 4: Lineage truncated                                      |
|   +----------------------------------------------------------+   |
|   | rdd.toDebugString() now shows:                           |   |
|   |   "ReliableCheckpointRDD" instead of full lineage        |   |
|   +----------------------------------------------------------+   |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Checkpoint vs Cache

Both persist data, but they serve different purposes:

```
CACHE:
+-------------+                        +-------------+
| RDD Data    | -- cache() -->         | Executor    |
|             |                        | Memory      |
+-------------+                        +-------------+
                                       (Fast access)
                                       (Lost if executor dies)
                                       (Lineage preserved)

CHECKPOINT:
+-------------+                        +-------------+
| RDD Data    | -- checkpoint() -->    | HDFS / S3   |
|             |                        | (reliable)  |
+-------------+                        +-------------+
                                       (Slower access)
                                       (Survives failures)
                                       (Lineage truncated)
```

---

### Comparison Table

| Feature | Cache | Checkpoint |
|---------|-------|------------|
| Storage Location | Executor memory/disk | External reliable storage |
| Speed | Fast | Slower |
| Survives Executor Failure | No | Yes |
| Lineage Preserved | Yes | No (truncated) |
| Use Case | Reuse within job | Fault tolerance, long lineages |

---

## When to Checkpoint

### Scenario 1: Long Lineages

```
# After many transformations
rdd1 = source.map(...)
rdd2 = rdd1.filter(...)
rdd3 = rdd2.flatMap(...)
# ... 50 more transformations ...
rdd53 = rdd52.map(...)

rdd53.checkpoint()  # Truncate lineage here
rdd53.count()       # Trigger checkpoint
```

---

### Scenario 2: Iterative Algorithms

```
# Checkpoint every N iterations
sc.setCheckpointDir("hdfs://checkpoint-dir")

model = initial_model
for i in range(100):
    model = update_model(model, data)
    
    if i % 10 == 0:  # Every 10 iterations
        model.checkpoint()
        model.count()  # Trigger checkpoint
        
# If failure occurs at iteration 95:
#   - Recover from iteration 90 checkpoint
#   - Only redo 5 iterations, not 95
```

---

### Scenario 3: Wide Dependency Before Reuse

```
# After a shuffle, checkpoint to avoid expensive recomputation
grouped = data.groupByKey()  # Wide transformation (shuffle)

grouped.checkpoint()
grouped.count()  # Materialize checkpoint

# Now grouped can be reused without expensive shuffle recovery
result1 = grouped.mapValues(process1)
result2 = grouped.mapValues(process2)
```

---

## Diagram: Recovery with Checkpoint

```
+------------------------------------------------------------------+
|                 RECOVERY WITH CHECKPOINT                         |
|                                                                  |
|   LINEAGE ONLY:                                                  |
|   source -> T1 -> T2 -> T3 -> T4 -> T5 -> T6 -> result           |
|                                                       ^          |
|                                                      LOST        |
|   Recovery: Recompute T1 -> T2 -> T3 -> T4 -> T5 -> T6           |
|   Time: Long                                                     |
|                                                                  |
|   -----------------------------------------------------------    |
|                                                                  |
|   WITH CHECKPOINT AT T3:                                         |
|                                                                  |
|   source -> T1 -> T2 -> T3 [CHECKPOINT] -> T4 -> T5 -> T6 -> result
|                              ^                              ^    |
|                           SAVED                           LOST   |
|                                                                  |
|   Recovery: Read T3 from checkpoint -> T4 -> T5 -> T6            |
|   Time: Short (only 3 steps, not 6)                              |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Checkpointing Best Practices

### 1. Set Checkpoint Directory

```
# Must set before checkpointing
sc.setCheckpointDir("hdfs:///user/spark/checkpoints")
```

---

### 2. Use Reliable Storage

```
GOOD: HDFS, S3, Azure Blob (replicated, reliable)
BAD:  Local file system (lost if node fails)
```

---

### 3. Checkpoint After Expensive Operations

```
# Checkpoint after shuffles or expensive computations
expensive_result = data.groupByKey().mapValues(complex_operation)
expensive_result.checkpoint()
expensive_result.count()
```

---

### 4. Combine with Cache

```
# Cache for speed, checkpoint for reliability
rdd.cache()
rdd.checkpoint()
rdd.count()  # First action: cache + checkpoint
rdd.map(...)  # Fast (from cache)
# If executor fails: recover from checkpoint
```

---

## Trade-offs

| Benefit | Cost |
|---------|------|
| Shorter recovery time | Storage space in reliable system |
| Truncated lineage | Extra write operation |
| Survives all failures | Slower than cache |
| Required for iterative algorithms | Management overhead |

---

## Key Takeaways

1. **Lineage can be too long:** Recovery time equals computation time.

2. **Checkpointing saves to reliable storage:** HDFS, S3, etc.

3. **Checkpointing truncates lineage:** Recovery reads from checkpoint.

4. **Use for iterative algorithms:** Break lineage every N iterations.

5. **Cache + Checkpoint:** Speed + reliability.

6. **Choose checkpoint location wisely:** Must be reliable storage.

---

## Additional Resources

- [RDD Checkpointing (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence)
- [Structured Streaming Checkpointing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
- [Checkpoint Best Practices (Databricks)](https://docs.databricks.com/en/structured-streaming/production.html#configure-checkpointing)
