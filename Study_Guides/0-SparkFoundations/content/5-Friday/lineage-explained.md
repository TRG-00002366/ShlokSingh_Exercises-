# Lineage Explained

## Learning Objectives
- Understand how Spark tracks the "recipe" for creating data
- Explain how lineage enables fault recovery
- Trace lineage through a multi-step computation
- Recognize when lineage is sufficient and when it is not

## Why This Matters

**Lineage** is Spark's memory of how each RDD was created. Instead of storing multiple copies of data for backup, Spark stores the "recipe" for recreating data. This is elegant, efficient, and enables fault tolerance without the overhead of data replication.

Understanding lineage helps you:
- Know how Spark recovers from failures
- Understand when checkpointing is needed
- Appreciate Spark's unique approach to fault tolerance

---

## What is Lineage?

**Lineage** (also called **dependency graph**) is the record of:
1. What parent RDD(s) this RDD came from
2. What transformation was applied

```
CODE:
lines = sc.textFile("log.txt")
errors = lines.filter(lambda x: "ERROR" in x)
messages = errors.map(lambda x: x.split("\t")[1])

LINEAGE:
+----------+                    +--------+                    +----------+
| lines    |  -- filter -->     | errors |  -- map -->        | messages |
| (source) |  "ERROR" in line   |        |  extract message   |          |
+----------+                    +--------+                    +----------+

messages knows:
  "I am errors.map(extract)"
  
errors knows:
  "I am lines.filter(has ERROR)"
  
lines knows:
  "I came from log.txt"
```

---

## Lineage as a Recipe

Think of lineage as a cooking recipe:

```
STORING DATA (wasteful):
- Store the raw ingredients
- Store the prepped ingredients
- Store the cooked ingredients
- Store the final dish
= 4x storage required

STORING LINEAGE (efficient):
- Store the raw ingredients (source data)
- Store the recipe (transformations)
= Only source + recipe needed

If the final dish is ruined, just follow the recipe again!
```

---

## Lineage Enables Recovery

When a partition is lost, Spark uses lineage to recompute it:

```
SCENARIO: Partition 2 of "messages" is lost

+----------+         +--------+         +----------+
| lines    |         | errors |         | messages |
| P0: OK   |         | P0: OK |         | P0: OK   |
| P1: OK   |         | P1: OK |         | P1: OK   |
| P2: OK   |         | P2: OK |         | P2: LOST |
| P3: OK   |         | P3: OK |         | P3: OK   |
+----------+         +--------+         +----------+

RECOVERY PROCESS:

Step 1: What is messages[P2]?
        -> messages[P2] = errors[P2].map(extract)
        
Step 2: What is errors[P2]?
        -> errors[P2] = lines[P2].filter(has ERROR)
        
Step 3: What is lines[P2]?
        -> lines[P2] = read chunk 2 from log.txt
        
Step 4: Execute the chain:
        lines[P2] -> filter -> errors[P2] -> map -> messages[P2]
        
RECOVERED!
```

---

## Narrow vs Wide Lineage

### Narrow Dependencies: Simple Recovery

With narrow dependencies, each child partition depends on one parent partition:

```
NARROW LINEAGE:
+----------+         +----------+
| Parent   |         | Child    |
| P0       | ------> | P0'      |
| P1       | ------> | P1'      |
| P2       | ------> | P2'      |
+----------+         +----------+

If P2' is lost:
  Just recompute from P2 (fast, single partition)
```

---

### Wide Dependencies: Complex Recovery

With wide dependencies, each child partition depends on ALL parent partitions:

```
WIDE LINEAGE:
+----------+         +----------+
| Parent   |         | Child    |
| P0       | -----+  |          |
| P1       | -----+->| P0'      |  (All P -> P0')
| P2       | -----+  |          |
+----------+         +----------+

If P0' is lost:
  Must recompute from ALL of P0, P1, P2
  If P0 is also lost, must recompute P0 first!
  
This can cascade back to the source data.
```

---

## Lineage Graph Example

```
+------------------------------------------------------------------+
|                    LINEAGE GRAPH                                 |
|                                                                  |
|   +-------------+                                                 |
|   | textFile    |  Source: "logs/*.txt"                          |
|   | (source)    |                                                 |
|   +------+------+                                                 |
|          |                                                       |
|          | filter(contains "ERROR")                              |
|          v                                                       |
|   +------+------+                                                 |
|   | filtered    |  Narrow dependency                             |
|   +------+------+                                                 |
|          |                                                       |
|          | map(extract timestamp)                                |
|          v                                                       |
|   +------+------+                                                 |
|   | timestamps  |  Narrow dependency                             |
|   +------+------+                                                 |
|          |                                                       |
|          | groupByKey()                                          |
|          v                                                       |
|   +------+------+                                                 |
|   | grouped     |  WIDE dependency (shuffle)                     |
|   +------+------+                                                 |
|          |                                                       |
|          | mapValues(count)                                      |
|          v                                                       |
|   +------+------+                                                 |
|   | counts      |  Narrow dependency                             |
|   +-------------+                                                 |
|                                                                  |
|   Each node knows its parent and the transformation applied.     |
+------------------------------------------------------------------+
```

---

## Lineage vs Replication

Traditional distributed systems use **replication** for fault tolerance:

```
REPLICATION APPROACH:
+--------+       +--------+       +--------+
| Data   | ----> | Replica| ----> | Replica|
| (copy 1)|      | (copy 2)|      | (copy 3)|
+--------+       +--------+       +--------+

3x storage cost!
But instant recovery (just use another copy).

---

LINEAGE APPROACH:
+--------+       +--------+
| Source | ----> | Recipe |
| Data   |       | (lineage)|
+--------+       +--------+

1x storage (source only) + tiny recipe!
But recovery requires recomputation.
```

### Trade-off

| Approach | Storage | Recovery Time | Best For |
|----------|---------|---------------|----------|
| Replication | High (Nx) | Instant | Databases, critical systems |
| Lineage | Low (1x + tiny) | Recompute time | Batch processing, analytics |

---

## When Lineage is Sufficient

Lineage works well when:

1. **Transformations are fast:** Recomputation is quick
2. **Lineage is short:** Few steps to trace back
3. **Source data is available:** Can always restart from source
4. **Failures are rare:** Recomputation happens infrequently

```
GOOD FOR LINEAGE:
Source -> Filter -> Map -> Reduce

Short chain, fast operations.
If final result is lost, recompute quickly.
```

---

## When Lineage is Not Enough

Lineage struggles when:

1. **Lineage is very long:** Many transformations in chain
2. **Source data is gone:** External source deleted
3. **Iterative algorithms:** Same computation repeated many times
4. **Wide dependencies:** Must recompute entire cluster's work

```
PROBLEMATIC FOR LINEAGE:
source -> T1 -> T2 -> ... -> T100 -> result
                                          ^
                                          |
                                        LOST

Recomputing T1 through T100 is expensive!
This is where CHECKPOINTING helps (next topic).
```

---

## How Spark Tracks Lineage

Each RDD stores:
1. **Parent RDD references**
2. **Partitioner** (how data is distributed)
3. **Compute function** (how to generate partition data)
4. **Preferred locations** (where data should ideally compute)

```
RDD Internal Structure:
+------------------------------------------+
| RDD: errors                              |
|                                          |
| Parent: lines                            |
| Partitioner: None (inherits parent)      |
| Compute: filter(contains "ERROR")        |
| Preferred Locations: inherit from parent |
+------------------------------------------+
```

---

## Key Takeaways

1. **Lineage is the "recipe":** How each RDD was created.

2. **Lineage enables recovery:** Recompute lost partitions from parents.

3. **Narrow dependencies recover quickly:** Only affected partition recomputed.

4. **Wide dependencies cascade:** May require recomputing many partitions.

5. **Lineage is more efficient than replication:** For batch processing workloads.

6. **Long lineages may need checkpointing:** To truncate the recovery chain.

---

## Additional Resources

- [Resilient Distributed Datasets Paper](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
- [RDD Fault Tolerance (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-fault-tolerance)
- [Understanding Spark Lineage (Video)](https://www.youtube.com/watch?v=dmL0N3qfSc8)
