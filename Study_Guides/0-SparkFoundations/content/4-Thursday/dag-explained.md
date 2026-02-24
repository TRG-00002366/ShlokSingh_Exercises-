# DAG Explained

## Learning Objectives
- Define what a Directed Acyclic Graph (DAG) is
- Understand how Spark builds DAGs from transformations
- Trace dependencies through a DAG
- Read DAG visualizations in Spark UI

## Why This Matters

When you write Spark code, you are implicitly building a **DAG**—a graph that shows how each piece of data depends on previous computations. Understanding DAGs helps you:
- Visualize job execution
- Identify bottlenecks
- Understand why certain operations trigger shuffles
- Debug performance problems using Spark UI

The DAG is Spark's execution blueprint. Knowing how to read it is essential.

---

## What is a DAG?

**DAG** stands for **Directed Acyclic Graph**:

- **Directed:** Edges have a direction (A -> B means A must complete before B)
- **Acyclic:** No cycles—you cannot go from A back to A
- **Graph:** Nodes connected by edges

```
DIRECTED ACYCLIC GRAPH:

    A -----> B -----> D
              \      /
               v    v
               C ---->

A must finish before B
B must finish before C and D
C must finish before D

This is valid (acyclic).

---

NOT A DAG (has cycle):

    A -----> B
    ^        |
    |        v
    +------- C

A requires C, C requires B, B requires A
This is a cycle - invalid!
```

---

## DAGs in Spark

In Spark, the DAG represents the **lineage** of your data:
- **Nodes:** Data states (RDDs, DataFrames, or intermediate results)
- **Edges:** Transformations that produce new data from existing data

```
CODE:
lines = sc.textFile("file.txt")           # Node 1: Raw lines
words = lines.flatMap(lambda x: x.split())# Node 2: Words
pairs = words.map(lambda x: (x, 1))       # Node 3: Word-count pairs
counts = pairs.reduceByKey(lambda a,b: a+b)# Node 4: Final counts

DAG:
+-------+    flatMap    +-------+    map    +-------+   reduceByKey  +-------+
| lines | ------------> | words | --------> | pairs | ------------> |counts |
+-------+               +-------+           +-------+               +-------+
```

---

## Building the DAG

### Narrow Dependencies

When transformations are narrow (no shuffle), the DAG shows direct lineage:

```
NARROW DEPENDENCY:

+-------+          +--------+          +--------+
| RDD A | -------> | RDD A' | -------> | RDD A''|
+-------+  filter  +--------+  map     +--------+

Each partition of A' comes from exactly one partition of A
Each partition of A'' comes from exactly one partition of A'

DAG representation:
A[0] -> A'[0] -> A''[0]
A[1] -> A'[1] -> A''[1]
A[2] -> A'[2] -> A''[2]
```

---

### Wide Dependencies

When transformations are wide (shuffle required), the DAG shows fan-out:

```
WIDE DEPENDENCY:

+-------+                  +-------+
| RDD A |                  | RDD B |
+-------+                  +-------+
| P0    | ----+       +--> | P0    |
| P1    | ----+----+--+--> | P1    |
| P2    | ----+----+--+--> | P2    |
+-------+     |    |  |    +-------+
              |    |  |
        All partitions contribute
        to all new partitions!

DAG shows this as a shuffle boundary (stage boundary)
```

---

## DAG and Stages

Spark divides the DAG into **stages** at shuffle boundaries:

```
TRANSFORMATIONS:
read -> filter -> map -> groupByKey -> map -> reduceByKey -> collect

DAG WITH STAGES:

+-----------------------------------------------------+
|                      STAGE 1                        |
|   read -> filter -> map                             |
|   (Narrow dependencies, can be pipelined)           |
+-------------------------+---------------------------+
                          |
                    SHUFFLE (groupByKey)
                          |
+-------------------------v---------------------------+
|                      STAGE 2                        |
|   groupByKey result -> map                          |
|   (Narrow dependencies, can be pipelined)           |
+-------------------------+---------------------------+
                          |
                    SHUFFLE (reduceByKey)
                          |
+-------------------------v---------------------------+
|                      STAGE 3                        |
|   reduceByKey result                                |
+-----------------------------------------------------+
```

---

## Why DAGs Matter

### 1. Fault Tolerance

If a partition is lost, Spark can recompute it by following the DAG:

```
DAG LINEAGE:

Source -> Filter -> Map -> GroupBy -> Result
                     |
                     X (lost partition!)
                     |
Spark says: "I can recompute this by re-running
             Filter on the source, then Map."
```

### 2. Optimization

Catalyst reads the DAG to find optimization opportunities:

```
ORIGINAL DAG:
Select(all columns) -> Filter -> Select(subset)

OPTIMIZED DAG:
Select(subset) -> Filter

Push projection to source!
```

### 3. Parallel Execution

Independent branches of the DAG can run in parallel:

```
      +-------+
      | Read1 |
      +---+---+
          |
      +---v---+
      |Filter1|
      +---+---+
          |         +-------+
          |         | Read2 |
          |         +---+---+
          |             |
          |         +---v---+
          |         |Filter2|
          |         +---+---+
          |             |
          +------+------+
                 |
             +---v---+
             | JOIN  |
             +-------+

Read1,Filter1 and Read2,Filter2 can run in parallel!
They are independent branches.
```

---

## Reading DAGs in Spark UI

The Spark UI shows the DAG for each job:

```
+------------------------------------------------------------------+
|                        SPARK UI DAG VIEW                         |
|                                                                  |
|   Job 0: count at Script.py:42                                   |
|                                                                  |
|   +----------------+                                              |
|   | Stage 0        |  [============] 4/4 tasks complete          |
|   | WholeStageCodegen                                            |
|   | - FileScan csv                                               |
|   | - Filter                                                     |
|   | - Project                                                    |
|   +--------+-------+                                              |
|            |                                                      |
|            | Exchange (shuffle)                                   |
|            |                                                      |
|   +--------v-------+                                              |
|   | Stage 1        |  [====        ] 2/4 tasks complete          |
|   | HashAggregate                                                |
|   +----------------+                                              |
|                                                                  |
+------------------------------------------------------------------+
```

Key elements:
- **WholeStageCodegen:** Multiple operations fused together
- **Exchange:** A shuffle between stages
- **FileScan:** Reading from source
- **Task counts:** Progress of each stage

---

## DAG Examples

### Example 1: Simple Pipeline

```
val result = sc.textFile("data.txt")   // Stage 1 start
               .filter(_.length > 10)   // Stage 1
               .map(_.toUpperCase)      // Stage 1
               .count()                  // Action

DAG:
+----------+     +--------+     +-----+     +-------+
| textFile | --> | filter | --> | map | --> | count |
+----------+     +--------+     +-----+     +-------+
                                            
              [All in Stage 1 - no shuffles]
```

---

### Example 2: With Shuffle

```
val result = sc.textFile("data.txt")
               .flatMap(_.split(" "))
               .map(word => (word, 1))
               .reduceByKey(_ + _)       // <-- SHUFFLE
               .filter(_._2 > 5)
               .collect()

DAG:
+----------+     +---------+     +-----+
| textFile | --> | flatMap | --> | map |
+----------+     +---------+     +--+--+
                                    |
                              SHUFFLE (Stage boundary)
                                    |
                                 +--v--+     +--------+     +---------+
                                 |reduce| --> | filter | --> | collect |
                                 +------+     +--------+     +---------+

Stage 1: textFile, flatMap, map
Stage 2: reduceByKey, filter, collect
```

---

### Example 3: Multiple Inputs

```
val joined = df1.join(df2, "key")  // Join requires shuffle of both

DAG:
        +------+                +------+
        | df1  |                | df2  |
        +--+---+                +--+---+
           |                       |
      SHUFFLE                 SHUFFLE
           |                       |
           +----------+------------+
                      |
                  +---v---+
                  | JOIN  |
                  +-------+

Both inputs shuffled to colocate matching keys
```

---

## Key Takeaways

1. **DAG = Directed Acyclic Graph:** Shows data dependencies.

2. **Nodes are data states:** Inputs, intermediate results, outputs.

3. **Edges are transformations:** Operations that create new data.

4. **Stages form at shuffles:** Wide transformations create stage boundaries.

5. **DAG enables fault tolerance:** Lineage allows recomputation.

6. **Spark UI shows DAGs:** Use it to understand job execution.

---

## Additional Resources

- [Understanding Spark DAG (Databricks)](https://docs.databricks.com/en/spark/dag.html)
- [Spark UI Guide (Official Docs)](https://spark.apache.org/docs/latest/web-ui.html)
- [DAG Scheduler Deep Dive (Video)](https://www.youtube.com/watch?v=49Hr5xZyTEA)
