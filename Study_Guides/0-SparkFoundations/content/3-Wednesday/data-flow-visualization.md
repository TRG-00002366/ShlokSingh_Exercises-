# Data Flow Visualization

## Learning Objectives
- Trace data flow through a complete Spark pipeline
- Identify partition boundaries and shuffle points
- Visualize stage transitions in a DAG
- Annotate data transformations at each step

## Why This Matters

Seeing data flow through a Spark pipeline ties together everything from this week: partitions, executors, narrow transformations, wide transformations, and shuffles. This visual understanding is what enables you to reason about Spark job performance and behavior.

This document provides annotated diagrams that you can use as mental models when working with Spark.

---

## Example Pipeline

Let us trace a realistic data pipeline:

```
Read sales data (1M rows)
  -> Filter for this year
  -> Calculate total per product
  -> Join with product names
  -> Get top 10 products
```

We will visualize each step.

---

## Step 1: Read Data

Data is loaded from distributed storage into partitions:

```
+------------------------------------------------------------------+
|                        READ FROM SOURCE                          |
|                                                                  |
|   DATA SOURCE (S3/HDFS)                                          |
|   +----------------------------------------------------------+   |
|   | file1.parquet | file2.parquet | file3.parquet | file4.p  |   |
|   +----------------------------------------------------------+   |
|                              |                                   |
|                       SPARK READS                                |
|                              |                                   |
|   +------------+  +------------+  +------------+  +------------+ |
|   | Partition 0|  | Partition 1|  | Partition 2|  | Partition 3| |
|   |  250K rows |  |  250K rows |  |  250K rows |  |  250K rows | |
|   +------------+  +------------+  +------------+  +------------+ |
|   | Executor 1 |  | Executor 2 |  | Executor 3 |  | Executor 4 | |
|   +------------+  +------------+  +------------+  +------------+ |
|                                                                  |
|   Total: 1 Million rows across 4 partitions                      |
+------------------------------------------------------------------+
```

---

## Step 2: Filter (Narrow Transformation)

Filtering keeps only matching rows. This is a **narrow transformation**—each partition is processed independently:

```
+------------------------------------------------------------------+
|                      FILTER (year = 2024)                        |
|                      Narrow Transformation                       |
|                                                                  |
|   BEFORE                           AFTER                         |
|   +------------+                   +------------+                |
|   | Partition 0|      filter()     | Partition 0|                |
|   |  250K rows | ----------------> |  150K rows |                |
|   +------------+                   +------------+                |
|   | Executor 1 |                   | Executor 1 |                |
|   +------------+                   +------------+                |
|                                                                  |
|   +------------+                   +------------+                |
|   | Partition 1|      filter()     | Partition 1|                |
|   |  250K rows | ----------------> |  145K rows |                |
|   +------------+                   +------------+                |
|   | Executor 2 |                   | Executor 2 |                |
|   +------------+                   +------------+                |
|                                                                  |
|   +------------+                   +------------+                |
|   | Partition 2|      filter()     | Partition 2|                |
|   |  250K rows | ----------------> |  155K rows |                |
|   +------------+                   +------------+                |
|   | Executor 3 |                   | Executor 3 |                |
|   +------------+                   +------------+                |
|                                                                  |
|   +------------+                   +------------+                |
|   | Partition 3|      filter()     | Partition 3|                |
|   |  250K rows | ----------------> |  150K rows |                |
|   +------------+                   +------------+                |
|   | Executor 4 |                   | Executor 4 |                |
|   +------------+                   +------------+                |
|                                                                  |
|   NO DATA MOVEMENT - Each executor processes locally             |
|   Total: 600K rows remaining (400K filtered out)                 |
+------------------------------------------------------------------+
```

---

## Step 3: Group and Sum (Wide Transformation)

Calculating totals per product requires a **groupBy**. This is a **wide transformation** that triggers a **shuffle**:

```
+------------------------------------------------------------------+
|                  GROUP BY PRODUCT_ID + SUM                       |
|                   Wide Transformation (SHUFFLE)                  |
|                                                                  |
|   BEFORE (Stage 1 - Map Side)                                    |
|   +------------+  +------------+  +------------+  +------------+ |
|   | Partition 0|  | Partition 1|  | Partition 2|  | Partition 3| |
|   |Product: A,B|  |Product: B,C|  |Product: A,C|  |Product: A,B| |
|   | A=100, B=50|  | B=60, C=30 |  | A=80, C=40 |  | A=90, B=70 | |
|   +------------+  +------------+  +------------+  +------------+ |
|        |              |               |               |          |
|        +-------+------+-------+-------+-------+-------+          |
|                |              |               |                  |
|            SHUFFLE: Redistribute by product_id                   |
|                |              |               |                  |
|        +-------+------+-------+-------+-------+-------+          |
|        |              |               |               |          |
|        v              v               v               v          |
|   +------------+  +------------+  +------------+                 |
|   | All A data |  | All B data |  | All C data |                 |
|   | A=100,80,90|  | B=50,60,70 |  | C=30,40    |                 |
|   +------------+  +------------+  +------------+                 |
|   | Executor 1 |  | Executor 2 |  | Executor 3 |                 |
|                                                                  |
|   AFTER (Stage 2 - Reduce Side)                                  |
|   +------------+  +------------+  +------------+                 |
|   | A = 270    |  | B = 180    |  | C = 70     |                 |
|   +------------+  +------------+  +------------+                 |
|                                                                  |
|   DATA MOVED ACROSS NETWORK - Shuffle occurred!                  |
|   3 aggregated results (one per product)                         |
+------------------------------------------------------------------+
```

---

## Step 4: Join with Product Names (Wide Transformation)

Joining with product name data triggers another **shuffle**:

```
+------------------------------------------------------------------+
|                    JOIN WITH PRODUCT NAMES                       |
|                   Wide Transformation (SHUFFLE)                  |
|                                                                  |
|   LEFT: Sales Totals            RIGHT: Product Names             |
|   +------------+                +-------------------+            |
|   | A = 270    |                | A = "Widget Pro"  |            |
|   | B = 180    |                | B = "Gadget Plus" |            |
|   | C = 70     |                | C = "Super Tool"  |            |
|   +------------+                +-------------------+            |
|        |                               |                         |
|        +----------- SHUFFLE -----------+                         |
|        (Repartition both by product_id)                          |
|                         |                                        |
|                         v                                        |
|   +--------------------------------------------------+           |
|   | A = 270, "Widget Pro"                            |           |
|   | B = 180, "Gadget Plus"                           |           |
|   | C = 70, "Super Tool"                             |           |
|   +--------------------------------------------------+           |
|                                                                  |
|   BOTH sides shuffled to match keys on same partitions           |
+------------------------------------------------------------------+
```

---

## Step 5: Top 10 (Wide Transformation)

Getting top 10 requires sorting, which is another **shuffle**:

```
+------------------------------------------------------------------+
|                         TOP 10 PRODUCTS                          |
|                   Wide Transformation (SHUFFLE)                  |
|                                                                  |
|   BEFORE: Distributed across partitions                          |
|   +------------+  +------------+  +------------+                 |
|   | Partition 1|  | Partition 2|  | Partition 3|                 |
|   | A=270      |  | B=180      |  | C=70       |                 |
|   +------------+  +------------+  +------------+                 |
|        |              |               |                          |
|        +---------- GLOBAL SORT (shuffle) --------+               |
|                          |                       |               |
|                          v                       v               |
|   +--------------------------------------------------+           |
|   | Sorted: A=270, B=180, C=70, ...                  |           |
|   +--------------------------------------------------+           |
|                          |                                       |
|                    TAKE TOP 10                                   |
|                          |                                       |
|   +--------------------------------------------------+           |
|   | Result: A=270 "Widget Pro"                       |           |
|   |         B=180 "Gadget Plus"                      |           |
|   |         C=70  "Super Tool"                       |           |
|   +--------------------------------------------------+           |
|                                                                  |
|   Result returned to DRIVER                                      |
+------------------------------------------------------------------+
```

---

## Complete Pipeline: DAG View

The entire pipeline as a Directed Acyclic Graph:

```
+------------------------------------------------------------------+
|                         COMPLETE DAG                             |
|                                                                  |
|   +----------------+                                              |
|   |    READ        | Stage 1                                      |
|   | (4 partitions) |                                              |
|   +-------+--------+                                              |
|           |                                                       |
|           v                                                       |
|   +----------------+                                              |
|   |    FILTER      | Stage 1 (continued - narrow, pipelined)      |
|   | (4 partitions) |                                              |
|   +-------+--------+                                              |
|           |                                                       |
|     SHUFFLE BOUNDARY (Stage 1 -> Stage 2)                        |
|           |                                                       |
|           v                                                       |
|   +----------------+                                              |
|   |   GROUP BY     | Stage 2                                      |
|   |   + SUM        |                                              |
|   +-------+--------+                                              |
|           |                                                       |
|     SHUFFLE BOUNDARY (Stage 2 -> Stage 3)                        |
|           |                                                       |
|           v                                                       |
|   +----------------+       +----------------+                     |
|   |  Sales Totals  |  JOIN | Product Names  |                     |
|   +-------+--------+       +--------+-------+                     |
|           |                         |                             |
|           +-----------+-------------+                             |
|                       |                                           |
|     SHUFFLE BOUNDARY (Stage 3 -> Stage 4)                        |
|                       |                                           |
|                       v                                           |
|               +----------------+                                  |
|               |     SORT       | Stage 4                          |
|               |   + TOP 10     |                                  |
|               +-------+--------+                                  |
|                       |                                           |
|                       v                                           |
|               +----------------+                                  |
|               |    RESULT      | -> Driver                        |
|               |   (10 rows)    |                                  |
|               +----------------+                                  |
|                                                                  |
|   Total: 4 Stages, 3 Shuffles                                    |
+------------------------------------------------------------------+
```

---

## Annotated Summary

| Step | Operation | Type | Data Movement | Result |
|------|-----------|------|---------------|--------|
| 1 | Read | Source | Disk to memory | 1M rows in 4 partitions |
| 2 | Filter | Narrow | None | 600K rows (same partitions) |
| 3 | GroupBy+Sum | Wide | **SHUFFLE** | 3 aggregates |
| 4 | Join | Wide | **SHUFFLE** | 3 rows with names |
| 5 | Top 10 | Wide | **SHUFFLE** | 10 rows to Driver |

---

## Key Observations

1. **Narrow transformations (filter) are pipelined:** No intermediate writes.

2. **Wide transformations create stage boundaries:** Each shuffle = new stage.

3. **Shuffles dominate runtime:** In this example, 3 shuffles for 3 wide operations.

4. **Data volume decreases through pipeline:** 1M -> 600K -> 3 aggregates -> 10 results.

5. **Final result goes to Driver:** Small result set (10 rows) is safe to collect.

---

## Key Takeaways

1. **Trace data through partitions:** Know where data lives at each step.

2. **Identify shuffle points:** Stage boundaries where wide transformations occur.

3. **Narrow operations are cheap:** Pipeline them without concern.

4. **Wide operations are expensive:** Each triggers a shuffle.

5. **Visualize before optimizing:** Understand the flow before trying to improve it.

---

## Additional Resources

- [Spark Web UI - Understanding DAGs](https://spark.apache.org/docs/latest/web-ui.html)
- [Spark Execution Model (Databricks)](https://docs.databricks.com/en/spark/execution.html)
- [Visualizing Spark Jobs (Video)](https://www.youtube.com/watch?v=49Hr5xZyTEA)
