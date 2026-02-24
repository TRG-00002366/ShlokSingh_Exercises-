# Fault Recovery in Action

## Learning Objectives
- Trace through a complete failure and recovery scenario
- Understand what happens when an executor fails mid-task
- Visualize partition reconstruction from lineage
- Recognize the limits of lineage-based recovery

## Why This Matters

Abstract concepts become concrete when you see them in action. This document walks through exactly what happens when a failure occurs in Spark—from detection to recovery to completion. Understanding this flow helps you:
- Trust that Spark will handle failures correctly
- Debug issues when recovery does not work as expected
- Make informed decisions about checkpointing

---

## The Scenario

We have a simple Spark job processing 1 million rows across 4 partitions:

```
JOB:
sales = spark.read.csv("sales.csv")    # 4 partitions
filtered = sales.filter(sales.amount > 100)
totals = filtered.groupBy("region").sum("amount")
totals.show()

CLUSTER:
- Driver: Machine D
- Executor 1: Machines A (Partitions 0, 1)
- Executor 2: Machine B (Partitions 2, 3)
```

---

## Normal Execution (No Failure)

```
+------------------------------------------------------------------+
|                    NORMAL EXECUTION                              |
|                                                                  |
|   STAGE 1: Read + Filter (Narrow)                                |
|   +----------------------------------------------------------+   |
|   | Executor 1 (Machine A)    | Executor 2 (Machine B)       |   |
|   | [Part 0] filter -> done   | [Part 2] filter -> done      |   |
|   | [Part 1] filter -> done   | [Part 3] filter -> done      |   |
|   +----------------------------------------------------------+   |
|                              |                                   |
|                         SHUFFLE                                  |
|                              |                                   |
|   STAGE 2: GroupBy + Sum (Wide)                                  |
|   +----------------------------------------------------------+   |
|   | Executor 1                | Executor 2                   |   |
|   | [Regions A-M] -> done     | [Regions N-Z] -> done        |   |
|   +----------------------------------------------------------+   |
|                              |                                   |
|                         RESULT                                   |
|                       -> Driver                                  |
|                                                                  |
|   Total time: 2 minutes                                          |
+------------------------------------------------------------------+
```

---

## Failure During Stage 1

Now, Executor 2 crashes while processing Stage 1:

```
+------------------------------------------------------------------+
|                    FAILURE DURING STAGE 1                        |
|                                                                  |
|   STAGE 1: Read + Filter                                         |
|   +----------------------------------------------------------+   |
|   | Executor 1 (Machine A)    | Executor 2 (Machine B)       |   |
|   | [Part 0] filter -> done   | [Part 2] filter -> running   |   |
|   | [Part 1] filter -> done   | [Part 3] filter -> pending   |   |
|   +----------------------------------------------------------+   |
|                              |           |                       |
|                              |     +-----v-----+                 |
|                              |     |   CRASH   |                 |
|                              |     | Machine B |                 |
|                              |     +-----------+                 |
|                                                                  |
|   STATUS:                                                        |
|   - Partitions 0, 1: Completed                                   |
|   - Partition 2: In progress, LOST                               |
|   - Partition 3: Never started, executor dead                    |
+------------------------------------------------------------------+
```

---

## Detection Phase

The Driver detects the failure through missing heartbeats:

```
+------------------------------------------------------------------+
|                    FAILURE DETECTION                             |
|                                                                  |
|   DRIVER                       EXECUTOR 2                        |
|   +--------+                   +--------+                        |
|   |        |                   |        |                        |
|   | Expect | <-- Heartbeat --  | Alive  |   Time T+0: Normal     |
|   |        |                   |        |                        |
|   +--------+                   +--------+                        |
|                                                                  |
|   +--------+                   +--------+                        |
|   |        |                   |  XXXX  |                        |
|   | Expect | <-- ??? -------   | CRASH  |   Time T+10s: No       |
|   | HB...  |                   |        |   heartbeat            |
|   +--------+                   +--------+                        |
|                                                                  |
|   +--------+                                                     |
|   |        |                                                     |
|   | TIMEOUT|   After 60s with no heartbeat:                      |
|   | DETECT |   "Executor 2 has failed"                           |
|   |        |                                                     |
|   +--------+                                                     |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Recovery: Request New Executor

The Driver requests a replacement executor:

```
+------------------------------------------------------------------+
|                    EXECUTOR REPLACEMENT                          |
|                                                                  |
|   DRIVER                       CLUSTER MANAGER                   |
|   +--------+                   +----------------+                |
|   |        |                   |                |                |
|   | Need 1 | -- Request -->    | Find available |                |
|   | executor|                  | node...        |                |
|   |        |                   |                |                |
|   +--------+                   +-------+--------+                |
|                                        |                         |
|                                        v                         |
|                                +----------------+                |
|                                | Machine C      |                |
|                                | (new executor) |                |
|                                | Executor 3     |                |
|                                +----------------+                |
|                                        |                         |
|                                        v                         |
|   +--------+                   +----------------+                |
|   |        | <-- Register --   | Executor 3     |                |
|   | Driver |                   | Ready!         |                |
|   |        |                   +----------------+                |
|   +--------+                                                     |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Recovery: Recompute Lost Tasks

The Driver reschedules the lost tasks:

```
+------------------------------------------------------------------+
|                    TASK RESCHEDULING                             |
|                                                                  |
|   DRIVER's VIEW:                                                 |
|   +----------------------------------------------------------+   |
|   | Task Status:                                             |   |
|   |   Part 0: COMPLETED (Executor 1)                         |   |
|   |   Part 1: COMPLETED (Executor 1)                         |   |
|   |   Part 2: FAILED (Executor 2 - dead)  -> RESCHEDULE      |   |
|   |   Part 3: FAILED (Executor 2 - dead)  -> RESCHEDULE      |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   RECOVERY EXECUTION:                                            |
|   +----------------------------------------------------------+   |
|   | Executor 1 (Machine A)    | Executor 3 (Machine C)       |   |
|   | [Part 0] done (cached)    | [Part 2] filter -> running   |   |
|   | [Part 1] done (cached)    | [Part 3] filter -> pending   |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   Lineage used:                                                  |
|   Part 2 = sales.csv[rows 500K-750K].filter(amount > 100)        |
|   Part 3 = sales.csv[rows 750K-1M].filter(amount > 100)          |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Recovery: Lineage Recomputation

The new Executor uses lineage to recompute:

```
+------------------------------------------------------------------+
|                    LINEAGE RECOMPUTATION                         |
|                                                                  |
|   FOR PARTITION 2:                                               |
|                                                                  |
|   Step 1: What is filtered[P2]?                                  |
|           -> sales[P2].filter(amount > 100)                      |
|                                                                  |
|   Step 2: What is sales[P2]?                                     |
|           -> Read rows 500K-750K from "sales.csv"                |
|                                                                  |
|   Step 3: Execute:                                               |
|           +------------------+                                   |
|           | Read sales.csv   |                                   |
|           | rows 500K-750K   |                                   |
|           +---------+--------+                                   |
|                     |                                            |
|                     v                                            |
|           +---------+--------+                                   |
|           | Filter:          |                                   |
|           | amount > 100     |                                   |
|           +---------+--------+                                   |
|                     |                                            |
|                     v                                            |
|           +---------+--------+                                   |
|           | RECOVERED!       |                                   |
|           | Partition 2      |                                   |
|           +------------------+                                   |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Recovery: Continue Execution

After recovery, the job continues:

```
+------------------------------------------------------------------+
|                    CONTINUED EXECUTION                           |
|                                                                  |
|   STAGE 1 COMPLETE:                                              |
|   +----------------------------------------------------------+   |
|   | Executor 1 (Machine A)    | Executor 3 (Machine C)       |   |
|   | [Part 0] done             | [Part 2] done (recovered)    |   |
|   | [Part 1] done             | [Part 3] done (recovered)    |   |
|   +----------------------------------------------------------+   |
|                              |                                   |
|                         SHUFFLE                                  |
|                              |                                   |
|   STAGE 2: GroupBy + Sum                                         |
|   +----------------------------------------------------------+   |
|   | Executor 1                | Executor 3                   |   |
|   | [Regions A-M] -> done     | [Regions N-Z] -> done        |   |
|   +----------------------------------------------------------+   |
|                              |                                   |
|                         RESULT                                   |
|                       -> Driver                                  |
|                                                                  |
|   Total time: 3 minutes (1 minute extra for recovery)            |
|   JOB COMPLETED SUCCESSFULLY!                                    |
+------------------------------------------------------------------+
```

---

## Complete Timeline

```
+------------------------------------------------------------------+
|                    FAILURE RECOVERY TIMELINE                     |
|                                                                  |
|   T+0:00   Job starts                                            |
|   T+0:30   Stage 1: Tasks running on Executors 1 and 2           |
|   T+0:45   Executor 2 crashes (Machine B failure)                |
|   T+1:00   Tasks 0,1 complete on Executor 1                      |
|   T+1:45   Driver detects failure (heartbeat timeout)            |
|   T+1:50   Driver requests new executor from Cluster Manager     |
|   T+2:00   Executor 3 starts on Machine C                        |
|   T+2:05   Tasks 2,3 rescheduled to Executor 3                   |
|   T+2:20   Tasks 2,3 recomputed using lineage                    |
|   T+2:30   Stage 1 complete, shuffle begins                      |
|   T+3:00   Stage 2 complete, results returned                    |
|                                                                  |
|   Without failure: ~2 minutes                                    |
|   With failure:    ~3 minutes                                    |
|   Overhead: 1 minute (detection + recovery + recomputation)      |
+------------------------------------------------------------------+
```

---

## Key Points from This Example

1. **Detection takes time:** Heartbeat timeout (typically 60s default)
2. **Executor replacement:** Cluster Manager launches new executor
3. **Lineage recomputation:** Only failed partitions are recomputed
4. **Completed work preserved:** Partitions 0,1 were not recomputed
5. **Job completes successfully:** Despite hardware failure

---

## Limits of This Approach

This recovery works because:
- Source data (CSV) was still available
- Lineage was short (just Read + Filter)
- Failure was during Stage 1 (narrow transformations)

Recovery is harder when:
- Source data is gone
- Many stages have completed (shuffle files may be lost)
- Wide dependencies mean many partitions to recompute

---

## Key Takeaways

1. **Spark detects failures via heartbeats:** Timeout triggers recovery.

2. **New executors replace dead ones:** Cluster Manager allocates.

3. **Only failed tasks are rescheduled:** Completed work is preserved.

4. **Lineage enables recomputation:** Read from source, apply transformations.

5. **Recovery adds overhead:** But job completes correctly.

6. **This is invisible to your code:** Spark handles it automatically.

---

## Additional Resources

- [Spark Fault Tolerance (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-fault-tolerance)
- [RDD Paper - Fault Tolerance Section](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)
- [Spark Application Recovery (Video)](https://www.youtube.com/watch?v=dmL0N3qfSc8)
