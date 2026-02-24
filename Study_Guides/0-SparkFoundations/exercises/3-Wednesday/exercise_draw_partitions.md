# Exercise: Draw Data Partitions

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based drawing exercise
- **Materials:** Pencil, colored pencils (optional)

## Learning Objective
Visualize how data is split into partitions and distributed across executors.

---

## Instructions

For each scenario, draw how the data would be partitioned and distributed across the given number of executors.

---

## Scenario 1: Even Distribution

**Setup:**
- Dataset: 1,000 rows of customer data
- Cluster: 4 executors
- Partitions: 4

**Task:** Draw how the data is distributed.

```
HINT: Each partition should have _____ rows

EXECUTOR 1          EXECUTOR 2          EXECUTOR 3          EXECUTOR 4
+-------------+     +-------------+     +-------------+     +-------------+
|             |     |             |     |             |     |             |
| Partition _ |     | Partition _ |     | Partition _ |     | Partition _ |
|             |     |             |     |             |     |             |
| Rows: _____ |     | Rows: _____ |     | Rows: _____ |     | Rows: _____ |
|             |     |             |     |             |     |             |
+-------------+     +-------------+     +-------------+     +-------------+
```

**Question:** Is this an ideal partition distribution? Why?

```
_________________________________________________
```

---

## Scenario 2: More Partitions Than Executors

**Setup:**
- Dataset: 1,200 rows
- Cluster: 3 executors
- Partitions: 6

**Task:** Draw how 6 partitions fit on 3 executors.

```
EXECUTOR 1              EXECUTOR 2              EXECUTOR 3
+----------------+      +----------------+      +----------------+
|                |      |                |      |                |
| Partition ___  |      | Partition ___  |      | Partition ___  |
| Rows: _____    |      | Rows: _____    |      | Rows: _____    |
|                |      |                |      |                |
| Partition ___  |      | Partition ___  |      | Partition ___  |
| Rows: _____    |      | Rows: _____    |      | Rows: _____    |
|                |      |                |      |                |
+----------------+      +----------------+      +----------------+
```

**Question:** How many partitions does each executor handle?

```
_________________________________________________
```

**Question:** If each executor has 2 cores, how many tasks run in parallel?

```
_________________________________________________
```

---

## Scenario 3: Data Skew Problem

**Setup:**
- Dataset: Sales by region
- Partitions by region key: 4 partitions
- Data distribution:
  - Region "North": 100 rows
  - Region "South": 100 rows
  - Region "East": 50 rows
  - Region "West": 750 rows (75% of data!)

**Task:** Draw the skewed distribution.

```
EXECUTOR 1          EXECUTOR 2          EXECUTOR 3          EXECUTOR 4
+-------------+     +-------------+     +-------------+     +-------------+
| Region:     |     | Region:     |     | Region:     |     | Region:     |
| _________   |     | _________   |     | _________   |     | _________   |
|             |     |             |     |             |     |             |
| Rows: _____ |     | Rows: _____ |     | Rows: _____ |     | Rows: _____ |
|             |     |             |     |             |     |             |
| [draw size] |     | [draw size] |     | [draw size] |     | [draw size] |
+-------------+     +-------------+     +-------------+     +-------------+
```

**Hint:** Draw the partition boxes proportional to data size (West should be much larger).

**Question:** If processing takes 1 second per 100 rows, how long does each executor take?

| Executor | Region | Rows | Processing Time |
|----------|--------|------|-----------------|
| 1 | | | seconds |
| 2 | | | seconds |
| 3 | | | seconds |
| 4 | | | seconds |

**Question:** What is the total job time? (Remember: job waits for slowest)

```
_________________________________________________
```

**Question:** What would ideal distribution look like for 1,000 rows across 4 partitions?

```
_________________________________________________
```

---

## Scenario 4: After a Filter Operation

**Setup:**
- Original: 4 partitions, 1,000 rows each (4,000 total)
- Filter: Keep only where status = "ACTIVE"
- Result: Only 10% of rows are ACTIVE

**Before Filter:**
```
Partition 0: 1,000 rows
Partition 1: 1,000 rows
Partition 2: 1,000 rows
Partition 3: 1,000 rows
```

**Task:** Draw the partitions after filtering:

```
AFTER FILTER (assuming even distribution of ACTIVE):

Partition 0: _____ rows
Partition 1: _____ rows
Partition 2: _____ rows
Partition 3: _____ rows
```

**Question:** Are these partitions now too small, too large, or just right?

```
_________________________________________________
```

**Question:** What might you do to improve this? (Hint: repartition)

```
_________________________________________________
```

---

## Scenario 5: Reading Multiple Files

**Setup:**
- 3 input files:
  - file1.csv: 500 MB
  - file2.csv: 300 MB
  - file3.csv: 200 MB
- Default partition size: 128 MB

**Task:** Calculate how many partitions each file becomes:

| File | Size | Partitions (Size / 128 MB, rounded up) |
|------|------|----------------------------------------|
| file1.csv | 500 MB | _____ partitions |
| file2.csv | 300 MB | _____ partitions |
| file3.csv | 200 MB | _____ partitions |
| **TOTAL** | | _____ partitions |

**Question:** If you have 5 executors with 2 cores each (10 cores), is this partition count good?

```
_________________________________________________
```

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Scenario 1:**
- Each partition: 250 rows
- Yes, ideal because partitions are equal sized

**Scenario 2:**
- Each executor handles 2 partitions
- 6 tasks total, but only 6 cores across 3 executors can run at once
- With 2 cores per executor, 6 tasks run in parallel

**Scenario 3:**
- Executor 1 (North): 100 rows, 1 second
- Executor 2 (South): 100 rows, 1 second
- Executor 3 (East): 50 rows, 0.5 seconds
- Executor 4 (West): 750 rows, 7.5 seconds
- Total job time: 7.5 seconds (waits for slowest)
- Ideal: 250 rows per partition

**Scenario 4:**
- Each partition: ~100 rows (10% of 1000)
- Partitions are now very small (only 100 rows each)
- Could repartition/coalesce to fewer, larger partitions

**Scenario 5:**
- file1.csv: 4 partitions
- file2.csv: 3 partitions
- file3.csv: 2 partitions
- Total: 9 partitions
- With 10 cores, 9 partitions is slightly underfilling resources (could use 10)

</details>
