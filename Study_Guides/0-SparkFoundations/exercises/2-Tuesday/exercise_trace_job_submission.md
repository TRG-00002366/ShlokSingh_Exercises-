# Exercise: Trace Job Submission

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based flowchart creation
- **Materials:** Pencil, paper

## Learning Objective
Understand the complete lifecycle of a Spark job by tracing each step from submission to result.

---

## Instructions

Given the job description below, create a flowchart showing:
1. Each step from spark-submit to final result
2. Which component handles each step
3. Arrows showing the flow of control

---

## The Job

```
Command: spark-submit my_job.py

Job code (pseudocode):
  1. Read "sales.csv" (1 GB, 8 partitions)
  2. Filter: amount > 100
  3. Group by: region
  4. Count per region
  5. Show results
```

---

## Part 1: Order the Steps

Put these steps in the correct order (1-12):

| Step Description | Order | Component |
|------------------|-------|-----------|
| Executors read partitions of sales.csv | ___ | __________ |
| Driver receives results and displays them | ___ | __________ |
| Cluster Manager allocates executors | ___ | __________ |
| Shuffle data (groupBy requires data movement) | ___ | __________ |
| spark-submit starts the Driver process | ___ | __________ |
| Driver creates DAG from transformations | ___ | __________ |
| Executors run filter tasks (Stage 1) | ___ | __________ |
| Driver requests executors from Cluster Manager | ___ | __________ |
| Driver divides DAG into stages | ___ | __________ |
| Executors run count tasks (Stage 2) | ___ | __________ |
| Driver sends tasks to executors | ___ | __________ |
| Executors register with Driver | ___ | __________ |

---

## Part 2: Draw the Flowchart

Using the steps above, draw a flowchart in this space. Include:
- Boxes for each component (Driver, Cluster Manager, Executors)
- Arrows showing flow
- Labels describing what happens

```
START: spark-submit









                    [Your flowchart here]









END: Results displayed
```

---

## Part 3: Stage Identification

For this job, identify the stages:

**Stage 1 includes which operations?**
```
_________________________________________________
```

**Stage 2 includes which operations?**
```
_________________________________________________
```

**What causes the stage boundary?**
```
_________________________________________________
```

---

## Part 4: Task Count

Given: 8 partitions in the input file

**How many tasks in Stage 1?**
```
Answer: _____ tasks

Explanation:
_________________________________________________
```

**How many tasks in Stage 2 (assuming 8 output partitions)?**
```
Answer: _____ tasks

Explanation:
_________________________________________________
```

---

## Part 5: Timeline

Estimate the order of operations in time:

```
TIME  |  DRIVER          |  CLUSTER MANAGER  |  EXECUTORS
------|------------------|-------------------|-------------
T+0   |                  |                   |
T+1   |                  |                   |
T+2   |                  |                   |
T+3   |                  |                   |
T+4   |                  |                   |
T+5   |                  |                   |
T+6   |                  |                   |
T+7   |                  |                   |
```

Fill in what each component is doing at each time step.

---

## Part 6: Failure Scenarios

**Scenario A:** Executor 3 crashes during Stage 1 filter operation.

What happens?
```
1. Detection: ____________________________________________
2. Recovery action: ______________________________________
3. Outcome: _____________________________________________
```

**Scenario B:** Driver crashes after Stage 1 completes but before Stage 2.

What happens?
```
1. Detection: ____________________________________________
2. Recovery action: ______________________________________
3. Outcome: _____________________________________________
```

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Part 1: Correct Order**
| Step Description | Order | Component |
|------------------|-------|-----------|
| Executors read partitions of sales.csv | 7 | Executors |
| Driver receives results and displays them | 12 | Driver |
| Cluster Manager allocates executors | 3 | Cluster Manager |
| Shuffle data (groupBy requires data movement) | 9 | Executors |
| spark-submit starts the Driver process | 1 | Driver |
| Driver creates DAG from transformations | 5 | Driver |
| Executors run filter tasks (Stage 1) | 8 | Executors |
| Driver requests executors from Cluster Manager | 2 | Driver |
| Driver divides DAG into stages | 6 | Driver |
| Executors run count tasks (Stage 2) | 10 | Executors |
| Driver sends tasks to executors | 7 | Driver |
| Executors register with Driver | 4 | Executors |

**Part 3: Stages**
- Stage 1: Read CSV, Filter (narrow transformations)
- Stage 2: GroupBy, Count (after shuffle)
- Stage boundary caused by: groupBy (wide transformation requiring shuffle)

**Part 4: Task Count**
- Stage 1: 8 tasks (one per input partition)
- Stage 2: 8 tasks (one per output partition)

**Part 6: Failure Scenarios**
- Scenario A: Detection via missed heartbeats. Recovery: reschedule failed tasks to other executors. Outcome: job completes successfully with slight delay.
- Scenario B: Detection via Executor disconnect. Recovery: none possible without checkpointing. Outcome: job fails completely, must restart.

</details>
