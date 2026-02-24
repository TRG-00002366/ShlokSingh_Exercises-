# Exercise: Label the Cluster Diagram

## Exercise Overview
- **Duration:** 20 minutes
- **Format:** Paper-based diagram labeling
- **Materials:** Pencil, colored pencils (optional)

## Learning Objective
Demonstrate understanding of Spark cluster components by correctly labeling and describing each part.

---

## Instructions

1. Label each numbered component in the diagram
2. Draw arrows showing the communication flow
3. Write a one-sentence description of each component's role

---

## The Diagram

Label components 1-8 and draw communication arrows:

```
                    +---------------+
                    |       1       |
                    |   _________   |
                    +-------+-------+
                            |
                            | (A)
                            v
                    +---------------+
                    |       2       |
                    |   _________   |
                    +-------+-------+
                            |
            +---------------+---------------+
            |               |               |
            v               v               v
    +---------------+ +---------------+ +---------------+
    |       3       | |       3       | |       3       |
    |   _________   | |   _________   | |   _________   |
    |               | |               | |               |
    | +-----------+ | | +-----------+ | | +-----------+ |
    | |     4     | | | |     4     | | | |     4     | |
    | | _________ | | | | _________ | | | | _________ | |
    | |           | | | |           | | | |           | |
    | | +-------+ | | | | +-------+ | | | | +-------+ | |
    | | |   5   | | | | | |   5   | | | | | |   5   | | |
    | | |_______| | | | | |_______| | | | | |_______| | |
    | +-----------+ | | +-----------+ | | +-----------+ |
    +---------------+ +---------------+ +---------------+
            ^               ^               ^
            |               |               |
            +-------+-------+-------+-------+
                    |       (B)
                    v
            +---------------+
            |       6       |
            |   _________   |
            +---------------+
```

---

## Part 1: Label the Components

Write the name of each component:

| Number | Component Name |
|--------|----------------|
| 1 | __________________ |
| 2 | __________________ |
| 3 | __________________ |
| 4 | __________________ |
| 5 | __________________ |
| 6 | __________________ |

---

## Part 2: Describe the Arrows

What communication happens at each arrow?

| Arrow | Direction | What is being communicated? |
|-------|-----------|----------------------------|
| (A) | 1 -> 2 | __________________________ |
| (A) | 2 -> 1 | __________________________ |
| (B) | 4 -> 6 | __________________________ |
| (B) | 6 -> 4 | __________________________ |

---

## Part 3: Component Descriptions

Write ONE sentence describing what each component does:

**Component 1:**
```
_________________________________________________
```

**Component 2:**
```
_________________________________________________
```

**Component 3:**
```
_________________________________________________
```

**Component 4:**
```
_________________________________________________
```

**Component 5:**
```
_________________________________________________
```

**Component 6:**
```
_________________________________________________
```

---

## Part 4: Scenario Questions

4.1 If Component 1 crashes, what happens to the job?

```
_________________________________________________
```

4.2 If one of the Component 4s crashes, what happens?

```
_________________________________________________
```

4.3 Which component decides where Component 4s run?

```
_________________________________________________
```

4.4 Where does your main() function execute?

```
_________________________________________________
```

4.5 Where does the filter() operation actually process data?

```
_________________________________________________
```

---

## Part 5: Draw the Missing Arrows

On the diagram above, draw arrows to show:
- Heartbeats from Executors to Driver (use dashed lines)
- Task results from Executors to Driver (use solid lines)
- Shuffle data between Executors (use wavy lines)

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Part 1: Component Names**
| Number | Component Name |
|--------|----------------|
| 1 | Driver |
| 2 | Cluster Manager |
| 3 | Worker Node |
| 4 | Executor |
| 5 | Task |
| 6 | Data Source (Storage) |

**Part 2: Arrow Communications**
| Arrow | Direction | What is being communicated? |
|-------|-----------|----------------------------|
| (A) | 1 -> 2 | Resource requests (need X executors) |
| (A) | 2 -> 1 | Executor allocation confirmation |
| (B) | 4 -> 6 | Data read requests |
| (B) | 6 -> 4 | Data being read to executor |

**Part 3: Descriptions**
- Component 1 (Driver): Coordinates the job, builds the DAG, schedules tasks, and collects results.
- Component 2 (Cluster Manager): Allocates resources across the cluster and launches executors.
- Component 3 (Worker Node): Physical machine that hosts executor processes.
- Component 4 (Executor): JVM process that runs tasks and caches data.
- Component 5 (Task): Single unit of work processing one partition.
- Component 6 (Data Source): External storage like HDFS, S3, or databases.

**Part 4: Scenario Answers**
- 4.1: The entire job fails (Driver is single point of failure)
- 4.2: Tasks are rescheduled to other executors, job continues
- 4.3: Cluster Manager
- 4.4: Driver
- 4.5: Executors (specifically in Tasks)

</details>
