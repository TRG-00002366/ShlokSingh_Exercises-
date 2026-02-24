# Exercise: Trace Failure Recovery

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based scenario analysis
- **Materials:** Pencil

## Learning Objective
Understand how Spark recovers from failures using lineage by tracing through recovery scenarios.

---

## Instructions

For each failure scenario:
1. Identify what was lost
2. Trace the lineage back to find recoverable data
3. Describe the recovery process
4. Estimate the recovery cost

---

## Scenario 1: Simple Pipeline Failure

**The Pipeline:**
```
Stage 1:
    source = read("data.csv")           # 4 partitions
    filtered = source.filter(x > 100)
    mapped = filtered.map(transform)

Stage 2 (after shuffle):
    grouped = mapped.groupBy("key").sum("value")
```

**The Failure:**
- Executor 2 crashes during Stage 1
- Executor 2 was processing partition 1 of `mapped`
- Partitions 0, 2, 3 are complete

**Questions:**

1.1 What data is lost?

```
_________________________________________________
```

1.2 Write the lineage for the lost partition:

```
mapped[partition 1] = ___________________________

filtered[partition 1] = _________________________

source[partition 1] = ___________________________
```

1.3 What does Spark do to recover?

```
Step 1: ________________________________________

Step 2: ________________________________________

Step 3: ________________________________________
```

1.4 Is the source data still available? Why?

```
_________________________________________________
```

---

## Scenario 2: Failure After Shuffle

**The Pipeline:**
```
Stage 1: read -> filter -> map (4 partitions)
         SHUFFLE
Stage 2: groupBy -> aggregate (4 partitions)
```

**The Situation:**
- Stage 1 completed successfully
- Shuffle files written to disk on all executors
- Executor 3 crashes during Stage 2
- Executor 3 had partition 2 of the grouped data

**Questions:**

2.1 Is the Stage 1 work lost?

```
[ ] Yes   [ ] No

Explain: _______________________________________
```

2.2 What needs to be recomputed?

```
_________________________________________________
```

2.3 What is the best case recovery?

```
_________________________________________________
```

2.4 What is the worst case recovery (if shuffle files are also lost)?

```
_________________________________________________
```

---

## Scenario 3: Wide Dependency Challenge

**The Pipeline:**
```
source: 1000 partitions
    |
    v
filtered: narrow (same 1000 partitions)
    |
    v
grouped: WIDE shuffle (100 partitions)
    |
    v
result: narrow (100 partitions)
```

**The Failure:**
- `result` partition 50 is lost
- `grouped` partition 50 is also lost (same executor)
- `filtered` was not cached

**Questions:**

3.1 To recover `grouped[50]`, what data is needed?

```
[ ] Only filtered[50]
[ ] Multiple filtered partitions
[ ] All 1000 filtered partitions

Explain: _______________________________________
```

3.2 Since `filtered` was not cached, what happens?

```
_________________________________________________
_________________________________________________
```

3.3 How many source partitions must be re-read?

```
_____ partitions

Explain: _______________________________________
```

3.4 What could have reduced this recovery cost?

```
_________________________________________________
```

---

## Scenario 4: Tracing the Lineage Graph

**Draw the lineage for this pipeline:**

```python
A = read("file.csv")           # Source
B = A.filter(condition1)        # Narrow
C = A.filter(condition2)        # Narrow (same source, different filter)
D = B.union(C)                  # Narrow
E = D.groupBy("key").count()    # Wide
F = E.orderBy("count")          # Wide
```

**Lineage Graph:**
```
Draw the complete lineage graph:

      +---+
      | A |  <- source
      +---+
       / \
      /   \
     v     v
  +---+   +---+
  | B |   | C |
  +---+   +---+
     \     /
      \   /
       v v
      +---+
      | D |
      +---+
        |
  ======|====== (shuffle)
        |
      +---+
      | E |
      +---+
        |
  ======|====== (shuffle)
        |
      +---+
      | F |
      +---+
```

**If F partition 0 is lost, trace back the recovery:**

4.1 What is needed to recover F[0]?

```
F[0] depends on: _______________________________
```

4.2 What is needed for E[0]?

```
E[0] depends on: _______________________________
```

4.3 How far back might recovery need to go?

```
_________________________________________________
```

---

## Scenario 5: Recovery Cost Analysis

**The Pipeline:**

```
source (1 TB) -> T1 -> T2 -> T3 -> T4 -> T5 -> result
                 10 min  5 min  20 min  15 min  10 min
```

Each transformation takes the time shown.

**Without Checkpoint:**

5.1 If `result` is lost, how long to recover?

```
Recovery time: _____ minutes

Calculation: ____________________________________
```

**With Checkpoint after T3:**

5.2 If `result` is lost, how long to recover?

```
Recovery time: _____ minutes

Calculation: ____________________________________
```

5.3 How much time was saved?

```
_____ minutes saved
```

5.4 When would you recommend checkpointing?

```
_________________________________________________
_________________________________________________
```

---

## Summary Questions

1. What makes lineage-based recovery possible?

```
_________________________________________________
```

2. Why is wide dependency recovery more expensive than narrow?

```
_________________________________________________
_________________________________________________
```

3. In what situation would checkpointing be essential?

```
_________________________________________________
```

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Scenario 1:**
- Lost: mapped partition 1
- Lineage: mapped[1] = filtered[1].map(); filtered[1] = source[1].filter(); source[1] = read("data.csv")[rows for partition 1]
- Recovery: Re-read source partition 1, apply filter, apply map
- Yes, source data is available (CSV file still exists on storage)

**Scenario 2:**
- No, Stage 1 work is in shuffle files on disk
- Only Stage 2 partition 2 needs recomputation
- Best case: Read shuffle files, recompute only partition 2 of Stage 2
- Worst case: If shuffle files are lost, must redo Stage 1 too

**Scenario 3:**
- All 1000 filtered partitions needed (wide dependency)
- Must recompute all of filtered from source
- All 1000 source partitions must be re-read
- Checkpointing after the shuffle would have reduced recovery cost

**Scenario 4:**
- F[0] depends on E (all partitions, after shuffle sort)
- E[0] depends on D (multiple partitions for that key range)
- Recovery might need to go all the way back to A (source)

**Scenario 5:**
- Without checkpoint: 10+5+20+15+10 = 60 minutes
- With checkpoint: 15+10 = 25 minutes
- Saved: 35 minutes
- Checkpoint when transformation chain is long or after expensive shuffles

</details>
