# Exercise: When to Use Checkpointing

## Exercise Overview
- **Duration:** 20 minutes
- **Format:** Paper-based decision analysis
- **Materials:** Pencil

## Learning Objective
Learn to identify scenarios where checkpointing is beneficial vs unnecessary.

---

## Instructions

For each scenario, decide:
1. Is checkpointing recommended?
2. If yes, where should the checkpoint be placed?
3. Justify your decision

---

## Scenario 1: Short Pipeline

```python
df = spark.read.csv("data.csv")          # 10 GB
filtered = df.filter(df.status == "active")
selected = filtered.select("id", "name", "value")
result = selected.groupBy("name").sum("value")
result.show()
```

**Decision:**

```
Checkpoint recommended?  [ ] Yes   [ ] No

If yes, after which operation? _______________________

Justification:
_________________________________________________
_________________________________________________
```

---

## Scenario 2: Machine Learning Iteration

```python
# Iterative K-means clustering
data = spark.read.parquet("features.parquet")  # 500 GB
centers = initialize_centers()

for i in range(100):
    # Assign points to nearest center
    assignments = data.map(lambda p: (nearest(p, centers), p))
    
    # Compute new centers
    new_centers = assignments.reduceByKey(average)
    
    centers = new_centers.collect()
```

**Decision:**

```
Checkpoint recommended?  [ ] Yes   [ ] No

If yes, how often? _______________________________

If yes, checkpoint which RDD? _____________________

Justification:
_________________________________________________
_________________________________________________
```

---

## Scenario 3: Multi-Stage ETL

```python
# Stage 1: Read and clean (30 minutes)
raw = spark.read.json("raw_events/")            # 2 TB
cleaned = raw.filter(valid).select(columns)
standardized = cleaned.transform(normalize)

# Stage 2: Aggregate (45 minutes)
by_user = standardized.groupBy("user_id").agg(...)

# Stage 3: Join with dimensions (20 minutes)
enriched = by_user.join(user_dims, "user_id")
enriched = enriched.join(product_dims, "product_id")

# Stage 4: Final aggregation (15 minutes)
result = enriched.groupBy("category").agg(...)
result.write.parquet("output/")
```

**Decision:**

```
Checkpoint recommended?  [ ] Yes   [ ] No

If yes, after which stage(s)? _____________________

Justification:
_________________________________________________
_________________________________________________
```

---

## Scenario 4: Interactive Analysis

```python
# User is exploring data interactively
df = spark.read.parquet("sales.parquet")

# Query 1
df.filter(df.year == 2024).count()

# Query 2
df.filter(df.year == 2024).groupBy("region").sum("amount").show()

# Query 3
df.filter(df.year == 2024).groupBy("product").count().orderBy("count").show()
```

**Decision:**

```
Checkpoint recommended?  [ ] Yes   [ ] No

Alternative recommendation: _______________________

Justification:
_________________________________________________
_________________________________________________
```

---

## Scenario 5: Graph Processing

```python
# PageRank algorithm (iterative)
edges = spark.read.parquet("web_graph/")       # 100 GB
ranks = edges.map(lambda e: (e.src, 1.0))       # Initial ranks

for i in range(50):
    contributions = edges.join(ranks).map(contribute)
    ranks = contributions.reduceByKey(add).mapValues(lambda x: 0.15 + 0.85 * x)

final_ranks = ranks.collect()
```

**Decision:**

```
Checkpoint recommended?  [ ] Yes   [ ] No

If yes, checkpoint every _____ iterations

Checkpoint which RDD? _______________________________

Why is checkpointing ESSENTIAL here (not just recommended)?
_________________________________________________
_________________________________________________
```

---

## Decision Framework

Fill in this decision tree for when to checkpoint:

```
Should I checkpoint?

1. Is the lineage very long (>10 transformations)?
   [ ] Yes -> Consider checkpoint
   [ ] No -> Probably not needed

2. Is there an iterative algorithm?
   [ ] Yes -> Checkpoint every N iterations
   [ ] No -> Continue to question 3

3. Is there an expensive shuffle I don't want to redo?
   [ ] Yes -> Checkpoint after the shuffle
   [ ] No -> Continue to question 4

4. Will I reuse this RDD multiple times?
   [ ] Yes -> Consider cache() instead
   [ ] No -> Checkpoint probably not needed

5. Can source data become unavailable?
   [ ] Yes -> Checkpoint to preserve data
   [ ] No -> Rely on lineage
```

---

## Trade-off Analysis

Complete this table:

| Factor | Lineage Only | With Checkpoint |
|--------|--------------|-----------------|
| Storage cost | None | _____________ |
| Write overhead | None | _____________ |
| Recovery time (short lineage) | Fast | _____________ |
| Recovery time (long lineage) | _____________ | Fast |
| Memory usage | _____________ | _____________ |

---

## Scenario 6: Design Your Own

**You have:**
- 1 TB of source data
- A pipeline with: 2 shuffles, 8 narrow transformations, 1 iterative loop (20 iterations)
- The job takes 2 hours without failures
- Failure probability: 5% per hour

**Design your checkpointing strategy:**

```
Checkpoint 1 location: ____________________________
Reason: _________________________________________

Checkpoint 2 location: ____________________________
Reason: _________________________________________

Expected recovery time with this strategy: __________

Expected recovery time without checkpointing: _______
```

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Scenario 1:** No checkpoint needed
- Pipeline is short (4 operations)
- Recovery would be fast anyway
- Adding checkpoint overhead is not worth it

**Scenario 2:** Yes, checkpoint every 10 iterations
- Checkpoint the `assignments` or `new_centers` RDD
- Without checkpoint, iteration 100 has lineage back through all 99 previous iterations
- Recovery would redo the entire algorithm!

**Scenario 3:** Yes, checkpoint after Stage 2 (after aggregation)
- This is after the expensive 30+45 = 75 minutes of work
- If Stage 3 or 4 fails, only redo 20+15 = 35 minutes
- Alternative: also checkpoint after Stage 3 joins

**Scenario 4:** No checkpoint, use CACHE instead
- Interactive analysis benefits from caching for repeated queries
- Checkpoint is for fault tolerance, cache is for reuse
- Cache the filtered 2024 data

**Scenario 5:** ESSENTIAL - checkpoint every 5-10 iterations
- Lineage grows with EVERY iteration
- After 50 iterations, lineage is 50 iterations deep
- Recovery = redo entire algorithm
- Must checkpoint to truncate lineage

**Trade-off Table:**
| Factor | Lineage Only | With Checkpoint |
|--------|--------------|-----------------|
| Storage cost | None | Checkpoint file size |
| Write overhead | None | Time to write checkpoint |
| Recovery time (short) | Fast | Slower (disk read) |
| Recovery time (long) | SLOW | Fast |
| Memory usage | Lower | May cache + checkpoint |

</details>
