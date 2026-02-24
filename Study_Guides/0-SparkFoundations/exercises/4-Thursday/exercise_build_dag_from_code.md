# Exercise: Build DAG from Pseudocode

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based diagram drawing
- **Materials:** Pencil, paper

## Learning Objective
Practice building Directed Acyclic Graphs (DAGs) from code and identifying stage boundaries.

---

## Instructions

For each code snippet:
1. Draw the DAG (boxes for operations, arrows for dependencies)
2. Identify stage boundaries (where shuffles occur)
3. Number the stages
4. Count the total stages

---

## Exercise 1: Simple Pipeline

```python
logs = spark.read.text("access_logs.txt")
errors = logs.filter(logs.value.contains("ERROR"))
parsed = errors.select(split(errors.value, " ").alias("parts"))
timestamps = parsed.select(parsed.parts[0].alias("timestamp"))
result = timestamps.count()
```

**Draw the DAG:**

```
+-------------+
|             |
|             |
+------+------+
       |
       v
[Continue drawing below...]
       
       
       
       
       
```

**Questions:**

1. How many stages does this DAG have? _____

2. Are there any shuffles? If yes, where?

```
_________________________________________________
```

3. Which operations are pipelined together (same stage)?

```
_________________________________________________
```

---

## Exercise 2: Aggregation Pipeline

```python
sales = spark.read.parquet("sales.parquet")
filtered = sales.filter(sales.year == 2024)
selected = filtered.select("product", "region", "amount")
by_product = selected.groupBy("product").agg(sum("amount").alias("total"))
sorted_result = by_product.orderBy(desc("total"))
top_10 = sorted_result.limit(10)
```

**Draw the DAG with stage boundaries:**

```
STAGE 1:
+-------------+
|             |
|             |
+------+------+
       |
       v
       .
       .
       .

(Mark stage boundaries with === SHUFFLE ===)
```

**Questions:**

1. How many stages? _____

2. Where are the stage boundaries? List the operations before each shuffle:

```
Stage 1: _________________________________________________

SHUFFLE

Stage 2: _________________________________________________

SHUFFLE (if applicable)

Stage 3: _________________________________________________
```

3. What causes each stage boundary?

```
Boundary 1: ____________________________________________

Boundary 2 (if applicable): _____________________________
```

---

## Exercise 3: Join with Aggregation

```python
customers = spark.read.csv("customers.csv")
orders = spark.read.csv("orders.csv")

active_customers = customers.filter(customers.status == "active")
large_orders = orders.filter(orders.total > 1000)

joined = active_customers.join(large_orders, "customer_id")

summary = joined.groupBy("customer_id").agg(
    count("order_id").alias("order_count"),
    sum("total").alias("total_spent")
)

result = summary.orderBy(desc("total_spent")).limit(100)
```

**Draw the DAG:**

Note: This has TWO input sources. Draw them side by side, then show where they merge.

```
     +------------+                +------------+
     | customers  |                |   orders   |
     +-----+------+                +-----+------+
           |                             |
           v                             v
     +------------+                +------------+
     |            |                |            |
     +-----+------+                +-----+------+
           |                             |
           +-------------+---------------+
                         |
                         v
                   [Continue...]


```

**Questions:**

1. How many stages? _____

2. How many shuffles? _____

3. Can the two filter operations run in parallel? Why?

```
_________________________________________________
```

4. What happens at the join? Explain the shuffle:

```
_________________________________________________
_________________________________________________
```

---

## Exercise 4: Complex Pipeline

```python
events = spark.read.json("events.json")

page_views = events.filter(events.type == "page_view")
clicks = events.filter(events.type == "click")

pv_by_user = page_views.groupBy("user_id").count().withColumnRenamed("count", "views")
clicks_by_user = clicks.groupBy("user_id").count().withColumnRenamed("count", "clicks")

combined = pv_by_user.join(clicks_by_user, "user_id", "outer")

result = combined.fillna(0).orderBy(desc("views")).limit(50)
```

**Draw the DAG (note: same source, two branches that rejoin):**

```
                    +-------------+
                    |   events    |
                    +------+------+
                           |
              +------------+------------+
              |                         |
              v                         v
        +-----+-----+             +-----+-----+
        |           |             |           |
        +-----------+             +-----------+
              |                         |
              .                         .
              .                         .
              .                         .
              
[Draw the complete DAG with both branches]
```

**Questions:**

1. How many stages? _____

2. How many shuffles? _____

3. Can Stage 1 for page_views run at the same time as Stage 1 for clicks?

```
_________________________________________________
```

4. If you wanted to reduce shuffles, what might you change?

```
_________________________________________________
_________________________________________________
```

---

## Summary

Fill in the table:

| Exercise | Stages | Shuffles | Wide Transformations |
|----------|--------|----------|----------------------|
| 1 | | | |
| 2 | | | |
| 3 | | | |
| 4 | | | |

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Exercise 1:**
- 1 stage (no shuffles)
- count() is an action, not a shuffle transformation
- All operations pipelined: read -> filter -> select -> select

**Exercise 2:**
- 3 stages
- Stage 1: read -> filter -> select
- SHUFFLE (groupBy)
- Stage 2: aggregate (sum)
- SHUFFLE (orderBy)
- Stage 3: sort -> limit

**Exercise 3:**
- 4 stages (both inputs, join, final agg)
- 3 shuffles (both sides of join, then groupBy, then orderBy)
- Yes, the two filters can run in parallel (independent branches)
- At join: both datasets shuffled to colocate matching customer_ids

**Exercise 4:**
- 5+ stages (complex)
- 4+ shuffles (2 groupBys, 1 join, 1 orderBy)
- Yes, the two groupBy stages can run in parallel (independent branches)
- Could use a single pass with conditional aggregation instead of two branches

</details>
