# Exercise: Identify Shuffle Operations

## Exercise Overview
- **Duration:** 25 minutes
- **Format:** Paper-based code analysis
- **Materials:** Pencil, highlighter (optional)

## Learning Objective
Learn to identify which operations cause shuffles by analyzing pseudocode.

---

## Instructions

For each code snippet:
1. Circle or highlight operations that cause a SHUFFLE
2. Explain WHY that operation requires a shuffle
3. Count the total number of shuffles

---

## Code Snippet 1: Basic Pipeline

```python
# Read data
df = spark.read.csv("transactions.csv")

# Process
result = df.filter(df.amount > 100)           # LINE A
             .select("customer", "amount")     # LINE B
             .groupBy("customer")              # LINE C
             .sum("amount")                    # LINE D
             .orderBy("sum(amount)")           # LINE E
             .limit(10)                        # LINE F
```

**Circle the shuffle operations:** (write the line letters)

```
Shuffle at line(s): _____________________
```

**Explain each shuffle:**

```
Line ___: Shuffle because _______________________________________

Line ___: Shuffle because _______________________________________
```

**Total shuffles:** _____

---

## Code Snippet 2: Join Operations

```python
# Two datasets
customers = spark.read.parquet("customers")
orders = spark.read.parquet("orders")

# Processing
customers_filtered = customers.filter(customers.active == True)    # LINE A
orders_2024 = orders.filter(orders.year == 2024)                   # LINE B

joined = customers_filtered.join(orders_2024, "customer_id")       # LINE C

result = joined.select("name", "order_total")                       # LINE D
               .groupBy("name")                                     # LINE E
               .agg(sum("order_total"))                             # LINE F
```

**Circle the shuffle operations:** (write the line letters)

```
Shuffle at line(s): _____________________
```

**Explain each shuffle:**

```
Line ___: Shuffle because _______________________________________

Line ___: Shuffle because _______________________________________
```

**Total shuffles:** _____

---

## Code Snippet 3: Multiple Aggregations

```python
df = spark.read.json("events.json")

# Various aggregations
by_user = df.groupBy("user_id").count()                            # LINE A
by_event = df.groupBy("event_type").count()                        # LINE B
by_both = df.groupBy("user_id", "event_type").count()              # LINE C

# Combine results
combined = by_user.union(by_event)                                  # LINE D

# Final output
final = combined.distinct()                                         # LINE E
```

**Circle the shuffle operations:** (write the line letters)

```
Shuffle at line(s): _____________________
```

**Total shuffles:** _____

**Question:** Why does `distinct()` cause a shuffle?

```
_________________________________________________
_________________________________________________
```

---

## Code Snippet 4: Tricky Cases

```python
df = spark.read.csv("data.csv")

# Case 1: coalesce vs repartition
small_df = df.filter(df.value > 1000)
coalescedDF = small_df.coalesce(2)                                  # LINE A
repartitionedDF = small_df.repartition(2)                           # LINE B

# Case 2: reduceByKey vs groupByKey (RDD equivalent)
pairs = df.rdd.map(lambda x: (x.key, x.value))
reduced = pairs.reduceByKey(lambda a, b: a + b)                     # LINE C
grouped = pairs.groupByKey()                                        # LINE D
```

**Which lines cause shuffles?**

| Line | Causes Shuffle? | Explanation |
|------|-----------------|-------------|
| A | Yes / No | |
| B | Yes / No | |
| C | Yes / No | |
| D | Yes / No | |

**Question:** Why is `reduceByKey` more efficient than `groupByKey` even though both shuffle?

```
_________________________________________________
_________________________________________________
```

---

## Code Snippet 5: Optimization Opportunity

```python
# ORIGINAL CODE (not optimized)
df = spark.read.parquet("sales")

step1 = df.groupBy("region").sum("amount")                          # Shuffle 1
step2 = step1.orderBy("sum(amount)")                                # Shuffle 2
step3 = step2.filter(step2["sum(amount)"] > 10000)                  # No shuffle
result = step3.limit(5)                                             # No shuffle
```

**Question:** Can we reduce the number of shuffles? Rewrite to use fewer shuffles:

```python
# YOUR OPTIMIZED CODE:
df = spark.read.parquet("sales")

# Hint: Can you filter before grouping?

_________________________________________________
_________________________________________________
_________________________________________________
_________________________________________________
```

**Explain your optimization:**

```
_________________________________________________
_________________________________________________
```

---

## Summary: Shuffle Classification

Classify each operation:

| Operation | Causes Shuffle? (Yes/No) |
|-----------|-------------------------|
| filter() | |
| map() | |
| select() | |
| groupBy() | |
| orderBy() / sort() | |
| join() | |
| union() | |
| distinct() | |
| repartition() | |
| coalesce() | |
| reduceByKey() | |
| count() (action) | |

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Snippet 1:**
- Shuffles at: C (groupBy), E (orderBy)
- Total: 2 shuffles
- Line C: Shuffle to group all rows with same customer together
- Line E: Shuffle for global sorting

**Snippet 2:**
- Shuffles at: C (join), E (groupBy)
- Total: 2 shuffles
- Line C: Both sides shuffled to colocate matching customer_ids
- Line E: Shuffle to group by name

**Snippet 3:**
- Shuffles at: A, B, C (all groupBy), E (distinct)
- Total: 4 shuffles
- distinct() shuffles because it must compare all rows to find duplicates

**Snippet 4:**
| Line | Causes Shuffle? | Explanation |
|------|-----------------|-------------|
| A | No | coalesce only merges partitions, no redistribution |
| B | Yes | repartition redistributes data evenly |
| C | Yes | But combines locally first, then shuffles smaller data |
| D | Yes | Shuffles all raw values, then groups |

**reduceByKey vs groupByKey:**
reduceByKey combines values locally before shuffle, reducing network traffic. groupByKey shuffles all values, then combines.

**Snippet 5 Optimization:**
```python
df = spark.read.parquet("sales")
# Filter individual rows first (before grouping)
filtered = df.filter(df.amount > some_threshold)  # Pre-filter if possible
result = filtered.groupBy("region").sum("amount")
                 .orderBy("sum(amount)")
                 .limit(5)
```
This may not reduce shuffles but reduces data volume being shuffled.

**Summary Classification:**
- filter(): No
- map(): No
- select(): No
- groupBy(): Yes
- orderBy()/sort(): Yes
- join(): Yes (usually)
- union(): No (usually)
- distinct(): Yes
- repartition(): Yes
- coalesce(): No
- reduceByKey(): Yes
- count(): No (action, not transformation)

</details>
