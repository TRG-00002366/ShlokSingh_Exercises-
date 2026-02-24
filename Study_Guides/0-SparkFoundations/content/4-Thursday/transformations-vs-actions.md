# Transformations vs Actions

## Learning Objectives
- Distinguish between transformations and actions in Spark
- Understand why Spark separates "planning" from "doing"
- Identify common transformations and actions
- Recognize when computation actually happens

## Why This Matters

Spark does not execute your code immediately. When you call `filter()` or `map()`, nothing actually happens yet—Spark just takes notes. Only when you call an **action** like `count()` or `collect()` does Spark actually run your computation.

This separation is fundamental to how Spark works. Understanding it helps you:
- Know when your data is actually being processed
- Debug unexpected behavior
- Write more efficient code by batching operations

---

## The Two Types of Operations

Every Spark operation falls into one of two categories:

### Transformations

**Transformations** define a new dataset from an existing one. They are **lazy**—they do not execute immediately.

```
df = spark.read.csv("data.csv")     # <- Transformation (read is lazy)
filtered = df.filter(df.age > 21)   # <- Transformation
selected = filtered.select("name")  # <- Transformation

# At this point, NOTHING has actually happened!
# Spark has just recorded what you WANT to do.
```

---

### Actions

**Actions** trigger actual computation and return results (or write to storage).

```
selected.count()      # <- ACTION: Now Spark actually runs everything!
selected.show()       # <- ACTION: Display results
selected.collect()    # <- ACTION: Return all data to driver
selected.write.csv()  # <- ACTION: Write to storage
```

---

## Why Lazy Execution?

Spark delays execution for several important reasons:

### 1. Optimization Opportunities

By seeing the entire plan before executing, Spark can optimize:

```
WHAT YOU WRITE:
df.select("name", "age", "city")
  .filter(df.age > 21)
  .select("name")

WHAT SPARK DOES:
- Notices you only need "name" at the end
- Pushes filter before select
- Only reads "name" and "age" columns from source

Optimized plan:
- Read only "name", "age" columns
- Filter age > 21
- Return only "name"
```

### 2. Pipelining Operations

Multiple transformations can be combined into a single pass:

```
WITHOUT PIPELINING (hypothetical eager execution):
+--------+   write   +--------+   write   +--------+
| filter | -------> | map    | -------> | flatMap|
| result |   disk   | result |   disk   | result |
+--------+          +--------+          +--------+

WITH PIPELINING (lazy execution):
+------------------------------------------------+
| filter -> map -> flatMap (all in one pass)     |
+------------------------------------------------+

No intermediate writes!
```

### 3. Avoiding Unnecessary Work

If you never call an action, Spark never runs the computation:

```
df = spark.read.csv("huge_file.csv")  # 100 GB file
filtered = df.filter(df.col > 100)    # Define filter
mapped = filtered.select("a", "b")    # Define select

# If you never call an action, the 100 GB file is never read!
# Useful for building up complex pipelines step by step.
```

---

## Transformation Examples

### Common Transformations

| Transformation | Description | Lazy? |
|----------------|-------------|-------|
| `map(func)` | Apply function to each element | Yes |
| `filter(condition)` | Keep elements matching condition | Yes |
| `flatMap(func)` | Apply function, flatten results | Yes |
| `select(cols)` | Select specific columns | Yes |
| `groupBy(cols)` | Group by columns | Yes |
| `join(other, key)` | Join two datasets | Yes |
| `union(other)` | Combine two datasets | Yes |
| `distinct()` | Remove duplicates | Yes |
| `orderBy(cols)` | Sort by columns | Yes |
| `withColumn(name, expr)` | Add/modify column | Yes |

---

### Transformation Chaining

Transformations return new datasets, allowing chaining:

```
result = df.filter(df.age > 21)       # Returns new DataFrame
           .select("name", "city")    # Returns new DataFrame
           .groupBy("city")           # Returns GroupedData
           .count()                   # Returns new DataFrame

# Still no computation! count() here is a transformation on GroupedData
# The action comes next:

result.show()  # <- NOW it runs!
```

---

## Action Examples

### Common Actions

| Action | Description | Returns |
|--------|-------------|---------|
| `count()` | Count number of rows | Number (to driver) |
| `collect()` | Return all data to driver | List (to driver) |
| `first()` | Return first element | Single element |
| `take(n)` | Return first n elements | List |
| `show(n)` | Display first n rows | None (prints) |
| `write.*` | Write to storage | None |
| `foreach(func)` | Apply function to each element | None |
| `reduce(func)` | Aggregate using function | Single value |
| `aggregate()` | More flexible aggregation | Single value |

---

### Actions Trigger Everything

When you call an action, Spark:
1. Looks at all transformations leading to this action
2. Builds an optimized execution plan
3. Executes the plan across the cluster
4. Returns the result

```
TRANSFORMATIONS (Building the plan):

df.filter(...)  ->  .map(...)  ->  .groupBy(...)  ->  .sum(...)
     |               |                 |                 |
     v               v                 v                 v
   [Plan]         [Plan]            [Plan]            [Plan]

ACTION (Executing the plan):

     .count()
         |
         v
   +---------------------+
   | TRIGGER EXECUTION!  |
   | Run the whole plan  |
   +---------------------+
```

---

## Visualizing Lazy Evaluation

```
+------------------------------------------------------------------+
|                     LAZY EVALUATION                              |
|                                                                  |
|   YOUR CODE:                                                     |
|   +----------------------------------------------------------+   |
|   | df = spark.read.csv("data.csv")                          |   |
|   | df2 = df.filter(df.age > 21)                             |   |
|   | df3 = df2.select("name", "city")                         |   |
|   | df4 = df3.groupBy("city")                                |   |
|   | result = df4.count()                                     |   |
|   | result.show()   # <-- ACTION                             |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   WHAT SPARK BUILDS (before action):                             |
|   +----------------------------------------------------------+   |
|   |                    LOGICAL PLAN                          |   |
|   |                                                          |   |
|   |   CSV Source -> Filter -> Select -> GroupBy -> Count     |   |
|   |                                                          |   |
|   |   (Just a plan, no data processed yet)                   |   |
|   +----------------------------------------------------------+   |
|                                                                  |
|   WHEN ACTION TRIGGERS:                                          |
|   +----------------------------------------------------------+   |
|   | 1. Optimize the plan (Catalyst)                          |   |
|   | 2. Create physical execution plan                        |   |
|   | 3. Divide into stages and tasks                          |   |
|   | 4. Execute on cluster                                    |   |
|   | 5. Return result                                         |   |
|   +----------------------------------------------------------+   |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Multiple Actions = Multiple Runs

Each action triggers a fresh execution:

```
df = spark.read.csv("data.csv")
filtered = df.filter(df.age > 21)

# First action - reads file, filters, counts
count1 = filtered.count()

# Second action - reads file AGAIN, filters AGAIN, takes first
first_row = filtered.first()

# The file was read TWICE!

SOLUTION: Cache if you will use the same data multiple times

filtered.cache()  # Mark for caching
filtered.count()  # First action: read, filter, cache
filtered.first()  # Second action: read from cache (fast!)
```

---

## Common Mistakes

### Mistake 1: Expecting Immediate Execution

```
# This does NOT print anything
df = spark.read.csv("data.csv")
df.filter(df.error == True)  # Seems like it should do something

# You need an action!
df.filter(df.error == True).show()  # Now it runs
```

---

### Mistake 2: Forgetting to Assign Transformations

```
# WRONG: Transformation result not captured
df.filter(df.age > 21)  # This returns a new DataFrame, but it is lost!
df.show()  # Shows ORIGINAL df, not filtered!

# RIGHT: Assign the result
filtered_df = df.filter(df.age > 21)
filtered_df.show()  # Shows filtered data
```

---

### Mistake 3: Unnecessary Actions

```
# INEFFICIENT: Two actions, two full runs
count = df.count()
first = df.first()

# BETTER: One action with both results (if possible)
# Or cache the DataFrame first
df.cache()
count = df.count()
first = df.first()  # Uses cached data
```

---

## Key Takeaways

1. **Transformations are lazy:** They define what to do but do not execute.

2. **Actions trigger execution:** Only actions cause Spark to run your code.

3. **Lazy execution enables optimization:** Spark sees the whole plan before running.

4. **Each action runs the full pipeline:** Unless you cache intermediate results.

5. **Chaining is free:** You can chain many transformations before an action.

6. **Know your operations:** Identify which are transformations and which are actions.

---

## Additional Resources

- [RDD Operations (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-operations)
- [Lazy Evaluation in Spark (Databricks)](https://docs.databricks.com/en/spark/lazy-evaluation.html)
- [Understanding Spark Lazy Evaluation (Video)](https://www.youtube.com/watch?v=_C8kWso4ne4)
