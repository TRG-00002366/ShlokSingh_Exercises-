# Reading Execution Plans

## Learning Objectives
- Interpret Spark execution plan output
- Identify stage boundaries and shuffle operations
- Recognize key operators in physical plans
- Use execution plans to understand job behavior

## Why This Matters

Execution plans are your window into what Spark will actually do when you run your query. They show:
- The order of operations
- Where shuffles will occur
- What optimizations were applied
- Potential performance issues

Learning to read execution plans turns Spark from a black box into a transparent system.

---

## What is an Execution Plan?

An execution plan is Spark's description of how it will execute your query. There are two types:

```
LOGICAL PLAN:
- What you asked for
- Abstract description of operations

PHYSICAL PLAN:
- How Spark will do it
- Specific algorithms and strategies
```

---

## Viewing Execution Plans

You can view plans without running the query:

```
df.explain()           # Default: Physical plan only
df.explain(True)       # Both logical and physical plans
df.explain("extended") # More detail
df.explain("cost")     # Include cost estimates
df.explain("formatted")# Nicely formatted output
```

---

## Anatomy of an Execution Plan

Execution plans are read **bottom-up** (data flows up):

```
== Physical Plan ==
*(2) HashAggregate(keys=[city#45], functions=[count(1)])
+- Exchange hashpartitioning(city#45, 200)
   +- *(1) HashAggregate(keys=[city#45], functions=[partial_count(1)])
      +- *(1) Project [city#45]
         +- *(1) Filter (age#46 > 21)
            +- *(1) FileScan csv [name#44,age#46,city#45]

READ BOTTOM-UP:
1. FileScan: Read from CSV
2. Filter: Keep rows where age > 21
3. Project: Select only city column
4. HashAggregate: Partial count by city
5. Exchange: SHUFFLE (redistribute by city)
6. HashAggregate: Final count by city
```

---

## Key Operators to Know

### Data Source Operators

| Operator | Description |
|----------|-------------|
| `FileScan` | Read from file (parquet, csv, json, etc.) |
| `Scan` | Read from table or data source |
| `InMemoryTableScan` | Read from cached data |

---

### Transformation Operators

| Operator | Description |
|----------|-------------|
| `Project` | Select columns |
| `Filter` | Filter rows |
| `HashAggregate` | Aggregation (group by, sum, count) |
| `Sort` | Order by operation |
| `SortMergeJoin` | Join using sort-merge algorithm |
| `BroadcastHashJoin` | Join with broadcast (small table) |
| `BroadcastExchange` | Send data to all executors |

---

### Exchange Operators (Shuffles)

| Operator | Description |
|----------|-------------|
| `Exchange hashpartitioning` | Shuffle by hash of keys |
| `Exchange rangepartitioning` | Shuffle by value ranges |
| `Exchange SinglePartition` | Collect to single partition |
| `Exchange RoundRobinPartitioning` | Evenly distribute |

**Exchange = Shuffle! This is a stage boundary.**

---

## Understanding Stage Numbers

The asterisk and number indicate the stage:

```
*(2) HashAggregate(...)          <-- Stage 2
+- Exchange hashpartitioning(...) <-- Stage boundary (shuffle)
   +- *(1) HashAggregate(...)     <-- Stage 1
      +- *(1) Project(...)        <-- Stage 1
         +- *(1) Filter(...)      <-- Stage 1
            +- *(1) FileScan(...) <-- Stage 1

Stage 1: FileScan -> Filter -> Project -> HashAggregate (partial)
         [All pipelined, no intermediate writes]

SHUFFLE (Exchange)

Stage 2: HashAggregate (final)
```

---

## Reading a Join Plan

Joins are common and worth understanding:

```
== Physical Plan ==
*(3) Project [name#10, order_id#25]
+- *(3) BroadcastHashJoin [customer_id#11], [customer_id#26], Inner
   :- *(1) Filter (age#12 > 21)
   :  +- *(1) FileScan parquet [customer_id#11,name#10,age#12]
   +- BroadcastExchange HashedRelationBroadcastMode
      +- *(2) FileScan parquet [customer_id#26,order_id#25]

Interpretation:
1. Stage 1: Read customers, filter age > 21
2. Stage 2: Read orders
3. BroadcastExchange: Send orders to all executors
4. Stage 3: Join using BroadcastHashJoin, then project
```

**BroadcastHashJoin** means one table (orders) is small enough to broadcast—no shuffle of the large table!

---

## Recognizing Good vs Bad Plans

### Good Signs

```
+- *(1) FileScan parquet [...pushed filters: age > 21...]
   ^^ Filter pushed to file scan!

+- BroadcastHashJoin [...]
   ^^ No shuffle for join!

+- *(1) Filter -> *(1) Project -> *(1) Aggregate
   ^^ All pipelined in one stage!
```

---

### Warning Signs

```
+- Exchange hashpartitioning(..., 200)
+- Exchange hashpartitioning(..., 200)
   ^^ Multiple exchanges - multiple shuffles!

+- SortMergeJoin [...]
   ^^ May indicate both sides are large (shuffle both)

+- *(1) FileScan [...all 50 columns...]
   ^^ Reading more columns than needed?
```

---

## Example: Before and After Optimization

### Query

```
"SELECT name FROM users WHERE age > 21 AND city = 'NYC' ORDER BY name"
```

### Unoptimized (Hypothetical)

```
Sort(name)
+- Project(name)
   +- Filter(age > 21 AND city = 'NYC')
      +- Scan(users, all columns)

Problems:
- Reads all columns
- Filter after read
- Client-side sort
```

### Optimized (Catalyst)

```
*(1) Sort [name ASC]
+- Exchange rangepartitioning(name, 200)
   +- *(1) Project [name]
      +- *(1) Filter (age > 21 AND city = 'NYC')
         +- *(1) FileScan parquet [name,age,city] pushed filters: [age > 21, city = 'NYC']

Improvements:
- Only reads 3 columns (projection pushdown)
- Filters pushed to scan
- Distributed sort via exchange
```

---

## Diagram: Plan Structure

```
+------------------------------------------------------------------+
|                    EXECUTION PLAN STRUCTURE                      |
|                                                                  |
|   TOP OF PLAN (Final output)                                     |
|   +----------------------------------------------------------+   |
|   | *(N) Final Operator                                      |   |
|   +----------------------------------------------------------+   |
|                           ^                                      |
|                           |                                      |
|   +----------------------------------------------------------+   |
|   | Exchange (SHUFFLE) - Stage Boundary                      |   |
|   +----------------------------------------------------------+   |
|                           ^                                      |
|                           |                                      |
|   +----------------------------------------------------------+   |
|   | *(N-1) Operators... (may be many)                        |   |
|   +----------------------------------------------------------+   |
|                           ^                                      |
|                           |                                      |
|   +----------------------------------------------------------+   |
|   | Exchange (SHUFFLE) - Stage Boundary                      |   |
|   +----------------------------------------------------------+   |
|                           ^                                      |
|                           |                                      |
|   +----------------------------------------------------------+   |
|   | *(1) Initial Operators                                   |   |
|   +----------------------------------------------------------+   |
|                           ^                                      |
|                           |                                      |
|   +----------------------------------------------------------+   |
|   | FileScan / Scan (Data Source)                            |   |
|   +----------------------------------------------------------+   |
|   BOTTOM OF PLAN (Data source)                                   |
|                                                                  |
|   Read from BOTTOM to TOP to follow data flow                    |
+------------------------------------------------------------------+
```

---

## Key Takeaways

1. **Plans read bottom-up:** Data flows from source to result.

2. **Exchange = Shuffle:** Look for Exchange operators to find stage boundaries.

3. **Stage numbers show pipelining:** Same number = same stage = efficient.

4. **Watch for pushdowns:** "pushed filters" and column pruning are good.

5. **Join strategies matter:** BroadcastHashJoin is usually better than SortMergeJoin.

6. **Use explain() liberally:** Check plans before running expensive queries.

---

## Additional Resources

- [Understanding Spark SQL Execution Plans (Databricks)](https://docs.databricks.com/en/sql/language-manual/explain.html)
- [Spark SQL Performance Tuning (Official Docs)](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Reading Spark Plans (Video)](https://www.youtube.com/watch?v=_C8kWso4ne4)
