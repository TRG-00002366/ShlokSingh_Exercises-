# RDD Immutability

## Learning Objectives
- Understand why Spark RDDs are immutable
- Explain how immutability enables fault tolerance
- Compare mutable vs immutable approaches to failure recovery
- Recognize the trade-offs of immutability

## Why This Matters

Immutability is a fundamental design decision in Spark that enables fault tolerance. You cannot modify an RDD—you can only create new RDDs from existing ones. This might seem restrictive, but it is the key to Spark's ability to recover from failures without losing data.

Understanding immutability helps you:
- Appreciate why Spark's recovery mechanism works
- Avoid common mental model mistakes
- Write code that works with Spark's design

---

## What is Immutability?

**Immutable** means "cannot be changed." Once an RDD is created, its contents never change.

```
MUTABLE DATA (like a Python list):

original = [1, 2, 3]
original.append(4)      # Modifies original!
# original is now [1, 2, 3, 4]

The original list was changed.

---

IMMUTABLE DATA (like Spark RDDs):

rdd1 = sc.parallelize([1, 2, 3])
rdd2 = rdd1.map(lambda x: x + 1)   # Creates NEW RDD!

# rdd1 is still [1, 2, 3]
# rdd2 is [2, 3, 4]

rdd1 was not changed. A new RDD was created.
```

---

## Why Immutability Matters for Fault Tolerance

Immutability enables recovery because **you always know what the correct answer is.**

### Mutable Systems: Recovery is Hard

```
MUTABLE SYSTEM:
+------------------------------------------------+
| Step 1: Load data           -> [1, 2, 3, 4, 5] |
| Step 2: Modify in place     -> [1, 4, 9, ?, ?] |
|                                      ^          |
|                                      |          |
|                              CRASH - lost track |
|                              of what was done   |
| Step 3: ??? What was original? What was done?  |
+------------------------------------------------+

If the system crashes mid-modification:
- Original data is gone (was overwritten)
- Partial results are unclear
- Cannot reliably recover
```

---

### Immutable Systems: Recovery is Easy

```
IMMUTABLE SYSTEM:
+------------------------------------------------+
| Step 1: rdd1 = load data    -> [1, 2, 3, 4, 5] |
| Step 2: rdd2 = rdd1.map(x^2)                   |
|                 rdd2[0,1,2] computed = [1, 4, 9]|
|                 rdd2[3,4] pending               |
|                              ^                  |
|                              |                  |
|                            CRASH                |
|                                                 |
| RECOVERY:                                       |
| - rdd1 still exists (immutable, not modified)  |
| - rdd2[0,1,2] may be lost                       |
| - Spark can REDO: rdd2 = rdd1.map(x^2)         |
|                                                 |
| Result: [1, 4, 9, 16, 25] - correct!           |
+------------------------------------------------+

Because rdd1 was never modified, we can always re-derive rdd2.
```

---

## The Lineage Connection

Immutability enables **lineage-based recovery**:

```
LINEAGE:
rdd1 = load("data.txt")
rdd2 = rdd1.filter(x > 10)
rdd3 = rdd2.map(x * 2)
rdd4 = rdd3.reduce(sum)

Each RDD knows:
- What RDD it came from (parent)
- What operation was applied

+------+    filter    +------+    map    +------+    reduce    +------+
| rdd1 | -----------> | rdd2 | --------> | rdd3 | -----------> | rdd4 |
+------+              +------+           +------+              +------+

If rdd3 is lost, Spark can:
1. Look up: rdd3 = rdd2.map(x * 2)
2. If rdd2 exists, apply map
3. If rdd2 is also lost, go further back: rdd2 = rdd1.filter(x > 10)
4. Continue until we find existing data or the source
```

Immutability guarantees that re-applying the same operation to the same input always produces the same output.

---

## Diagram: Mutable vs Immutable Recovery

```
+------------------------------------------------------------------+
|                    MUTABLE SYSTEM                                |
|                                                                  |
|   +--------+     +--------+     +--------+                       |
|   | Data   |     | Data   |     | Data   |                       |
|   | v1     | --> | v2     | --> | v3     |                       |
|   | exists |     | lost   |     | never  |                       |
|   +--------+     | (over- |     | created|                       |
|                  | written)|    +--------+                       |
|                  +--------+                                      |
|                                                                  |
|   v1 was overwritten by v2. When v2 processing fails:            |
|   - v1 is GONE (cannot recover)                                  |
|   - Must reload from external source                             |
|                                                                  |
+------------------------------------------------------------------+

+------------------------------------------------------------------+
|                   IMMUTABLE SYSTEM                               |
|                                                                  |
|   +--------+     +--------+     +--------+                       |
|   | rdd1   |     | rdd2   |     | rdd3   |                       |
|   | exists | --> | lost   | --> | never  |                       |
|   | still! |     | (can   |     | created|                       |
|   +--------+     | rederive)    +--------+                       |
|                  +--------+                                      |
|                                                                  |
|   rdd1 still exists. When rdd2 processing fails:                 |
|   - rdd1 is AVAILABLE                                            |
|   - Spark re-applies: rdd2 = rdd1.filter(...)                    |
|   - Then continues: rdd3 = rdd2.map(...)                         |
|                                                                  |
+------------------------------------------------------------------+
```

---

## Transformations Create New RDDs

Every transformation returns a new RDD:

```
Original:
rdd1 = sc.parallelize([1, 2, 3, 4, 5])

Transformations create new RDDs:
rdd2 = rdd1.filter(lambda x: x > 2)     # rdd1 unchanged, rdd2 = [3, 4, 5]
rdd3 = rdd2.map(lambda x: x * 10)       # rdd2 unchanged, rdd3 = [30, 40, 50]
rdd4 = rdd3.filter(lambda x: x < 45)    # rdd3 unchanged, rdd4 = [30, 40]

All four RDDs exist (conceptually):
rdd1 = [1, 2, 3, 4, 5]
rdd2 = [3, 4, 5]
rdd3 = [30, 40, 50]
rdd4 = [30, 40]

Each can be recomputed from its parent if lost.
```

---

## Trade-offs of Immutability

### Benefits

| Benefit | Description |
|---------|-------------|
| **Fault tolerance** | Can always recompute from lineage |
| **Consistency** | No partial updates or race conditions |
| **Parallelism** | Safe to read same data from multiple tasks |
| **Reproducibility** | Same input + same operations = same output |

---

### Costs

| Cost | Description |
|------|-------------|
| **Memory usage** | Multiple versions of data may exist |
| **Recomputation** | Must redo work instead of just fixing in place |
| **No in-place updates** | Cannot modify; must create + replace |

Spark mitigates these costs:
- **Lazy evaluation:** Intermediate RDDs are not always materialized
- **Pipelining:** Multiple transformations fused into one pass
- **Caching:** Explicitly persist frequently-used RDDs

---

## Immutability in DataFrames

DataFrames are also immutable:

```
df1 = spark.read.csv("data.csv")
df2 = df1.filter(df1.age > 21)      # df1 unchanged
df3 = df2.select("name", "city")    # df2 unchanged
df4 = df3.withColumn("new", lit(1)) # df3 unchanged

Each operation creates a new DataFrame.
The original is never modified.
```

---

## Key Takeaways

1. **RDDs and DataFrames are immutable:** They cannot be modified after creation.

2. **Transformations create new RDDs:** They do not change existing ones.

3. **Immutability enables fault tolerance:** We can always recompute from lineage.

4. **Mutable systems cannot easily recover:** Original data may be lost.

5. **Trade-offs exist:** Memory overhead, but mitigated by lazy evaluation and caching.

6. **This is fundamental to Spark's design:** Everything else builds on immutability.

---

## Additional Resources

- [RDD Programming Guide (Official Docs)](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Why Immutability Matters in Distributed Systems](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
- [Functional Programming Principles](https://www.scala-lang.org/docu/files/ScalaTutorial.pdf)
