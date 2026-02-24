# Exercise: Predict Execution Points

## Exercise Overview
- **Duration:** 20 minutes
- **Format:** Paper-based analysis
- **Materials:** Pencil

## Learning Objective
Understand lazy evaluation by predicting when actual computation occurs.

---

## Instructions

For each code snippet:
1. Mark each line as PLANS (no execution) or EXECUTES (triggers computation)
2. Explain why execution happens (or doesn't) at each point
3. Identify how many times the source data is read

---

## Exercise 1: Basic Pipeline

Annotate each line:

```python
# Line 1
df = spark.read.csv("data.csv")
# This line: [ ] PLANS   [ ] EXECUTES
# Why: _________________________________

# Line 2
filtered = df.filter(df.age > 21)
# This line: [ ] PLANS   [ ] EXECUTES
# Why: _________________________________

# Line 3
selected = filtered.select("name", "city")
# This line: [ ] PLANS   [ ] EXECUTES
# Why: _________________________________

# Line 4
result = selected.count()
# This line: [ ] PLANS   [ ] EXECUTES
# Why: _________________________________

# Line 5
print(f"Total: {result}")
# This line: [ ] PLANS   [ ] EXECUTES
# Why: _________________________________
```

**Question:** At which line does Spark actually read the CSV file?

```
Line _____
```

---

## Exercise 2: Multiple Actions

```python
# Line 1
df = spark.read.parquet("sales.parquet")

# Line 2
expensive = df.filter(df.amount > 1000)

# Line 3
count1 = expensive.count()          # ACTION 1

# Line 4
first_row = expensive.first()       # ACTION 2

# Line 5
all_rows = expensive.collect()      # ACTION 3
```

**Mark execution points:**

| Line | Executes? | Data Read from Source? |
|------|-----------|------------------------|
| 1 | Yes / No | Yes / No |
| 2 | Yes / No | Yes / No |
| 3 | Yes / No | Yes / No |
| 4 | Yes / No | Yes / No |
| 5 | Yes / No | Yes / No |

**Question:** How many times is the parquet file read?

```
_____ times
```

**Question:** How could you avoid re-reading the file?

```
_________________________________________________
```

---

## Exercise 3: Cache Usage

```python
# Line 1
df = spark.read.json("events.json")

# Line 2
processed = df.filter(df.valid == True).select("id", "timestamp", "value")

# Line 3
processed.cache()                   # Mark for caching

# Line 4
count = processed.count()           # ACTION 1 (also materializes cache)

# Line 5
sample = processed.take(10)         # ACTION 2

# Line 6
stats = processed.describe()        # ACTION 3

# Line 7
stats.show()                        # ACTION 4
```

**Mark what happens at each line:**

| Line | What Happens |
|------|--------------|
| 1 | |
| 2 | |
| 3 | |
| 4 | |
| 5 | |
| 6 | |
| 7 | |

**Question:** How many times is events.json read from disk?

```
_____ time(s)
```

**Question:** Why is Line 5 faster than Line 4 (assuming same data size)?

```
_________________________________________________
```

---

## Exercise 4: Tricky Cases

Determine if computation happens:

```python
# Case A: .show() vs .explain()
df = spark.read.csv("data.csv")
df.filter(df.x > 10).explain()     # Does this execute the filter?
                                    # [ ] Yes   [ ] No
                                    # Why: ____________________

df.filter(df.x > 10).show()         # Does this execute the filter?
                                    # [ ] Yes   [ ] No
                                    # Why: ____________________

# Case B: Saving output
df = spark.read.csv("data.csv")
result = df.groupBy("category").count()
result.write.parquet("output/")    # Does this execute?
                                    # [ ] Yes   [ ] No
                                    # Why: ____________________

# Case C: Creating a view
df = spark.read.csv("data.csv")
df.createOrReplaceTempView("my_table")  # Does this read the CSV?
                                         # [ ] Yes   [ ] No
                                         # Why: ____________________

spark.sql("SELECT * FROM my_table").show()  # Does this read the CSV?
                                             # [ ] Yes   [ ] No
                                             # Why: ____________________
```

---

## Exercise 5: Execution Flow

Draw a timeline showing when computation happens:

```python
# This code
df1 = spark.read.csv("file1.csv")           # T1
df2 = spark.read.csv("file2.csv")           # T2
filtered1 = df1.filter(df1.x > 0)           # T3
filtered2 = df2.filter(df2.y < 100)         # T4
joined = filtered1.join(filtered2, "id")    # T5
aggregated = joined.groupBy("category").sum("value")  # T6
aggregated.show()                            # T7
```

**Fill in the timeline:**

```
Timeline:
T1 (read csv1):      [ ] Plans only   [ ] Executes
T2 (read csv2):      [ ] Plans only   [ ] Executes
T3 (filter):         [ ] Plans only   [ ] Executes
T4 (filter):         [ ] Plans only   [ ] Executes
T5 (join):           [ ] Plans only   [ ] Executes
T6 (groupBy):        [ ] Plans only   [ ] Executes
T7 (show):           [ ] Plans only   [ ] Executes <- Everything runs here!
```

**Question:** At T7, what is the order of actual execution?

```
1. _________________________________________________
2. _________________________________________________
3. _________________________________________________
4. _________________________________________________
5. _________________________________________________
```

---

## Summary Questions

1. What is the difference between a transformation and an action?

```
Transformation: ________________________________________

Action: ________________________________________________
```

2. List 5 actions (operations that trigger execution):

```
1. _______________
2. _______________
3. _______________
4. _______________
5. _______________
```

3. Why does Spark use lazy evaluation?

```
Reason 1: ______________________________________________

Reason 2: ______________________________________________
```

---

## Answer Key

<details>
<summary>Click to reveal answers</summary>

**Exercise 1:**
- Line 1: PLANS (read is lazy)
- Line 2: PLANS (filter is transformation)
- Line 3: PLANS (select is transformation)
- Line 4: EXECUTES (count is action)
- Line 5: Just Python print, result already computed
- CSV file read at Line 4 (when count triggers execution)

**Exercise 2:**
- Lines 1-2: Plans only
- Lines 3, 4, 5: All execute
- File read 3 times (once per action)
- Solution: processed.cache() before first action

**Exercise 3:**
- Line 3: Just marks for caching
- Line 4: Executes AND caches
- Lines 5-7: Read from cache (fast)
- events.json read 1 time
- Line 5 faster because data is in cache

**Exercise 4:**
- explain(): No (just shows plan)
- show(): Yes (displays data)
- write.parquet(): Yes (writing is an action)
- createOrReplaceTempView(): No (just registers)
- spark.sql(...).show(): Yes (show triggers)

**Exercise 5:**
- T1-T6: All plans only
- T7: Everything executes
- Order: Read both files -> filters (can be parallel) -> join -> groupBy -> show

</details>
