# Pair Programming: Spark Optimization

## Overview
Work in pairs to configure and optimize a Spark application for performance. One person acts as the **Driver** (writes code), the other as the **Navigator** (reviews and guides). Switch roles halfway.

**Duration:** 2-3 hours  
**Mode:** Pair Programming (Driver/Navigator)

---

## The Scenario

You have been given a poorly performing Spark job that processes sales data. Your task is to analyze, configure, and optimize it for better performance.

---

## Setup

Create the starter files in your working directory.

**slow_job.py** (the problematic code):
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SlowJob") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Generate sample data (simulating large dataset)
data = [(f"product_{i % 100}", f"category_{i % 10}", i * 1.5, i % 50) 
        for i in range(100000)]

rdd = sc.parallelize(data)

# Problematic Pattern 1: Using groupByKey
grouped = rdd.map(lambda x: (x[1], x[2])) \
             .groupByKey() \
             .mapValues(lambda values: sum(values))

# Problematic Pattern 2: Collecting large data
all_data = rdd.collect()

# Problematic Pattern 3: Multiple actions on same RDD
count1 = rdd.count()
count2 = rdd.filter(lambda x: x[3] > 25).count()
count3 = rdd.map(lambda x: x[2]).reduce(lambda a, b: a + b)

print(f"Grouped: {grouped.take(5)}")
print(f"Counts: {count1}, {count2}, {count3}")

spark.stop()
```

---

## Core Tasks

### Phase 1: Analysis (Driver: Person A, Navigator: Person B)

**Task 1:** Identify the performance problems in `slow_job.py`

Document at least 3 issues:
1. Issue: _________________ Why it is bad: _________________
2. Issue: _________________ Why it is bad: _________________
3. Issue: _________________ Why it is bad: _________________

**Task 2:** Create an optimized version `optimized_job.py`

Apply these fixes:
- Replace `groupByKey` with `reduceByKey`
- Replace `collect()` with `take()` or remove
- Cache the RDD if used multiple times
- Add appropriate configuration

### Phase 2: Configuration (Switch roles - Driver: Person B, Navigator: Person A)

**Task 3:** Add spark-submit configuration

Create `run_optimized.sh`:
```bash
spark-submit \
    --master local[*] \
    --driver-memory 2g \
    # Add more configuration options
    optimized_job.py
```

**Task 4:** Experiment with these settings:
- `spark.sql.shuffle.partitions`
- `spark.default.parallelism`
- Memory settings

### Phase 3: Measurement

**Task 5:** Compare performance

Run both versions and record:

| Metric | slow_job.py | optimized_job.py |
|--------|-------------|------------------|
| Execution time | | |
| Memory usage | | |
| Shuffle size | | |

---

## Deliverables

1. `slow_job.py` - Original (for reference)
2. `optimized_job.py` - Your optimized version
3. `run_optimized.sh` - spark-submit script
4. `optimization_report.md` - Document issues found and fixes applied

---

## optimization_report.md Template

```markdown
# Spark Optimization Report

## Team Members
- Driver 1: [Name]
- Driver 2: [Name]

## Issues Identified

### Issue 1: [Name]
- Problem: 
- Impact:
- Solution:

### Issue 2: [Name]
...

## Performance Comparison

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Time   |        |       |             |
| Memory |        |       |             |

## Configuration Used
[List spark-submit options]

## Lessons Learned
[What did you learn about Spark optimization?]
```

---

## Definition of Done

- [ ] All performance issues documented
- [ ] `optimized_job.py` runs faster than original
- [ ] `run_optimized.sh` includes appropriate configuration
- [ ] `optimization_report.md` is complete
- [ ] Both partners contributed (Driver/Navigator roles switched)

---

## Additional Resources
- Written Content: `configuration.md`
- Written Content: `shared-variables.md`
- Demo: `demo_spark_config.py`
