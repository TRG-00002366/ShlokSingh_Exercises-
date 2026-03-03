# Spark Optimization Report

## Team Members
- Driver 1: Shlok Singh
- Driver 2: __________________

## Issues Identified

### Issue 1: `groupByKey()` (wide shuffle + memory blowup risk)
- Problem: The job does `map(...).groupByKey().mapValues(sum)`. `groupByKey()` shuffles *all values* for each key across the network and stores large iterables per key.
- Impact: Higher shuffle traffic, higher memory pressure, slower execution (especially as data grows).
- Solution: Replace with `reduceByKey()` (or `aggregateByKey()`), so Spark does **map-side combining** and shuffles far less data.

### Issue 2: `collect()` on a large RDD (driver memory / crash risk)
- Problem: `all_data = rdd.collect()` pulls the entire dataset back to the driver.
- Impact: Can cause driver OOM, slowdowns from massive serialization + transfer, and it’s unnecessary for most pipelines.
- Solution: Remove it, or replace with `take(n)` / `sample()` for debugging.

### Issue 3: Multiple actions on the same RDD without caching
- Problem: `rdd.count()`, `rdd.filter(...).count()`, and `rdd.map(...).reduce(...)` each trigger a full recomputation of the same lineage.
- Impact: Repeated scans of the dataset = wasted CPU time and longer total runtime.
- Solution: `rdd.cache()` (or `persist(StorageLevel.MEMORY_ONLY)` / `MEMORY_AND_DISK`) before multiple actions, and optionally materialize cache once with a single action (ex: `rdd.count()`).

### Issue 4 (extra): No Spark configs tuned for workload
- Problem: Defaults may not match local machine cores and shuffle patterns.
- Impact: Suboptimal parallelism and unnecessary scheduling overhead.
- Solution: Set `spark.default.parallelism` (RDD), and if using DataFrames also tune `spark.sql.shuffle.partitions`. Add memory settings for driver/executor.

## Performance Comparison

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Time   | 20.724s (slow_job.py, spark-submit local[*]) | N/A (optimized script returned in 0.004s, which indicates Spark did not actually run) | N/A |
| Memory | Higher risk (driver `collect()` + `groupByKey()` buffering) | Lower risk (no `collect`, uses `reduceByKey`, caching) | Expected improvement |

> Note: The optimized run must be re-measured using `time spark-submit ... optimized_job.py` (or fixing `run_optimized.sh`) to record a valid "After" runtime.

## Configuration Used
Planned spark-submit options for optimized job (example):
- `--master local[*]`
- `--driver-memory 2g`
- `--conf spark.default.parallelism=8`
- (If using Spark SQL / DataFrames) `--conf spark.sql.shuffle.partitions=8`
- `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer`

## Lessons Learned
- `groupByKey()` is usually a performance trap for aggregations; prefer `reduceByKey()` / `aggregateByKey()` for map-side combining.
- `collect()` is dangerous on large datasets; use `take()` / `sample()` unless you truly need everything on the driver.
- If you run multiple actions on the same RDD, caching/persisting can save a lot of recomputation time.
- Performance comparisons must be measured consistently (same command style, same configs). A shell script that exits instantly usually means Spark never executed.