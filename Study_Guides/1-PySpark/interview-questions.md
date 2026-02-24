# Interview Questions: PySpark (Weeks 1-2)

This question bank is designed to prepare trainees for technical interviews covering PySpark fundamentals, RDD operations, Spark SQL, DataFrames, and cluster deployment concepts.

---

## Beginner (Foundational)

### Q1: What is Apache Spark, and why is it faster than Hadoop MapReduce?
**Keywords:** In-memory processing, Distributed computing, DAG execution, Disk I/O

<details>
<summary>Click to Reveal Answer</summary>

Apache Spark is an open-source, distributed computing system designed for fast, large-scale data processing. It is faster than Hadoop MapReduce because Spark uses in-memory processing, keeping intermediate results in RAM across operations rather than writing them to disk after each stage. Additionally, Spark's DAG (Directed Acyclic Graph) execution engine optimizes the entire computation, combining multiple operations into efficient stages and minimizing data shuffling.
</details>

---

### Q2: What is PySpark, and why would a data engineer use it?
**Keywords:** Python API, Spark, Integration, Data science ecosystem

<details>
<summary>Click to Reveal Answer</summary>

PySpark is the Python API for Apache Spark. Data engineers use PySpark because Python is the most popular language for data science and engineering. PySpark allows developers to leverage their existing Python knowledge while benefiting from Spark's distributed computing capabilities. It also integrates seamlessly with the Python ecosystem, including libraries like NumPy and Pandas, and works well with Jupyter notebooks for interactive development.
</details>

---

### Q3: What is an RDD, and what does "Resilient" mean in this context?
**Keywords:** Resilient Distributed Dataset, Immutable, Partitioned, Lineage, Fault tolerance

<details>
<summary>Click to Reveal Answer</summary>

An RDD (Resilient Distributed Dataset) is an immutable, partitioned collection of elements that can be processed in parallel across a cluster. "Resilient" refers to its fault tolerance: Spark tracks the sequence of transformations (lineage) used to build each RDD. If a partition is lost due to node failure, Spark can automatically recompute it from the original data using the lineage information, without requiring data replication.
</details>

---

### Q4: Explain the difference between transformations and actions in Spark.
**Keywords:** Lazy evaluation, Computation, Return type, Trigger

<details>
<summary>Click to Reveal Answer</summary>

Transformations are lazy operations that define a computation on an RDD without executing it immediately. They create a new RDD from an existing one (e.g., `map`, `filter`, `flatMap`). Actions trigger the actual computation and return results to the driver or write to storage (e.g., `count`, `collect`, `reduce`). Spark builds a computation plan from transformations and only executes when an action is called.
</details>

---

### Q5: What is the difference between SparkContext and SparkSession?
**Keywords:** Entry point, Unified API, Spark 2.0, SQLContext, HiveContext

<details>
<summary>Click to Reveal Answer</summary>

SparkContext is the original entry point for Spark functionality, used primarily for creating RDDs and connecting to a Spark cluster. SparkSession, introduced in Spark 2.0, is the unified entry point that combines SparkContext, SQLContext, and HiveContext. SparkSession is the preferred way to initialize Spark applications as it provides access to all Spark functionality through a single object and simplifies application development.
</details>

---

### Q6: What are the three key properties of an RDD?
**Keywords:** Immutability, Partitioning, Lineage

<details>
<summary>Click to Reveal Answer</summary>

The three key properties of an RDD are:
1. **Immutability:** RDDs cannot be modified once created; transformations produce new RDDs.
2. **Partitioning:** RDDs are divided into partitions distributed across cluster nodes, enabling parallel processing.
3. **Lineage:** Spark tracks the sequence of transformations that created each RDD, allowing reconstruction of lost partitions for fault tolerance.
</details>

---

### Q7: What is lazy evaluation, and why does Spark use it?
**Keywords:** Deferred execution, Optimization, Pipelining, Query planning

<details>
<summary>Click to Reveal Answer</summary>

Lazy evaluation means transformations on RDDs do not execute immediately. Instead, Spark builds a computation plan and only executes when an action is called. Spark uses lazy evaluation because it enables query optimization (combining multiple operations efficiently), reduces unnecessary computations (only computing what is needed for the action), and allows pipelining of operations without intermediate data materialization.
</details>

---

### Q8: What is a DataFrame in Spark, and how does it differ from an RDD?
**Keywords:** Named columns, Schema, Catalyst optimizer, Structured data

<details>
<summary>Click to Reveal Answer</summary>

A DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database or a Pandas DataFrame. Unlike RDDs, which are unstructured collections of objects, DataFrames have a schema and support optimized execution through Spark's Catalyst optimizer. DataFrames provide higher-level operations, better performance for structured data, and more readable code compared to raw RDD operations.
</details>

---

### Q9: Name three common RDD actions and describe what each returns.
**Keywords:** collect, count, reduce, take, foreach

<details>
<summary>Click to Reveal Answer</summary>

Three common RDD actions are:
1. **collect():** Returns all elements of the RDD as a list to the driver program.
2. **count():** Returns the number of elements in the RDD as an integer.
3. **reduce(func):** Aggregates all elements using a specified function and returns a single combined result.

Other common actions include `take(n)` which returns the first n elements, and `foreach(func)` which applies a function to each element.
</details>

---

### Q10: What is an executor in Spark?
**Keywords:** JVM process, Worker node, Tasks, Memory management, Caching

<details>
<summary>Click to Reveal Answer</summary>

An executor is a JVM process that runs on worker nodes in a Spark cluster. Each executor runs tasks assigned by the driver, stores data for caching, reports results and status back to the driver, and manages memory for computation and storage. Executors can run multiple tasks concurrently based on the number of cores configured.
</details>

---

## Intermediate (Application)

### Q11: Explain the difference between narrow and wide transformations. Why does this distinction matter?
**Keywords:** Shuffle, Partition, Network transfer, Performance, Stage boundary
**Hint:** Think about data movement across the cluster.

<details>
<summary>Click to Reveal Answer</summary>

Narrow transformations (e.g., `map`, `filter`) operate on data within the same partition; each child partition depends on only one parent partition, requiring no data movement across the network. Wide transformations (e.g., `groupByKey`, `reduceByKey`, `join`) require data to be shuffled across partitions via the network because child partitions depend on multiple parent partitions.

This distinction matters for performance because shuffles are expensive operations involving network I/O, disk I/O, and serialization. Wide transformations create stage boundaries in Spark's execution plan. Minimizing wide transformations improves job performance.
</details>

---

### Q12: Why should you prefer `reduceByKey` over `groupByKey` for aggregations?
**Keywords:** Local aggregation, Shuffle size, Memory efficiency, Combiner
**Hint:** Think about where the aggregation happens.

<details>
<summary>Click to Reveal Answer</summary>

`reduceByKey` performs local aggregation on each partition before shuffling, similar to a combiner in MapReduce. This reduces the amount of data transferred during the shuffle phase. `groupByKey` shuffles all values for each key to a single partition before any aggregation happens, which can cause memory issues and excessive network transfer.

For example, summing counts: `reduceByKey` sends partial sums, while `groupByKey` sends every individual value. For large datasets, this difference can mean orders of magnitude less data shuffled.
</details>

---

### Q13: What are broadcast variables, and when would you use them?
**Keywords:** Read-only, Lookup table, Efficiency, Executor cache
**Hint:** Think about sharing large data across many tasks.

<details>
<summary>Click to Reveal Answer</summary>

Broadcast variables efficiently distribute large read-only data to all executors once, rather than shipping a copy with each task. You would use broadcast variables for lookup tables, feature dictionaries, or model parameters that need to be accessed by many tasks. Without broadcast, data is sent with each task, which for 1000 tasks with a 100MB lookup table would transfer 100GB instead of just 100MB (once per executor).

Use `sc.broadcast(data)` to create a broadcast variable and access it via `.value` in your functions.
</details>

---

### Q14: How do accumulators work, and what is their limitation regarding transformations?
**Keywords:** Aggregation, Driver, Guarantee, foreach, Task retry
**Hint:** Think about lazy evaluation and task re-execution.

<details>
<summary>Click to Reveal Answer</summary>

Accumulators allow tasks to add values that are aggregated back at the driver. They are useful for counters and sums across distributed operations. However, accumulator updates in transformations are NOT guaranteed to execute exactly once - if a task is retried due to failure, the accumulator may be updated multiple times. If a partition is not needed for the action, the update may not happen at all.

For guaranteed behavior, use accumulators within actions like `foreach()`. Accumulator values are only reliable after the action has completed.
</details>

---

### Q15: You have a Spark job that is running slowly. How would you approach performance tuning?
**Keywords:** Shuffle, Partition, Caching, Executor memory, Data skew
**Hint:** Consider the execution plan and resource utilization.

<details>
<summary>Click to Reveal Answer</summary>

Performance tuning approach:
1. **Check the Spark UI:** Look for slow stages, skewed tasks, and excessive shuffle.
2. **Filter early:** Move filters before expensive operations to reduce data volume.
3. **Minimize shuffles:** Use `reduceByKey` instead of `groupByKey`, avoid unnecessary repartitions.
4. **Cache strategically:** Cache RDDs/DataFrames that are reused multiple times.
5. **Tune partitions:** Adjust `spark.sql.shuffle.partitions` and partition count to match cluster parallelism (2-4 partitions per core).
6. **Address data skew:** Repartition or salt keys if some partitions have much more data.
7. **Resource allocation:** Increase executor memory for memory-intensive operations, ensure adequate cores for parallelism.
</details>

---

### Q16: Explain client mode vs cluster mode in Spark. When would you use each?
**Keywords:** Driver location, Resource management, Interactive, Production
**Hint:** Think about where the driver program runs.

<details>
<summary>Click to Reveal Answer</summary>

In **client mode**, the driver runs on the machine that submitted the application (e.g., your laptop or edge node). In **cluster mode**, the driver runs inside the cluster on one of the worker nodes.

Use client mode for:
- Interactive development and debugging
- Spark shells and notebooks
- When you need to see output directly

Use cluster mode for:
- Production jobs submitted to a cluster
- Long-running applications
- When the submitting machine should not be tied up
- Automated scheduling systems
</details>

---

### Q17: How would you handle a KeyError safely when accessing a dictionary in a PySpark transformation?
**Keywords:** .get() method, Default value, Exception handling
**Hint:** Think about the `.get()` method.

<details>
<summary>Click to Reveal Answer</summary>

Use the `.get()` method instead of direct key access. The `.get()` method allows you to provide a default value if the key is missing, preventing a KeyError crash.

```python
# Unsafe - may crash
value = my_dict[key]

# Safe - returns None or default if key missing
value = my_dict.get(key)
value = my_dict.get(key, "default_value")
```

This is especially important in distributed processing where you cannot easily predict all possible key values.
</details>

---

### Q18: What is the purpose of `mapPartitions`, and when would you use it over `map`?
**Keywords:** Partition-level, Setup cost, Database connection, Efficiency
**Hint:** Think about expensive initialization operations.

<details>
<summary>Click to Reveal Answer</summary>

`mapPartitions` applies a function to an entire partition at once, rather than element by element like `map`. Use it when you have expensive setup or teardown operations that should happen once per partition rather than once per element.

Common use cases:
- Database connections (open once per partition, not per record)
- File handles or network connections
- Loading machine learning models
- Batch API calls

```python
def process_partition(partition):
    connection = open_db_connection()  # Once per partition
    for record in partition:
        yield connection.process(record)
    connection.close()

rdd.mapPartitions(process_partition)
```
</details>

---

### Q19: Describe the differences between `union`, `intersect`, and `subtract` operations on DataFrames.
**Keywords:** Set operations, Combine, Common elements, Difference

<details>
<summary>Click to Reveal Answer</summary>

These are set operations for combining DataFrames:

- **union:** Combines all rows from both DataFrames (equivalent to SQL UNION ALL). Duplicates are kept.
- **intersect:** Returns only rows that exist in both DataFrames (equivalent to SQL INTERSECT). 
- **subtract:** Returns rows from the first DataFrame that do not exist in the second (equivalent to SQL EXCEPT or MINUS).
- **distinct:** Often used after union to remove duplicates (achieving SQL UNION behavior).

Note: These operations require compatible schemas between the DataFrames.
</details>

---

### Q20: What are the different join types in Spark, and when would you use each?
**Keywords:** inner, left, right, outer, semi, anti

<details>
<summary>Click to Reveal Answer</summary>

Spark supports these join types:

- **inner:** Returns only matching rows from both DataFrames. Use when you only want records with matches on both sides.
- **left (left_outer):** Returns all rows from left DataFrame plus matching rows from right. Use when you need all left records regardless of match.
- **right (right_outer):** Returns all rows from right DataFrame plus matching rows from left.
- **full (full_outer):** Returns all rows from both DataFrames, with nulls where no match exists.
- **left_semi:** Returns rows from left DataFrame where a match exists in right (like a filtered left table).
- **left_anti:** Returns rows from left DataFrame where NO match exists in right (finding orphan records).
- **cross:** Cartesian product of all rows (use with caution - can explode data size).
</details>

---

## Advanced (Deep Dive)

### Q21: Explain how Spark achieves fault tolerance without data replication. What role does lineage play?
**Keywords:** Lineage graph, Recomputation, DAG, Checkpoint, Partition recovery
**Hint:** Think about how Spark can rebuild lost data.

<details>
<summary>Click to Reveal Answer</summary>

Spark achieves fault tolerance through lineage tracking rather than data replication. Every RDD stores the sequence of transformations (lineage) used to create it from the original data source. When a partition is lost due to node failure, Spark can recompute just that partition by replaying the transformations on the original data.

This is more efficient than replication because:
1. No storage overhead for redundant copies
2. Only lost partitions are recomputed, not entire datasets
3. For narrow transformations, recovery only requires the parent partition

For very long lineage chains, checkpointing can be used to truncate the lineage by materializing an RDD to reliable storage (like HDFS), providing a recovery point.
</details>

---

### Q22: What happens under the hood when you call `collect()` on a large RDD? Why can this be dangerous?
**Keywords:** Driver memory, Network transfer, OutOfMemoryError, Serialization
**Hint:** Think about where the data ends up.

<details>
<summary>Click to Reveal Answer</summary>

When `collect()` is called:
1. All executor tasks serialize their partition data
2. Data is transferred over the network to the driver
3. Results are deserialized and combined into a single Python list in driver memory

This is dangerous for large RDDs because:
- The driver has limited memory (typically much less than cluster total)
- All data must fit in driver memory simultaneously
- Can cause OutOfMemoryError and application crash
- Network bandwidth becomes a bottleneck

Safer alternatives:
- Use `take(n)` to retrieve a limited sample
- Use `saveAsTextFile()` to write to distributed storage
- Use `toLocalIterator()` for streaming iteration
- Aggregate on the cluster with `reduce()` or DataFrame operations before collecting
</details>

---

### Q23: Design a strategy to handle severe data skew in a Spark job where one key has millions of records while others have only a few.
**Keywords:** Salting, Broadcast join, Two-stage aggregation, Repartition
**Hint:** Think about distributing the hot key across multiple partitions.

<details>
<summary>Click to Reveal Answer</summary>

Data skew creates stragglers (tasks that run much longer than others). Strategies include:

**1. Salting:** Add a random suffix to the skewed key to distribute it across partitions, then aggregate in two stages.
```python
# Stage 1: Aggregate with salted keys
salted = rdd.map(lambda x: ((x[0], random.randint(0, 9)), x[1]))
partial = salted.reduceByKey(lambda a, b: a + b)

# Stage 2: Remove salt and final aggregate
final = partial.map(lambda x: (x[0][0], x[1])).reduceByKey(lambda a, b: a + b)
```

**2. Broadcast Join:** If one side is small enough, broadcast it to avoid shuffle entirely.

**3. Adaptive Query Execution (Spark 3.0+):** Enable `spark.sql.adaptive.enabled` to automatically handle skew.

**4. Isolate and Separate:** Process the skewed key separately from others, then union results.
</details>

---

### Q24: Explain the memory architecture of a Spark executor. How is memory divided, and what happens when execution memory is exhausted?
**Keywords:** Reserved memory, User memory, Execution memory, Storage memory, Spill to disk

<details>
<summary>Click to Reveal Answer</summary>

Executor memory is divided into:

1. **Reserved Memory (~300MB):** Internal Spark overhead, not configurable.

2. **User Memory (~40% of remaining):** For data structures in user code, UDFs, and metadata.

3. **Spark Memory (~60% of remaining):** Shared pool for:
   - **Execution Memory:** Shuffles, joins, sorts, aggregations
   - **Storage Memory:** Cached RDDs and broadcast variables

The execution and storage pools can borrow from each other (unified memory management since Spark 1.6). Storage can be evicted to make room for execution, but execution cannot evict itself.

When execution memory is exhausted:
1. Spark spills data to disk (shuffle files)
2. Performance degrades significantly due to disk I/O
3. If spill space is also exhausted, OutOfMemoryError occurs

Tuning involves adjusting `spark.memory.fraction` and `spark.memory.storageFraction`.
</details>

---

### Q25: How would you optimize a PySpark job that performs multiple actions on the same transformed DataFrame?
**Keywords:** Caching, Persist, Storage level, Unpersist, DAG recomputation
**Hint:** Think about avoiding redundant computation.

<details>
<summary>Click to Reveal Answer</summary>

Without caching, Spark recomputes the entire transformation chain for each action, reading from source data multiple times.

**Solution: Cache or persist the intermediate DataFrame.**

```python
# Transform data
transformed_df = raw_df.filter(...).join(...).groupBy(...).agg(...)

# Cache before multiple actions
transformed_df.cache()  # or .persist(StorageLevel.MEMORY_AND_DISK)

# Now multiple actions share cached data
count = transformed_df.count()
sample = transformed_df.take(10)
transformed_df.write.parquet("output/")

# Clean up when done
transformed_df.unpersist()
```

**Storage level options:**
- `MEMORY_ONLY`: Fastest but may not fit
- `MEMORY_AND_DISK`: Spills to disk if needed
- `MEMORY_ONLY_SER`: Serialized (less memory, more CPU)
- `DISK_ONLY`: When memory is constrained

Always unpersist when data is no longer needed to free resources.
</details>

---

## Study Tips

1. **Practice explaining concepts out loud** - Interviewers assess communication skills as much as technical knowledge.
2. **Know the "why" behind each concept** - Understanding motivation helps you answer follow-up questions.
3. **Be ready to discuss trade-offs** - There is rarely a single correct answer; discuss pros and cons.
4. **Prepare real examples** - Relate concepts to work you have done in exercises and projects.
5. **Understand the Spark UI** - Many interviews include debugging scenarios using execution metrics.

---

*Generated by Quality Assurance Agent based on PySpark Week 1-2 curriculum content.*
