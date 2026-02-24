# Spark Streaming Troubleshooting

## Learning Objectives

- Identify common issues in Spark Streaming applications
- Diagnose and resolve backpressure and consumer lag
- Troubleshoot memory-related problems in streaming jobs
- Use the Spark UI to monitor and debug streaming applications
- Apply best practices to prevent common streaming failures

## Why This Matters

Building a streaming application is one thing; running it reliably in production is another. Streaming jobs run **continuously**, meaning issues like memory leaks, processing delays, or configuration problems can compound over time and lead to failures.

As a data engineer, you will inevitably encounter streaming issues. This content equips you with the knowledge to **diagnose problems quickly** and **apply fixes confidently**, ensuring your Kafka-Spark pipelines remain stable.

## The Concept

### Common Streaming Issues Overview

| Issue | Symptom | Root Cause |
|-------|---------|------------|
| **Backpressure** | Processing falls behind input rate | Slow transformations, insufficient resources |
| **Consumer Lag** | Kafka offsets fall behind | Processing slower than production rate |
| **Memory Issues** | OOM errors, GC pauses | State accumulation, large shuffles |
| **Checkpoint Failures** | Job restarts from scratch | Corrupted checkpoints, storage issues |
| **Data Skew** | Some tasks much slower | Uneven partition distribution |

### Backpressure

**What is Backpressure?**

Backpressure occurs when your streaming application **cannot process data as fast as it arrives**. The processing pipeline becomes a bottleneck, causing data to queue up.

```
Production Rate:   1000 events/sec
Processing Rate:    800 events/sec
                        |
                        v
              Backpressure builds up
              Queue grows over time
```

**Symptoms:**
- Increasing batch processing times
- Growing delay between event time and processing time
- Memory usage increasing over time

**Diagnosis:**

1. Check the Spark UI Streaming tab for processing time trends
2. Compare input rate vs. processing rate
3. Look for slow stages in the Jobs tab

**Solutions:**

| Solution | When to Apply |
|----------|---------------|
| **Increase parallelism** | Processing is CPU-bound |
| **Add more executors** | Need more compute resources |
| **Optimize transformations** | Inefficient code (e.g., UDFs) |
| **Increase trigger interval** | Allow larger, more efficient batches |
| **Reduce input rate** | Limit Kafka consumer rate |

**Limiting Kafka Input Rate:**

```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .option("maxOffsetsPerTrigger", 10000)  # Limit records per batch
    .load()
```

### Consumer Lag

**What is Consumer Lag?**

Lag is the difference between the **latest message offset in Kafka** and the **offset your consumer has processed**. High lag means your application is falling behind.

```
Kafka Topic Offset:     1,000,000
Consumer Offset:          950,000
                              |
Lag: 50,000 messages behind  <-
```

**Monitoring Lag:**

Use Kafka command-line tools:

```bash
# Check consumer group lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --group spark-kafka-source-* \
    --describe
```

Output shows `LAG` column for each partition.

**Diagnosis Checklist:**

1. Is the Spark job running? (Check for failures)
2. Is processing time increasing? (Backpressure)
3. Are there problematic partitions? (Data skew)
4. Did the job restart recently? (Processing historical data)

**Solutions:**

- **Scale out**: Add more Spark executors
- **Scale up**: Increase executor memory/cores
- **Optimize**: Reduce transformation complexity
- **Rebalance**: Ensure Kafka partitions are evenly distributed

### Memory Issues

Memory problems are common in long-running streaming jobs, especially those with stateful operations.

**Out of Memory (OOM) Errors:**

```
java.lang.OutOfMemoryError: Java heap space
```

**Common Causes:**

| Cause | Description |
|-------|-------------|
| **Unbounded state** | State grows without limits (e.g., groupBy without watermark) |
| **Large shuffles** | Aggregations or joins that exceed memory |
| **Driver overload** | Too much data collected to driver |
| **Memory leaks** | Objects not released properly |

**Diagnosing Memory Issues:**

1. Check Spark UI Executors tab for memory usage
2. Look for GC (Garbage Collection) time in executor logs
3. Monitor state size in the Streaming tab

**Solutions:**

**For unbounded state, use watermarks:**

```python
# BAD: State grows forever
df.groupBy("user_id").count()

# GOOD: State is bounded by watermark
df.withWatermark("timestamp", "1 hour") \
    .groupBy("user_id", window("timestamp", "10 minutes")) \
    .count()
```

**Increase executor memory:**

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryOverhead", "1g") \
    .getOrCreate()
```

**Tune shuffle partitions:**

```python
# Default is 200, may need adjustment
spark.conf.set("spark.sql.shuffle.partitions", 100)
```

### Checkpoint Failures

Checkpoints are critical for fault tolerance. Issues here can cause data loss or duplicate processing.

**Common Checkpoint Issues:**

| Issue | Cause | Solution |
|-------|-------|----------|
| **Checkpoint not found** | Path changed or deleted | Never change checkpoint path for running queries |
| **Incompatible checkpoint** | Schema or code changed | Start fresh with new checkpoint (may lose state) |
| **Storage failures** | Disk full, permissions | Monitor storage, use reliable storage (HDFS, S3) |
| **Slow checkpointing** | Large state, slow storage | Use faster storage, reduce state size |

**Best Practices:**

```python
# Use a reliable, distributed storage system for checkpoints
query = df.writeStream \
    .option("checkpointLocation", "hdfs:///checkpoints/my-query") \
    .start()

# Or cloud storage
query = df.writeStream \
    .option("checkpointLocation", "s3a://bucket/checkpoints/my-query") \
    .start()
```

**Recovering from Checkpoint Issues:**

1. If checkpoint is corrupted, you may need to start fresh
2. Consider the trade-offs: lose state vs. reprocess data
3. For Kafka, you can specify `startingOffsets` to control where to restart

### Using Spark UI for Debugging

The Spark UI is your primary tool for diagnosing streaming issues.

**Key Tabs:**

| Tab | What to Look For |
|-----|------------------|
| **Streaming** | Input rate, processing time, batch duration |
| **Jobs** | Failed jobs, slow stages |
| **Stages** | Task distribution, shuffle read/write |
| **Executors** | Memory usage, GC time, failed tasks |
| **SQL** | Query plans, physical operators |

**Streaming Tab Metrics:**

```
Input Rate:        1000 records/sec    <- How fast data arrives
Process Rate:       950 records/sec    <- How fast you process
Batch Duration:      2.1 seconds       <- Time per micro-batch
Operation Duration:  1.8 seconds       <- Actual processing time
```

**Warning Signs:**
- Batch Duration > Trigger Interval (falling behind)
- Process Rate < Input Rate (backpressure)
- Increasing memory usage over time (state/memory leak)

### Data Skew

Data skew occurs when some partitions have significantly more data than others, causing some tasks to take much longer.

**Symptoms:**
- A few tasks take 10x longer than others
- Most executors are idle while a few are overloaded
- Timeouts on specific partitions

**Diagnosis:**

1. Check the Stages tab for task duration distribution
2. Look for partition sizes in shuffle reads

**Solutions:**

**Salting Keys:**

```python
from pyspark.sql.functions import concat, lit, rand

# Add salt to skewed key to distribute data
salted_df = df.withColumn(
    "salted_key", 
    concat(col("skewed_key"), lit("_"), (rand() * 10).cast("int"))
)

# Aggregate on salted key, then aggregate again to combine
```

**Repartitioning:**

```python
# Repartition to distribute data more evenly
df = df.repartition(100, "key_column")
```

## Troubleshooting Checklist

When a streaming job fails or underperforms:

1. **Check if the job is running** - Look for exceptions in driver logs
2. **Check the Streaming tab** - Is processing keeping up with input?
3. **Check the Executors tab** - Memory issues? GC problems?
4. **Check the Stages tab** - Any slow tasks? Data skew?
5. **Check Kafka lag** - Is the consumer falling behind?
6. **Check checkpoints** - Any storage/compatibility issues?
7. **Review recent changes** - Did you change code, configs, or schema?

## Summary

- **Backpressure** means processing cannot keep up with input; scale resources or optimize code
- **Consumer lag** indicates Kafka offset is falling behind; monitor with Kafka CLI tools
- **Memory issues** often stem from unbounded state; use watermarks to limit state size
- **Checkpoint failures** can cause data loss; use reliable storage and never change paths
- **Data skew** causes uneven task distribution; use salting or repartitioning
- The **Spark UI** is essential for diagnosing all streaming issues

## Additional Resources

- [Spark Structured Streaming - Monitoring](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#monitoring-streaming-queries)
- [Databricks: Troubleshooting Streaming Applications](https://docs.databricks.com/structured-streaming/troubleshooting.html)
- [Kafka Consumer Lag Monitoring](https://kafka.apache.org/documentation/#basic_ops_consumer_lag)
