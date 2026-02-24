# Interview Questions: Week 3 - Apache Kafka

This question bank prepares trainees for technical interviews covering Apache Kafka fundamentals, producer-consumer patterns, message delivery semantics, and Kafka-Spark integration.

---

## Beginner (Foundational)

### Q1: What is Apache Kafka, and what are its three core capabilities?
**Keywords:** Event streaming platform, Publish-subscribe, Storage, Processing

<details>
<summary>Click to Reveal Answer</summary>

Apache Kafka is an open-source distributed event streaming platform. Its three core capabilities are:
1. **Publish and Subscribe:** Applications can send (publish) and receive (subscribe to) streams of records
2. **Store:** Records are stored durably and reliably for as long as needed
3. **Process:** Streams of records can be processed as they occur

Kafka acts as a highly scalable, fault-tolerant messaging system at the center of data infrastructure.
</details>

---

### Q2: What are the "four pillars" of Kafka architecture?
**Keywords:** Topics, Producers, Consumers, Brokers

<details>
<summary>Click to Reveal Answer</summary>

The four pillars of Kafka architecture are:
1. **Topics:** Named categories where messages are stored; divided into partitions
2. **Producers:** Applications that write (publish) messages to topics
3. **Consumers:** Applications that read (subscribe to) messages from topics
4. **Brokers:** Kafka servers that store topic data and handle client requests

These components work together to create a robust event streaming platform where producers and consumers are decoupled from each other.
</details>

---

### Q3: What is a Kafka topic, and what are its key characteristics?
**Keywords:** Named channel, Partitions, Append-only, Retention

<details>
<summary>Click to Reveal Answer</summary>

A Kafka topic is a category or feed name to which records are published. Key characteristics include:
- Topics are identified by a unique name (e.g., `user-events`, `transactions`)
- Each topic is divided into one or more **partitions** for parallelism
- Topics are **append-only**: you can only add new messages, not modify existing ones
- Messages are retained for a configurable period (default: 7 days)

Topics act like named channels or mailboxes where messages are stored.
</details>

---

### Q4: What is a Kafka broker, and what does it do?
**Keywords:** Server, Storage, Client requests, Replication, Cluster

<details>
<summary>Click to Reveal Answer</summary>

A Kafka broker is a server that stores data and serves client requests. A Kafka cluster consists of one or more brokers. Each broker:
- Is identified by a unique ID
- Stores topic partitions on disk
- Handles read/write requests from producers and consumers
- Coordinates with other brokers for replication and fault tolerance

Brokers are like post offices that receive messages from producers, store them temporarily, and deliver them when consumers request them.
</details>

---

### Q5: Explain why producers are "decoupled" from consumers in Kafka.
**Keywords:** Independent scaling, Topic name, Fault tolerance, No direct connection

<details>
<summary>Click to Reveal Answer</summary>

In Kafka, producers are decoupled from consumers because:
- Producers do not need to know who consumes their data
- Consumers do not need to know who produces the data
- They only share knowledge of the **topic name**

This decoupling enables:
- Independent scaling of producers and consumers
- Adding new consumers without changing producers
- Fault tolerance (if a consumer fails, messages are not lost since they persist in the topic)
</details>

---

### Q6: What is a partition in Kafka, and why does it matter?
**Keywords:** Parallelism, Ordering, Offset, Distribution

<details>
<summary>Click to Reveal Answer</summary>

A partition is a subset of a topic's data. Each topic is divided into one or more partitions. Partitions matter because:
- **Parallelism:** Multiple consumers can read from different partitions simultaneously
- **Scalability:** Partitions can be spread across multiple brokers
- **Ordering:** Messages within a single partition are strictly ordered (but not across partitions)

Each message within a partition has a sequential ID called an **offset** that uniquely identifies it.
</details>

---

### Q7: What is a consumer offset in Kafka?
**Keywords:** Position, Sequential ID, Commit, __consumer_offsets

<details>
<summary>Click to Reveal Answer</summary>

An offset is a sequential ID that uniquely identifies each message within a partition. Consumers track their position in a topic using offsets. Key points:
- Offsets are stored in a special internal topic called `__consumer_offsets`
- Each consumer group maintains its own offset per partition
- Consumers can commit offsets automatically or manually
- If a consumer restarts, it resumes from its last committed offset
</details>

---

### Q8: What is the difference between batch processing and stream processing?
**Keywords:** Data at rest, Data in motion, Latency, Real-time

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | Batch Processing | Stream Processing |
|--------|------------------|-------------------|
| **Data** | Data at rest (bounded, complete) | Data in motion (unbounded, continuous) |
| **Timing** | Process after all data arrives | Process as data arrives |
| **Latency** | Minutes to hours | Seconds to milliseconds |
| **Use Case** | Historical analysis, ETL | Real-time alerts, dashboards |

Kafka enables stream processing (real-time reactions), while Spark traditionally handles batch processing (historical analysis).
</details>

---

### Q9: What is the difference between DStreams and Structured Streaming in Spark?
**Keywords:** RDD-based, DataFrame-based, Legacy, Modern, Micro-batch

<details>
<summary>Click to Reveal Answer</summary>

| API | DStreams | Structured Streaming |
|-----|----------|---------------------|
| **Status** | Legacy (deprecated) | Modern (recommended) |
| **Foundation** | RDD-based | DataFrame-based |
| **Processing** | Manual state management | Native event-time and watermarks |
| **API Style** | Low-level | Uses same API as batch processing |
| **Guarantees** | At-least-once | Exactly-once support |

Structured Streaming treats a live data stream as an **unbounded table** that is continuously appended with new data, making it intuitive for SQL users.
</details>

---

### Q10: What is micro-batch processing?
**Keywords:** Time interval, Batch efficiency, Fault tolerance, Trigger

<details>
<summary>Click to Reveal Answer</summary>

Micro-batch processing collects events over a small time interval (the trigger interval) and processes them as a batch, rather than processing each event individually.

**Advantages:**
- Efficiency: Batch operations are more efficient than per-record processing
- Fault Tolerance: If a batch fails, only that batch needs to be reprocessed
- Consistency: Easier to guarantee exactly-once semantics
- Familiar APIs: Reuse batch processing logic

The trade-off is higher latency (seconds rather than milliseconds), but this is acceptable for most use cases.
</details>

---

## Intermediate (Application)

### Q11: Explain the three message delivery semantics in Kafka: at-most-once, at-least-once, and exactly-once.
**Keywords:** Data loss, Duplicates, Commit order, Idempotency
**Hint:** Think about when the offset is committed relative to processing.

<details>
<summary>Click to Reveal Answer</summary>

| Semantic | Description | Risk | Configuration |
|----------|-------------|------|---------------|
| **At-most-once** | Messages may be lost, never duplicated | Data loss | Commit offset BEFORE processing |
| **At-least-once** | Messages never lost, may be duplicated | Duplicates | Commit offset AFTER processing |
| **Exactly-once** | Messages delivered exactly one time | None | Idempotent producer + transactions |

**Implementation:**
- At-most-once: `enable_auto_commit=True` (commits immediately)
- At-least-once: `enable_auto_commit=False` (manual commit after processing)
- Exactly-once: `enable_idempotence=True` + transactional producer

At-least-once is most common and requires **idempotent** processing logic (applying the same message twice has no additional effect).
</details>

---

### Q12: What is the purpose of the producer `acks` setting, and what are the three options?
**Keywords:** Durability, Latency, Leader, Replicas, Trade-off
**Hint:** Consider the trade-off between durability and performance.

<details>
<summary>Click to Reveal Answer</summary>

The `acks` setting controls how many acknowledgments the producer requires before considering a message successfully sent:

| Setting | Behavior | Durability | Latency |
|---------|----------|------------|---------|
| `acks=0` | Fire and forget; no acknowledgment | None | Lowest |
| `acks=1` | Leader broker acknowledges | Medium | Low |
| `acks='all'` (-1) | All in-sync replicas acknowledge | Highest | Higher |

**When to use:**
- `acks=0`: Metrics/logs where some loss is acceptable
- `acks=1`: Most general use cases
- `acks='all'`: Critical data, financial transactions
</details>

---

### Q13: What is a consumer group, and how does Kafka assign partitions to consumers in a group?
**Keywords:** Parallel processing, Partition assignment, Rebalancing, Scaling
**Hint:** Think about the relationship between partition count and consumer count.

<details>
<summary>Click to Reveal Answer</summary>

A consumer group is a set of consumers that cooperate to consume data from topics. Key rules:
- Each partition is assigned to exactly **one** consumer within a group
- If consumers < partitions: some consumers handle multiple partitions
- If consumers > partitions: some consumers sit idle
- Different consumer groups read the topic independently

When consumers join or leave a group, Kafka **rebalances** partition assignments automatically. For optimal parallelism, partition count should be >= consumer count. Consumer groups enable horizontal scaling of message consumption.
</details>

---

### Q14: What is the difference between a Kafka leader and follower replica? What happens when a leader fails?
**Keywords:** Replication, ISR, Election, Fault tolerance
**Hint:** Think about which replica handles client requests.

<details>
<summary>Click to Reveal Answer</summary>

- **Leader:** The replica that handles all reads and writes for a partition
- **Followers:** Replicas that copy data from the leader to maintain redundancy
- **ISR (In-Sync Replicas):** Followers that are fully caught up with the leader

When a leader fails:
1. The Kafka cluster detects the failure
2. An in-sync follower is elected as the new leader
3. Producers and consumers automatically connect to the new leader
4. No data is lost (if the elected follower was in the ISR)

This automatic failover is how Kafka achieves high availability.
</details>

---

### Q15: How does message partitioning work when a producer sends messages to a topic?
**Keywords:** Key, Hash, Round-robin, Ordering guarantee
**Hint:** Consider messages with and without keys.

<details>
<summary>Click to Reveal Answer</summary>

When a producer sends a message:

**With a key:**
- The hash of the key determines the partition
- Messages with the same key always go to the same partition
- This guarantees ordering for messages with the same key

**Without a key:**
- Round-robin distribution across partitions
- Maximizes parallelism but no ordering guarantee

```python
# Same key = same partition (ordered)
producer.send("orders", key="customer-123", value="order-1")  # -> Partition 2
producer.send("orders", key="customer-123", value="order-2")  # -> Partition 2

# No key = round-robin (unordered)
producer.send("logs", value="log entry 1")  # -> Partition 0
producer.send("logs", value="log entry 2")  # -> Partition 1
```
</details>

---

### Q16: What is an idempotent producer, and why would you enable it?
**Keywords:** Duplicates, Retries, Sequence number, Exactly-once
**Hint:** Think about what happens during network failures with retries enabled.

<details>
<summary>Click to Reveal Answer</summary>

An idempotent producer prevents duplicate messages during retries. When enabled:
- The producer assigns sequence numbers to messages
- The broker detects and discards duplicates based on these sequence numbers
- Guarantees exactly-once delivery from producer to broker

**Why enable it:**
- Network issues may cause the producer to retry a send even if the broker received it
- Without idempotence, this results in duplicate messages
- With idempotence, the broker recognizes the duplicate and acknowledges without storing again

Configuration: `enable_idempotence=True` (requires `acks='all'`)
</details>

---

### Q17: What are the producer batching settings `batch_size` and `linger_ms`, and how do they affect performance?
**Keywords:** Throughput, Latency, Network efficiency, Trade-off
**Hint:** Consider the trade-off between waiting for more messages and sending immediately.

<details>
<summary>Click to Reveal Answer</summary>

Batching settings control how the producer groups messages before sending:

- **`batch_size`**: Maximum batch size in bytes (default: 16 KB)
- **`linger_ms`**: Maximum time to wait for more messages before sending (default: 0)

**Trade-offs:**
- Higher `linger_ms` = more messages per batch = higher throughput, but higher latency
- Lower `linger_ms` = messages sent faster = lower latency, but more network overhead
- Larger `batch_size` = bigger batches when traffic is high

**Example configuration for throughput optimization:**
```python
producer = KafkaProducer(
    batch_size=32768,  # 32 KB batches
    linger_ms=20       # Wait up to 20ms for batch to fill
)
```
</details>

---

### Q18: Why are Kafka and Spark commonly used together in real-time pipelines?
**Keywords:** Ingestion, Processing, Complementary roles, Scalability
**Hint:** Think about what each technology does best.

<details>
<summary>Click to Reveal Answer</summary>

Kafka and Spark serve complementary roles:

| Component | Role | Strength |
|-----------|------|----------|
| **Kafka** | Message broker / event bus | High-throughput, durable, distributed messaging |
| **Spark** | Processing engine | Complex transformations, aggregations, ML |

Kafka handles **ingestion and distribution** (the highway that moves data), while Spark handles **transformation and analytics** (the factory that processes data).

Together they enable:
- Processing streaming data at scale using Spark's distributed computing
- Complex transformations (joins, aggregations, windowing) on live data
- End-to-end real-time pipelines from ingestion to analytics
</details>

---

### Q19: What is the difference between ZooKeeper mode and KRaft mode in Kafka?
**Keywords:** Cluster coordination, Metadata, Simplified deployment, Modern
**Hint:** Think about external dependencies.

<details>
<summary>Click to Reveal Answer</summary>

Kafka requires a coordinator to manage cluster metadata, elect leaders, and handle configuration.

**ZooKeeper Mode (Legacy):**
- Requires a separate ZooKeeper cluster
- Stores cluster metadata externally
- Additional operational complexity
- Being phased out

**KRaft Mode (Modern):**
- Kafka manages its own metadata (no external dependencies)
- Simplified deployment and operations
- Better performance at scale
- Recommended for new deployments (Kafka 3.0+)

KRaft eliminates the need to run and manage a separate ZooKeeper ensemble.
</details>

---

### Q20: Explain how to configure a consumer for at-least-once delivery semantics.
**Keywords:** Manual commit, Processing order, Auto-commit disabled
**Hint:** The key is when you commit the offset relative to processing.

<details>
<summary>Click to Reveal Answer</summary>

For at-least-once delivery, commit the offset **after** successful processing:

```python
consumer = KafkaConsumer(
    'events',
    bootstrap_servers='localhost:9092',
    group_id='my-consumer-group',
    enable_auto_commit=False  # Disable automatic commits
)

for message in consumer:
    try:
        process(message)       # Process first
        consumer.commit()      # Then commit offset
    except Exception as e:
        # If processing fails, offset is not committed
        # Message will be re-delivered on next poll
        handle_error(e)
```

This ensures no message is lost, but messages may be reprocessed if the consumer crashes after processing but before committing. Your processing logic should be **idempotent** to handle this.
</details>

---

## Advanced (Deep Dive)

### Q21: Design a fault-tolerant Kafka producer for a financial transaction system. What configuration settings would you choose and why?
**Keywords:** acks, idempotence, retries, durability, exactly-once
**Hint:** Financial data cannot be lost or duplicated.

<details>
<summary>Click to Reveal Answer</summary>

For financial transactions requiring maximum reliability:

```python
producer = KafkaProducer(
    bootstrap_servers='broker1:9092,broker2:9092,broker3:9092',
    
    # Maximum durability
    acks='all',                    # Wait for all ISRs
    enable_idempotence=True,       # Prevent duplicates on retry
    
    # Retry configuration
    retries=5,                     # Retry transient failures
    retry_backoff_ms=100,          # Wait between retries
    delivery_timeout_ms=120000,    # Total time to attempt delivery
    
    # For exactly-once across topics
    transactional_id='txn-producer-001'  # Enable transactions
)

# Transaction usage
producer.init_transactions()
producer.begin_transaction()
try:
    producer.send('transactions', value=transaction_data)
    producer.commit_transaction()
except Exception:
    producer.abort_transaction()
```

**Why these settings:**
- `acks='all'`: Message survives any single broker failure
- `enable_idempotence=True`: No duplicates during retries
- Transactions: Atomic writes across multiple topics
- Multiple bootstrap servers: Survives individual broker failures at connection time
</details>

---

### Q22: A Kafka Spark Streaming job is experiencing significant lag. How would you diagnose and address this issue?
**Keywords:** Consumer lag, Partitions, Parallelism, Backpressure, Checkpointing
**Hint:** Consider both Kafka and Spark configuration.

<details>
<summary>Click to Reveal Answer</summary>

**Diagnosis steps:**

1. **Check consumer lag:**
   - Use `kafka-consumer-groups.sh --describe --group <group>` to see lag per partition
   - Lag indicates how many messages behind the consumer is

2. **Check Spark UI:**
   - Look for slow stages or skewed tasks
   - Check processing time vs. trigger interval

**Common causes and solutions:**

| Issue | Solution |
|-------|----------|
| Too few partitions | Increase topic partitions (enables more parallel consumers) |
| Insufficient Spark parallelism | Increase `spark.executor.cores` or executor count |
| Processing too slow | Optimize transformations, increase batch interval |
| Checkpointing overhead | Use faster storage for checkpoint location |
| Memory pressure | Increase executor memory, tune `spark.memory.fraction` |

**Configuration to address lag:**
- Match Spark parallelism to Kafka partition count
- Increase `maxOffsetsPerTrigger` if processing can handle more
- Enable back-pressure: `spark.streaming.backpressure.enabled=true`
</details>

---

### Q23: Explain how Spark Streaming achieves exactly-once processing guarantees when reading from Kafka. What role do offsets and checkpointing play?
**Keywords:** Offset tracking, Checkpoint, Idempotent sinks, Atomic commit
**Hint:** Think about what state needs to survive a failure.

<details>
<summary>Click to Reveal Answer</summary>

Spark Streaming achieves exactly-once through a combination of mechanisms:

**1. Offset Management:**
- Spark reads messages from Kafka and tracks the offsets
- Offsets are stored in Spark's checkpoint, not committed to Kafka immediately
- This ensures Spark controls which messages have been processed

**2. Checkpointing:**
- Spark periodically saves its state (including offsets) to reliable storage
- If a failure occurs, Spark restarts from the last checkpoint
- This guarantees no message is skipped

**3. Atomic Microbatch Processing:**
- Each microbatch is processed atomically
- Either all records in the batch are processed, or none are
- Output is written and offset is updated atomically

**4. Idempotent Sinks:**
- For true exactly-once, output sinks must be idempotent
- Writing the same record twice should have no additional effect
- Example: Use upserts with unique keys

**Flow:**
```
Read Kafka -> Process -> Write Output + Checkpoint Offset (atomic)
```

If failure occurs after checkpoint, Spark resumes from saved offset and reprocesses. The idempotent sink handles the potential rewrite.
</details>

---

### Q24: What are the performance trade-offs between different Kafka compression algorithms? When would you choose each?
**Keywords:** gzip, snappy, lz4, zstd, CPU, Bandwidth
**Hint:** Consider network bandwidth vs. CPU usage in different environments.

<details>
<summary>Click to Reveal Answer</summary>

**Compression comparison:**

| Algorithm | Compression Ratio | CPU Usage | Speed | Best For |
|-----------|-------------------|-----------|-------|----------|
| None | 1.0x | Lowest | Fastest | Low-latency, fast network |
| Snappy | ~1.5x | Low | Fast | Low-latency with moderate compression |
| LZ4 | ~2.0x | Low | Fast | **General purpose (recommended)** |
| GZIP | ~2.5x | Medium | Medium | Bandwidth-constrained, archival |
| ZSTD | ~3.0x | Medium | Medium | Best ratio, modern systems |

**Decision factors:**

- **Network-bound (cloud, WAN):** Use GZIP or ZSTD for maximum compression
- **CPU-bound (high volume):** Use LZ4 or Snappy for minimal CPU overhead
- **Low-latency requirements:** Use LZ4 or no compression
- **Disk storage concerns:** Use ZSTD for best ratio with good speed

**Recommendation:** LZ4 is the best general-purpose choice, offering good compression with minimal CPU overhead.
</details>

---

### Q25: Design the architecture for a real-time fraud detection system using Kafka and Spark. What components would you include and how would they interact?
**Keywords:** Event sourcing, Windowing, State management, Alerting, Scalability
**Hint:** Consider the full pipeline from data ingestion to action.

<details>
<summary>Click to Reveal Answer</summary>

**Architecture:**

```
+----------------+     +------------------+     +------------------+
| Payment Apps   | --> | Kafka Topic      | --> | Spark Streaming  |
| (Producers)    |     | (transactions)   |     | (ML Inference)   |
+----------------+     +------------------+     +--------+---------+
                                                         |
                       +---------------------------+-----+
                       |                           |
                       v                           v
              +----------------+          +----------------+
              | Kafka Topic    |          | Alert Service  |
              | (fraud-alerts) |          | (Real-time)    |
              +-------+--------+          +----------------+
                      |
                      v
              +----------------+
              | Investigation  |
              | Dashboard      |
              +----------------+
```

**Components:**

1. **Kafka Producers:** Payment gateways, e-commerce apps send transaction events
2. **Transactions Topic:** Partitioned by customer ID for ordering guarantees
3. **Spark Streaming Application:**
   - Reads from transactions topic
   - Applies ML model for fraud scoring
   - Maintains stateful aggregations (velocity checks, spending patterns)
   - Uses windowing for time-based analysis (e.g., transactions in last 5 minutes)
4. **Fraud Alerts Topic:** High-risk transactions published here
5. **Alert Service:** Consumes alerts, triggers immediate actions (block card, notify)
6. **Investigation Dashboard:** Historical view for analysts

**Key design decisions:**
- Partition by customer ID to ensure all transactions for a customer go to the same partition (ordering)
- Use `acks='all'` for producers (cannot lose transactions)
- Checkpoint Spark state for fault tolerance
- Separate topics for different data flows (separation of concerns)
</details>

---

## Study Tips

1. **Understand the pub-sub model** - Kafka interview questions often start with "explain how producers and consumers work."
2. **Know the delivery semantics** - Be able to explain at-least-once vs. exactly-once and when to use each.
3. **Practice configuration** - Be ready to explain the purpose of key settings like `acks`, `retries`, and `batch_size`.
4. **Connect Kafka to Spark** - Many data engineering roles require both; know how they work together.
5. **Think about failure scenarios** - Interviewers love asking "what happens if X fails?"

---

*Generated by Quality Assurance Agent based on Kafka Week 3 curriculum content.*
