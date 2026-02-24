# Mock Interview Study Guide
## Data Engineering: Airflow, Kafka & Spark


## Apache Airflow (25 Questions)

### Core Concepts
1. What is Apache Airflow and what problem does it solve?
2. What is a DAG? Why is it called "directed acyclic"?
3. What is the Airflow Scheduler and what does it do?
4. What is the Airflow Executor? Name three types.
5. What is `catchup` and when would you disable it?
6. What is `start_date` and how does it affect DAG runs?
7. What is `execution_date` vs the actual run time?

### Operators & Tasks
8. What is an Operator in Airflow?
9. What is the difference between `BashOperator` and `PythonOperator`?
10. How do you define task dependencies using `>>` and `<<`?
11. What is a Sensor? Give an example use case.
12. What is `trigger_rule`? Name two options.
13. How do retries work? What is `retry_delay`?
14. What is the difference between `BranchPythonOperator` and `ShortCircuitOperator`?

### XComs, Variables & Templating
15. What is an XCom?
16. How do you push and pull XCom values?
17. What is an Airflow Variable?
18. What is an Airflow Connection?
19. What is Jinja templating in Airflow?
20. What does `{{ ds }}` represent?
21. What is `default_args` and why is it useful?

### Operations
22. A task is stuck in "queued" state. What would you check?
23. How do you manually trigger a DAG run?
24. What is backfilling and when would you use it?
25. How do you view logs for a specific task run?

---

## Apache Kafka (25 Questions)

### Core Concepts
1. What is Apache Kafka and what problem does it solve?
2. What is the publish-subscribe messaging pattern?
3. How does Kafka differ from a traditional message queue?
4. What is a Kafka broker?
5. What is the role of Zookeeper in Kafka?

### Topics & Partitions
6. What is a Kafka topic?
7. What is a partition?
8. Why are partitions important for scalability?
9. What is a message key and how does it affect partitioning?
10. What is partition ordering guarantee?

### Producers
11. What is a Kafka producer?
12. What is the `acks` configuration? What do values 0, 1, and "all" mean?
13. What is producer batching?
14. What is `linger.ms`?

### Consumers
15. What is a Kafka consumer?
16. What is a consumer group?
17. What happens when you add a consumer to a consumer group?
18. What is an offset?
19. What is consumer commit?
20. What is consumer lag?
21. What is a rebalance?

### Delivery & Replication
22. What are the three message delivery semantics?
23. What is replication factor?
24. What is ISR (In-Sync Replica)?
25. What is the default message retention period?

---

## Apache Spark (30 Questions)

### Core Concepts
1. What is Apache Spark and what problem does it solve?
2. How does Spark differ from Hadoop MapReduce?
3. What is in-memory processing and why is it faster?
4. What is PySpark?

### RDDs & DataFrames
5. What is an RDD (Resilient Distributed Dataset)?
6. What does "resilient" mean in RDD?
7. When would you use RDDs vs DataFrames?
8. What is a DataFrame in Spark?
9. How does a DataFrame differ from an RDD?
10. What is the Catalyst optimizer?

### Transformations & Actions
11. What is a transformation in Spark?
12. What is an action in Spark?
13. What is lazy evaluation?
14. Why does Spark use lazy evaluation?
15. What is the difference between `map()` and `flatMap()`?

### Spark SQL
16. What is Spark SQL?
17. How do you register a DataFrame as a temporary view?
18. How do you run SQL queries on a DataFrame?

### Cluster Architecture
19. What is the Spark Driver?
20. What is an Executor?
21. What is a Spark job, stage, and task? How are they related?

### Partitioning & Performance
22. What is a partition in Spark?
23. What is a shuffle operation? Why are shuffles expensive?
24. What is `repartition()` vs `coalesce()`?
25. What is a broadcast variable?
26. What is an accumulator?

### Spark Submit & Caching
27. What is `spark-submit`?
28. What is the difference between `--deploy-mode client` and `cluster`?
29. What is `cache()` in Spark?
30. What is `persist()` and what are the different storage levels?

---

## Quick Definitions

Be prepared to define these terms in one or two sentences:

### Airflow
- DAG
- Operator
- XCom
- Backfill

### Kafka
- Topic
- Partition
- Offset
- Consumer Group
- ISR

### Spark
- RDD
- DataFrame
- Lazy Evaluation
- Shuffle
- Executor
