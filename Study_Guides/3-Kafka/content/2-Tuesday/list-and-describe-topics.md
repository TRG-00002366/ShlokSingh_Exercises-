# List and Describe Topics

## Learning Objectives
- Retrieve a list of all topics in a Kafka cluster
- Describe topic configurations and metadata
- Inspect partition and replica details

## Why This Matters

Before working with topics, you often need to:
- Verify a topic exists after creation
- Check the current configuration of existing topics
- Understand partition distribution across brokers
- Troubleshoot replication or partition issues

These inspection commands are essential for operations and debugging in real-world Kafka environments.

## The Concept

### Listing Topics via CLI

The `kafka-topics.sh` command provides topic listing capabilities.

**List all topics:**
```bash
kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Example output:**
```
__consumer_offsets
order-events
payment-transactions
user-activity
inventory-updates
```

Note: Topics starting with `__` are internal Kafka topics (like `__consumer_offsets` for consumer group coordination).

### Describing Topics via CLI

To get detailed information about a specific topic:

```bash
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic order-events
```

**Example output:**
```
Topic: order-events   TopicId: abc123   PartitionCount: 3   ReplicationFactor: 2   Configs: retention.ms=604800000
    Topic: order-events   Partition: 0   Leader: 1   Replicas: 1,2   Isr: 1,2
    Topic: order-events   Partition: 1   Leader: 2   Replicas: 2,3   Isr: 2,3
    Topic: order-events   Partition: 2   Leader: 3   Replicas: 3,1   Isr: 3,1
```

**Understanding the output:**

| Field | Description |
|-------|-------------|
| TopicId | Unique identifier for the topic |
| PartitionCount | Number of partitions |
| ReplicationFactor | Number of replicas per partition |
| Configs | Non-default configuration values |
| Partition | Partition number (0-indexed) |
| Leader | Broker ID that is the leader for this partition |
| Replicas | List of broker IDs holding replicas |
| Isr | In-Sync Replicas (replicas caught up with leader) |

### Describing All Topics

To describe all topics at once:

```bash
kafka-topics.sh --describe --bootstrap-server localhost:9092
```

### Filtering Topics

List topics matching a pattern:

```bash
# Topics containing "event"
kafka-topics.sh --list --bootstrap-server localhost:9092 | grep event
```

### Listing Topics with Python

Using `kafka-python`:

```python
from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='topic-inspector'
)

# Get list of all topics
topics = admin_client.list_topics()
print("Available topics:")
for topic in topics:
    print(f"  - {topic}")

admin_client.close()
```

**Output:**
```
Available topics:
  - __consumer_offsets
  - order-events
  - user-activity
  - payment-transactions
```

### Describing Topics with Python

```python
from kafka.admin import KafkaAdminClient

admin_client = KafkaAdminClient(
    bootstrap_servers='localhost:9092',
    client_id='topic-inspector'
)

# Describe specific topics
topics_to_describe = ['order-events', 'user-activity']
topic_descriptions = admin_client.describe_topics(topics_to_describe)

for topic in topic_descriptions:
    print(f"\nTopic: {topic['topic']}")
    print(f"  Partitions: {len(topic['partitions'])}")
    for partition in topic['partitions']:
        print(f"    Partition {partition['partition']}:")
        print(f"      Leader: {partition['leader']}")
        print(f"      Replicas: {partition['replicas']}")
        print(f"      ISR: {partition['isr']}")

admin_client.close()
```

### Getting Topic Configurations

To retrieve the configuration settings of a topic:

**CLI approach:**
```bash
kafka-configs.sh --describe \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name order-events
```

**Output:**
```
Dynamic configs for topic order-events are:
  retention.ms=604800000 sensitive=false synonyms={...}
  cleanup.policy=delete sensitive=false synonyms={...}
```

**Python approach:**
```python
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

admin_client = KafkaAdminClient(bootstrap_servers='localhost:9092')

# Define the resource to describe
config_resource = ConfigResource(
    ConfigResourceType.TOPIC,
    'order-events'
)

# Get configurations
configs = admin_client.describe_configs([config_resource])

for resource, config in configs.items():
    print(f"Configuration for {resource}:")
    for key, value in config.items():
        print(f"  {key}: {value}")

admin_client.close()
```

### Using confluent-kafka

```python
from confluent_kafka.admin import AdminClient

admin = AdminClient({'bootstrap.servers': 'localhost:9092'})

# List topics
metadata = admin.list_topics(timeout=10)

print("Topics and their partitions:")
for topic, topic_metadata in metadata.topics.items():
    if not topic.startswith('__'):  # Skip internal topics
        print(f"\n{topic}:")
        for partition_id, partition in topic_metadata.partitions.items():
            print(f"  Partition {partition_id}: Leader={partition.leader}")
```

### Checking Topic Health

A healthy topic has:
- All partitions with active leaders
- ISR count equal to replica count
- No under-replicated partitions

**Check for under-replicated partitions:**
```bash
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --under-replicated-partitions
```

**Check for partitions without a leader:**
```bash
kafka-topics.sh --describe \
  --bootstrap-server localhost:9092 \
  --unavailable-partitions
```

### Topic Inspection Workflow

```
1. List all topics
   |
   v
2. Filter to find topics of interest
   |
   v
3. Describe specific topic(s)
   |
   v
4. Check partition distribution
   |
   v
5. Verify ISR health
   |
   v
6. Review configurations if needed
```

## Code Example

Complete inspection script:

```python
from kafka.admin import KafkaAdminClient

def inspect_kafka_topics(bootstrap_servers='localhost:9092'):
    """Inspect all topics in a Kafka cluster."""
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id='cluster-inspector'
    )
    
    # List all topics
    all_topics = admin_client.list_topics()
    user_topics = [t for t in all_topics if not t.startswith('__')]
    
    print(f"Found {len(user_topics)} user topics:\n")
    
    # Describe each topic
    if user_topics:
        descriptions = admin_client.describe_topics(user_topics)
        
        for topic_info in descriptions:
            topic_name = topic_info['topic']
            partitions = topic_info['partitions']
            
            print(f"Topic: {topic_name}")
            print(f"  Partition count: {len(partitions)}")
            
            # Check replication health
            for p in partitions:
                leader = p['leader']
                replicas = p['replicas']
                isr = p['isr']
                
                health = "OK" if len(isr) == len(replicas) else "UNDER-REPLICATED"
                print(f"  Partition {p['partition']}: Leader={leader}, "
                      f"Replicas={replicas}, ISR={isr} [{health}]")
            print()
    
    admin_client.close()

# Run inspection
inspect_kafka_topics()
```

## Summary

- Use `kafka-topics.sh --list` to see all topics in a cluster
- Use `kafka-topics.sh --describe` to get partition and replica details
- Key metrics include partition count, replication factor, leader assignment, and ISR status
- Python libraries (`kafka-python`, `confluent-kafka`) provide programmatic access
- Monitor for under-replicated or unavailable partitions to ensure cluster health
- Internal topics (prefixed with `__`) are managed by Kafka automatically

## Additional Resources

- [Kafka Topic Operations - Official Docs](https://kafka.apache.org/documentation/#basic_ops_add_topic)
- [Monitoring Kafka - Confluent](https://docs.confluent.io/platform/current/kafka/monitoring.html)
- [kafka-python API Reference](https://kafka-python.readthedocs.io/en/master/)
