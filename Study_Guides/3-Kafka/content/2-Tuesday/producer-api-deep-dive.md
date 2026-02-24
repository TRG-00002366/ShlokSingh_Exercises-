# Producer API Deep Dive

## Learning Objectives
- Master the KafkaProducer class and its configuration options
- Understand acknowledgment settings and their trade-offs
- Implement retry strategies and error handling
- Tune producer performance with batching and compression

## Why This Matters

The Producer API is your primary interface for sending data to Kafka. Understanding its configuration options allows you to:

- Optimize for throughput or latency based on your needs
- Ensure data durability with proper acknowledgment settings
- Handle failures gracefully with retry logic
- Build production-ready data pipelines

This deep dive supports our Weekly Epic by giving you the skills to build robust producer applications.

## The Concept

### KafkaProducer Class Overview

The `KafkaProducer` is the main class for sending records to Kafka:

```python
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    # ... configuration options
)
```

### Essential Configuration Categories

| Category | Purpose |
|----------|---------|
| **Connection** | How to connect to Kafka |
| **Serialization** | How to encode keys/values |
| **Acknowledgments** | Durability guarantees |
| **Batching** | Throughput optimization |
| **Retries** | Failure handling |
| **Compression** | Bandwidth optimization |

### Connection Configuration

```python
producer = KafkaProducer(
    # Comma-separated list of broker addresses
    bootstrap_servers='broker1:9092,broker2:9092,broker3:9092',
    
    # Client identifier (for monitoring/debugging)
    client_id='my-producer-app',
    
    # Security (if required)
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='user',
    sasl_plain_password='password'
)
```

### Acknowledgment Settings (acks)

The `acks` parameter controls durability guarantees:

**acks=0: Fire and Forget**
```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks=0  # Do not wait for any acknowledgment
)
```
- Highest throughput, lowest latency
- No guarantee message was received
- Use for: Metrics, logs where some loss is acceptable

**acks=1: Leader Acknowledgment**
```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks=1  # Wait for leader to acknowledge
)
```
- Balanced throughput and durability
- Message survives unless leader fails immediately
- Use for: Most general use cases

**acks='all' (-1): Full Acknowledgment**
```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    acks='all'  # Wait for all in-sync replicas
)
```
- Lowest throughput, highest durability
- Message survives any single broker failure
- Use for: Critical data, financial transactions

### Comparison of acks Settings

| Setting | Durability | Throughput | Latency | Data Loss Risk |
|---------|------------|------------|---------|----------------|
| `acks=0` | None | Highest | Lowest | High |
| `acks=1` | Leader only | High | Low | Medium |
| `acks='all'` | All ISRs | Lower | Higher | Very Low |

### Retry Configuration

Handle transient failures with retries:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    
    # Number of retry attempts
    retries=5,
    
    # Time to wait between retries (ms)
    retry_backoff_ms=100,
    
    # Total time to retry before giving up (2 minutes)
    delivery_timeout_ms=120000,
    
    # Max time to wait for request to complete (30 seconds)
    request_timeout_ms=30000
)
```

### Idempotent Producer

Prevent duplicate messages during retries:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    enable_idempotence=True,  # Prevent duplicates
    acks='all',               # Required for idempotence
    retries=5                 # Retries are safe with idempotence
)
```

**How idempotence works:**
- Producer assigns sequence numbers to messages
- Broker detects and discards duplicates
- Guarantees exactly-once delivery from producer to broker

### Batching Configuration

Batch multiple messages for efficiency:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    
    # Maximum batch size in bytes (default: 16 KB)
    batch_size=32768,  # 32 KB
    
    # Time to wait for more messages before sending (ms)
    linger_ms=20,  # Wait up to 20ms
    
    # Buffer memory for all batches (default: 32 MB)
    buffer_memory=67108864  # 64 MB
)
```

**Batching behavior:**
```
Without batching (linger_ms=0):
  Message 1 --> [Send immediately]
  Message 2 --> [Send immediately]
  Message 3 --> [Send immediately]

With batching (linger_ms=20):
  Message 1 --|
  Message 2 --+--> [Wait 20ms] --> [Send batch]
  Message 3 --|
```

### Compression Configuration

Reduce network bandwidth with compression:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    
    # Compression algorithm: gzip, snappy, lz4, zstd
    compression_type='gzip'
)
```

**Compression comparison:**

| Algorithm | Compression Ratio | CPU Usage | Speed |
|-----------|-------------------|-----------|-------|
| None | 1.0x | Lowest | Fastest |
| Snappy | ~1.5x | Low | Fast |
| LZ4 | ~2.0x | Low | Fast |
| GZIP | ~2.5x | Medium | Medium |
| ZSTD | ~3.0x | Medium | Medium |

### Buffer Management

Control memory usage:

```python
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    
    # Total buffer memory
    buffer_memory=67108864,  # 64 MB
    
    # Max time to block if buffer is full (ms)
    max_block_ms=60000  # 1 minute
)
```

### Error Handling Patterns

**Pattern 1: Synchronous with Exception Handling**
```python
from kafka.errors import KafkaError

def send_message_sync(producer, topic, value):
    try:
        future = producer.send(topic, value=value)
        metadata = future.get(timeout=10)
        return metadata
    except KafkaError as e:
        print(f"Kafka error: {e}")
        raise
```

**Pattern 2: Asynchronous with Callbacks**
```python
def on_success(metadata):
    print(f"Sent to {metadata.topic}:{metadata.partition}:{metadata.offset}")

def on_error(exception):
    print(f"Failed: {exception}")
    # Implement retry logic, dead letter queue, etc.

producer.send('topic', value=b'data').add_callback(
    on_success
).add_errback(
    on_error
)
```

**Pattern 3: Batch Processing with Error Collection**
```python
def send_batch(producer, messages):
    futures = []
    errors = []
    
    for msg in messages:
        future = producer.send('topic', value=msg)
        futures.append((msg, future))
    
    producer.flush()  # Wait for all sends
    
    for msg, future in futures:
        try:
            future.get(timeout=10)
        except Exception as e:
            errors.append((msg, e))
    
    return errors
```

### Production-Ready Configuration

```python
from kafka import KafkaProducer
import json

def create_production_producer():
    """Create a producer configured for production use."""
    
    return KafkaProducer(
        # Connection
        bootstrap_servers='broker1:9092,broker2:9092,broker3:9092',
        client_id='order-service-producer',
        
        # Serialization
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        
        # Durability
        acks='all',
        enable_idempotence=True,
        
        # Retries
        retries=5,
        retry_backoff_ms=100,
        delivery_timeout_ms=120000,
        
        # Batching (throughput vs latency)
        batch_size=16384,
        linger_ms=10,
        
        # Compression
        compression_type='lz4',
        
        # Memory
        buffer_memory=33554432,
        max_block_ms=60000
    )

producer = create_production_producer()
```

### Monitoring Producer Metrics

Access producer metrics for monitoring:

```python
# Get all metrics
metrics = producer.metrics()

# Key metrics to monitor
print("Records sent:", metrics.get('record-send-total'))
print("Send rate:", metrics.get('record-send-rate'))
print("Buffer available:", metrics.get('buffer-available-bytes'))
print("Batch size avg:", metrics.get('batch-size-avg'))
```

## Code Example

Complete production-ready producer:

```python
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import logging
import atexit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OrderProducer:
    """Production-ready Kafka producer for order events."""
    
    def __init__(self, bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            enable_idempotence=True,
            retries=5,
            retry_backoff_ms=100,
            linger_ms=10,
            compression_type='lz4'
        )
        
        # Ensure cleanup on exit
        atexit.register(self.close)
    
    def send_order(self, order):
        """Send an order event to Kafka."""
        key = f"order-{order['order_id']}"
        
        future = self.producer.send(
            topic='orders',
            key=key,
            value=order
        )
        
        future.add_callback(self._on_success)
        future.add_errback(self._on_error)
        
        return future
    
    def send_orders_batch(self, orders):
        """Send multiple orders and return any failures."""
        failures = []
        
        for order in orders:
            future = self.send_order(order)
            try:
                future.get(timeout=10)
            except KafkaError as e:
                failures.append({'order': order, 'error': str(e)})
        
        return failures
    
    def _on_success(self, metadata):
        logger.info(
            f"Message sent: topic={metadata.topic}, "
            f"partition={metadata.partition}, offset={metadata.offset}"
        )
    
    def _on_error(self, exception):
        logger.error(f"Message failed: {exception}")
    
    def close(self):
        """Flush and close the producer."""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")

# Usage
producer = OrderProducer('localhost:9092')

order = {
    'order_id': 'ORD-12345',
    'customer': 'John Doe',
    'items': ['laptop', 'mouse'],
    'total': 1099.99
}

producer.send_order(order)
producer.producer.flush()
```

## Summary

- **acks** setting controls durability: `0` (none), `1` (leader), `all` (full)
- **Idempotence** prevents duplicates during retries
- **Batching** (`linger_ms`, `batch_size`) optimizes throughput
- **Compression** reduces network bandwidth (`lz4` is a good default)
- **Retries** with backoff handle transient failures gracefully
- Use **callbacks** for asynchronous error handling
- Always **flush** and **close** producers in production
- Monitor producer metrics for operational visibility

## Additional Resources

- [Producer Configuration Reference](https://kafka.apache.org/documentation/#producerconfigs)
- [Idempotent Producer Deep Dive](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Tuning Kafka Producers - Confluent](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html)
