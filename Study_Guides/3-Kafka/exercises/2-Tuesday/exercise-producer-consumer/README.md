# Lab: Build a Producer-Consumer Pipeline

## Overview
**Mode:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Difficulty:** Intermediate

## Learning Objectives
By completing this exercise, you will:
- Implement a Kafka producer that sends structured messages
- Implement a Kafka consumer that processes messages
- Understand message serialization and deserialization
- Observe partition distribution and offset tracking

## Prerequisites
- Kafka cluster running
- Python 3.8+ with kafka-python installed
- Completed reading: `producer-consumer-overview.md`, `sending-messages.md`
- Observed instructor demo: `demo_producer_consumer`

---

## The Scenario

You are building an order processing system. The system has two components:
1. **Order Service** (Producer): Receives orders and sends them to Kafka
2. **Fulfillment Service** (Consumer): Reads orders and processes them

Your task is to implement both components.

---

## Core Tasks

### Task 1: Create the Orders Topic (10 minutes)

Create a topic for orders with the following configuration:
- Topic name: `orders-exercise`
- Partitions: 3
- Replication factor: 1

```bash
docker exec kafka-broker kafka-topics --create \
    --topic orders-exercise \
    --partitions 3 \
    --replication-factor 1 \
    --bootstrap-server localhost:9092
```

### Task 2: Implement the Producer (60 minutes)

1. Open `starter_code/order_producer.py`
2. Complete the `TODO` sections to:
   - Create a KafkaProducer with JSON serialization
   - Generate 10 order messages with proper structure
   - Send messages with order_id as the key
   - Print the partition and offset for each sent message

**Message Structure:**
```json
{
    "order_id": "ORD-001",
    "customer_id": "CUST-123",
    "product": "Laptop",
    "quantity": 1,
    "price": 999.99,
    "timestamp": "2024-12-15T10:30:00"
}
```

3. Run your producer:
```bash
python order_producer.py
```

**Expected Output:**
```
Sending 10 orders...
  Order ORD-001 sent to partition 2, offset 0
  Order ORD-002 sent to partition 0, offset 0
  ...
All orders sent successfully!
```

### Task 3: Implement the Consumer (60 minutes)

1. Open `starter_code/order_consumer.py`
2. Complete the `TODO` sections to:
   - Create a KafkaConsumer with JSON deserialization
   - Subscribe to the `orders-exercise` topic
   - Process each message and print order details
   - Commit offsets after processing

3. Run your consumer in a new terminal:
```bash
python order_consumer.py
```

**Expected Output:**
```
Waiting for orders...
Received Order:
  Order ID: ORD-001
  Customer: CUST-123
  Product: Laptop
  Quantity: 1
  Price: $999.99
  Partition: 2, Offset: 0
...
```

### Task 4: Test the Pipeline (30 minutes)

1. **Terminal 1:** Start the consumer first
2. **Terminal 2:** Run the producer to send 10 orders
3. Observe messages appearing in the consumer
4. Run the producer again with 10 more orders
5. Verify the consumer receives the new orders with incremented offsets

**Questions to Answer:**
- What partition did most orders go to? Why?
- What happens if you restart the consumer? Does it re-read messages?
- What is the highest offset for each partition?

---

## Stretch Goals (Optional)

1. **Custom Partitioning:** Modify the producer to send all orders from the same customer to the same partition
2. **Error Handling:** Add try/except blocks to handle connection errors gracefully
3. **Multiple Consumers:** Run two consumers with the same group_id and observe partition assignment
4. **Async Producer:** Modify the producer to use callbacks instead of synchronous sends

---

## Definition of Done

- [ ] Topic `orders-exercise` created with 3 partitions
- [ ] Producer sends 10 orders with proper JSON structure
- [ ] Producer prints partition and offset for each message
- [ ] Consumer receives and displays all 10 orders
- [ ] Consumer shows partition and offset information
- [ ] `SUBMISSION.md` contains output from both producer and consumer

---

## Submission

Create a file called `SUBMISSION.md` containing:
1. Output from your producer (showing all 10 orders sent)
2. Output from your consumer (showing all 10 orders received)
3. Answers to the three questions in Task 4
4. Any issues you encountered and how you resolved them

---

## Cleanup

Delete the exercise topic:
```bash
docker exec kafka-broker kafka-topics --delete \
    --topic orders-exercise \
    --bootstrap-server localhost:9092
```
