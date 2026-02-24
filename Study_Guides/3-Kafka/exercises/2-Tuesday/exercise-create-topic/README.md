# Lab: Creating Kafka Topics

## Overview
**Mode:** Implementation (Code Lab)  
**Duration:** 1.5-2 hours  
**Difficulty:** Beginner

## Learning Objectives
By completing this exercise, you will:
- Create Kafka topics using CLI commands
- Create Kafka topics programmatically with Python
- Configure partition count and replication factor
- Verify topic creation and inspect topic metadata

## Prerequisites
- Kafka cluster running (from `exercise-kafka-setup`)
- Python 3.8+ with kafka-python installed
- Completed reading: `creating-kafka-topics.md`, `list-and-describe-topics.md`

---

## The Scenario

Your e-commerce platform needs several Kafka topics for different event types. You need to create these topics with appropriate configurations based on their use cases.

---

## Core Tasks

### Task 1: Create Topics via CLI (30 minutes)

Using the Kafka CLI, create the following topics:

| Topic Name | Partitions | Replication Factor | Use Case |
|------------|------------|-------------------|----------|
| `user-signups` | 2 | 1 | New user registrations |
| `page-views` | 6 | 1 | Website analytics (high volume) |
| `purchases` | 3 | 1 | Order transactions |

**Commands to use:**
```bash
docker exec kafka-broker kafka-topics --create \
    --topic <topic-name> \
    --partitions <count> \
    --replication-factor <count> \
    --bootstrap-server localhost:9092
```

**Verification:**
After creating each topic, verify it exists:
```bash
docker exec kafka-broker kafka-topics --describe \
    --topic <topic-name> \
    --bootstrap-server localhost:9092
```

**Checkpoint:** Record the output of `--describe` for each topic.

### Task 2: Create Topics via Python (45 minutes)

1. Open `starter_code/create_topics.py`
2. Complete the `TODO` sections to:
   - Create a KafkaAdminClient
   - Define a topic with custom configuration
   - Create multiple topics in a batch

3. Create these additional topics using your Python script:

| Topic Name | Partitions | Retention (days) |
|------------|------------|------------------|
| `inventory-updates` | 4 | 3 |
| `price-changes` | 2 | 7 |
| `notifications` | 3 | 1 |

4. Run your script:
```bash
python create_topics.py
```

### Task 3: List and Describe All Topics (20 minutes)

1. Complete the `list_topics()` function in `starter_code/inspect_topics.py`
2. The function should:
   - List all topics (excluding internal ones starting with `__`)
   - For each topic, display:
     - Topic name
     - Partition count
     - Replication factor
     - Leader broker for each partition

3. Run the inspection:
```bash
python inspect_topics.py
```

**Expected Output Format:**
```
Topic: user-signups
  Partitions: 2
  Partition 0: Leader=1, Replicas=[1], ISR=[1]
  Partition 1: Leader=1, Replicas=[1], ISR=[1]
```

---

## Stretch Goals (Optional)

1. **Custom Retention:** Create a topic with 1-hour retention using `--config retention.ms=3600000`
2. **Cleanup Policy:** Create a compacted topic using `--config cleanup.policy=compact`
3. **Topic Deletion:** Delete a topic using CLI and recreate it with different settings

---

## Definition of Done

- [ ] 3 topics created via CLI (`user-signups`, `page-views`, `purchases`)
- [ ] 3 topics created via Python (`inventory-updates`, `price-changes`, `notifications`)
- [ ] Inspection script runs and displays all 6 topics with their configurations
- [ ] `SUBMISSION.md` contains describe output for all topics

---

## Submission

Create a file called `SUBMISSION.md` containing:
1. CLI output from creating each topic
2. Output from your Python topic creation script
3. Full output from your topic inspection script

---

## Cleanup

Delete all exercise topics:
```bash
docker exec kafka-broker kafka-topics --delete --topic user-signups --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --delete --topic page-views --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --delete --topic purchases --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --delete --topic inventory-updates --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --delete --topic price-changes --bootstrap-server localhost:9092
docker exec kafka-broker kafka-topics --delete --topic notifications --bootstrap-server localhost:9092
```
