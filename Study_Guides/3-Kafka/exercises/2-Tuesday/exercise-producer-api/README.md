# Pair Programming: Robust Producer Implementation

## Overview
**Mode:** Collaborative (Pair Programming)  
**Duration:** 3-4 hours  
**Difficulty:** Intermediate to Advanced

## Learning Objectives
By completing this exercise, you will:
- Configure a production-ready Kafka producer
- Implement callback-based error handling
- Handle edge cases like network failures and invalid data
- Practice pair programming with Driver/Navigator roles

## Prerequisites
- Kafka cluster running
- Python 3.8+ with kafka-python installed
- Completed reading: `producer-api-deep-dive.md`
- Observed instructor demo: `demo_producer_api`

---

## Pair Programming Roles

### Driver
- Writes the code
- Focuses on syntax and implementation details
- Asks Navigator for guidance when stuck

### Navigator
- Reviews code as it is written
- Thinks about edge cases and architecture
- Suggests improvements and catches errors
- Keeps track of requirements

**Switch roles every 25 minutes!**

---

## The Scenario

Your company processes payment events. These are critical messages that:
- Must never be lost
- Must be delivered exactly once when possible
- Must handle network failures gracefully
- Must alert operators when issues occur

You need to build a robust producer that meets these requirements.

---

## Core Requirements

### Requirement 1: High-Durability Configuration
Configure the producer for maximum reliability:
- `acks='all'` - Wait for all replicas
- `enable_idempotence=True` - Prevent duplicates
- `retries=5` - Retry on transient failures
- `retry_backoff_ms=100` - Wait between retries

### Requirement 2: Callback-Based Error Handling
Implement callbacks that:
- Log successful sends with partition and offset
- Log failed sends with error details
- Track success/failure counts

### Requirement 3: Graceful Failure Handling
Handle these edge cases:
- Connection timeout
- Invalid message format
- Topic does not exist
- Buffer full

### Requirement 4: Metrics Tracking
Track and report:
- Total messages sent
- Successful sends
- Failed sends
- Average latency

---

## Task Breakdown

### Session 1: Producer Configuration (45 minutes)
**Driver:** Partner A | **Navigator:** Partner B

1. Open `starter_code/robust_producer.py`
2. Complete the `create_producer()` function with durability settings
3. Test that the producer connects successfully

**Checkpoint:** Producer creates without errors

### Session 2: Message Generation (45 minutes)
**Driver:** Partner B | **Navigator:** Partner A

1. Complete the `generate_payment_event()` function
2. Implement validation to reject invalid payments
3. Create 20 diverse test payment events

**Checkpoint:** Payment events generate with proper structure

### Session 3: Callback Implementation (45 minutes)
**Driver:** Partner A | **Navigator:** Partner B

1. Implement `on_send_success()` callback
2. Implement `on_send_error()` callback
3. Add metrics tracking to the callbacks

**Checkpoint:** Callbacks fire and log appropriately

### Session 4: Error Handling & Edge Cases (45 minutes)
**Driver:** Partner B | **Navigator:** Partner A

1. Add try/except blocks for connection errors
2. Handle topic not found scenario
3. Implement graceful shutdown on SIGINT
4. Test with various failure scenarios

**Checkpoint:** Producer handles all edge cases gracefully

---

## Deliverables

### 1. Working Producer Script
A fully functional `robust_producer.py` that:
- Sends 20 payment events
- Uses callbacks for all sends
- Handles errors gracefully
- Reports metrics at the end

### 2. Test Report (`SUBMISSION.md`)
Document your testing:
- Normal operation (all messages sent)
- What happens when Kafka is stopped during sends
- What happens with invalid payment data

### 3. Pair Programming Reflection
Answer these questions:
- How did switching roles help you learn?
- What edge cases did the Navigator catch that the Driver missed?
- What would you do differently in a real production system?

---

## Starter Code Structure

```
starter_code/
    robust_producer.py    # Main producer (complete the TODOs)
    payment_validator.py  # Validation utilities (provided)
    test_scenarios.py     # Test helpers (optional)
```

---

## Grading Rubric

| Criterion | Points |
|-----------|--------|
| Producer correctly configured for durability | 20 |
| Callbacks implemented and working | 20 |
| Error handling for all edge cases | 20 |
| Metrics tracking accurate | 15 |
| Code quality and documentation | 10 |
| Test report complete | 10 |
| Pair programming reflection | 5 |
| **Total** | **100** |

---

## Definition of Done

- [ ] Producer sends 20 payment events successfully
- [ ] All sends use callback-based async pattern
- [ ] Success and error callbacks fire appropriately
- [ ] Metrics reported at end (total, success, failed, avg latency)
- [ ] Error handling for connection/validation issues
- [ ] `SUBMISSION.md` with test report and reflections
- [ ] Both partners contributed (visible in role switches)

---

## Tips for Success

1. **Start simple:** Get basic sends working before adding complexity
2. **Test incrementally:** Verify each feature before moving on
3. **Communicate:** Navigator should explain their thinking out loud
4. **Take breaks:** Switch roles naturally, take short breaks
5. **Document as you go:** Add comments explaining non-obvious code

---

## Cleanup

```bash
docker exec kafka-broker kafka-topics --delete \
    --topic payments-exercise \
    --bootstrap-server localhost:9092
```
