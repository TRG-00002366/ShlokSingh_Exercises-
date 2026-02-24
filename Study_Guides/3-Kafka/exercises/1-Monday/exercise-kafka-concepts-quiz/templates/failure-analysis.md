# Failure Analysis

## Scenario

Your Kafka cluster has 3 brokers. The "orders" topic has 3 partitions with replication factor 2:

| Partition | Leader | Follower |
|-----------|--------|----------|
| Partition 0 | Broker 1 | Broker 2 |
| Partition 1 | Broker 2 | Broker 3 |
| Partition 2 | Broker 3 | Broker 1 |

**Event: Broker 2 suddenly crashes.**

---

## Analysis Questions

### 1. Which partitions are affected?

[Your answer here]


### 2. What happens to Partition 0?

*Consider: Broker 2 was the follower for Partition 0. What is the impact?*

[Your answer here]


### 3. What happens to Partition 1?

*Consider: Broker 2 was the leader for Partition 1. Who becomes the new leader?*

[Your answer here]


### 4. Can producers still send messages to all partitions? Why or why not?

[Your answer here]


### 5. What is the cluster's replication status after the failure?

*Consider: How many replicas does each partition have now? Is the cluster at risk?*

[Your answer here]


---

## Bonus Question

If Broker 3 also fails immediately after Broker 2, what happens to Partition 1?

[Your answer here]
