# Lab: Kafka Environment Setup

## Overview
**Mode:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Difficulty:** Beginner

## Learning Objectives
By completing this exercise, you will:
- Set up a local Kafka development environment using Docker
- Verify that ZooKeeper and Kafka broker are running correctly
- Use basic CLI commands to interact with your cluster
- Write a Python script to verify connectivity

## Prerequisites
- Docker Desktop installed and running
- Python 3.8+ with pip
- Terminal/Command Prompt access
- Completed reading: `intro-to-kafka.md`, `kafka-architecture.md`

---

## The Scenario

Your team is starting a new project that requires real-time event streaming. Before you can build any producers or consumers, you need a working Kafka environment for local development.

Your task is to set up this environment and verify it is working correctly.

---

## Core Tasks

### Task 1: Start the Kafka Cluster (30 minutes)

1. Navigate to the `starter_code/` directory
2. Review the `docker-compose.yml` file to understand the services
3. Start the cluster:
   ```bash
   docker-compose up -d
   ```
4. Verify all containers are running:
   ```bash
   docker-compose ps
   ```

**Expected Result:** Three containers should be running:
- `kafka-zookeeper` (port 2181)
- `kafka-broker` (port 9092)
- `kafka-ui` (port 8080)

### Task 2: Explore the Cluster via CLI (30 minutes)

1. Access the Kafka container shell:
   ```bash
   docker exec -it kafka-broker bash
   ```

2. List all topics (should be empty or show only internal topics):
   ```bash
   kafka-topics --list --bootstrap-server localhost:9092
   ```

3. Check broker information:
   ```bash
   kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

4. Exit the container shell:
   ```bash
   exit
   ```

**Checkpoint:** Take a screenshot of your topic list output.

### Task 3: Verify with Python (45 minutes)

1. Install the required library:
   ```bash
   pip install kafka-python
   ```

2. Open `starter_code/verify_connection.py`

3. Complete the `TODO` sections to:
   - Connect to the Kafka cluster
   - List available topics
   - Print broker information

4. Run your script:
   ```bash
   python verify_connection.py
   ```

**Expected Output:**
```
Connected to Kafka cluster!
Brokers: [Broker 1: kafka:29092]
Topics: ['__consumer_offsets']
```

### Task 4: Explore Kafka UI (15 minutes)

1. Open your browser to http://localhost:8080
2. Navigate through the UI and find:
   - The broker information
   - The (empty) topics list
   - The cluster overview

**Checkpoint:** Take a screenshot of the Kafka UI dashboard.

---

## Stretch Goals (Optional)

1. **Custom Configuration:** Modify `docker-compose.yml` to add a second Kafka broker
2. **Health Check Script:** Extend your Python script to continuously monitor cluster health
3. **Alternative Setup:** Try setting up Kafka without Docker (native installation)

---

## Definition of Done

- [ ] All three Docker containers are running
- [ ] You can list topics using the CLI
- [ ] Your Python verification script runs without errors
- [ ] You can access and navigate the Kafka UI
- [ ] Screenshots captured for checkpoints

---

## Submission

Create a file called `SUBMISSION.md` in this directory containing:
1. Your two checkpoint screenshots
2. The output of your Python verification script
3. Any issues you encountered and how you resolved them

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Docker not running | Start Docker Desktop |
| Port 9092 in use | Stop other Kafka instances or change port |
| Connection refused | Wait 30 seconds for containers to fully start |
| Python import error | Run `pip install kafka-python` |

---

## Cleanup

When finished, stop the cluster:
```bash
docker-compose down
```

To completely remove all data:
```bash
docker-compose down -v
```
