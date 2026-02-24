# Exercise: Cluster Optimization

## Overview
Configure memory and executor settings for optimal performance on a Spark cluster.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## The Scenario

You have a data processing job that is running slowly. Your task is to analyze the workload and configure optimal memory and executor settings.

---

## Core Tasks

### Task 1: Understand the Workload

Given job characteristics:
- Input data: ~10GB
- Operations: Multiple joins and aggregations
- Current running time: 30 minutes (too slow)

Answer these questions:

1. Is this memory-intensive or CPU-intensive?
2. Should we optimize for more executors or larger executors?
3. What shuffle partition count makes sense?

### Task 2: Calculate Optimal Settings

Given cluster resources:
- 10 nodes
- Each node: 16 cores, 64GB RAM
- YARN overhead: 10%

Calculate:

| Setting | Formula | Your Value |
|---------|---------|------------|
| Total cores | | |
| Total memory | | |
| Executor cores | 4-5 per executor | |
| Executor memory | | |
| Number executors | | |

### Task 3: Create Configuration

Create `optimized_submit.sh`:

```bash
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory YOUR_VALUE \
    --executor-memory YOUR_VALUE \
    --executor-cores YOUR_VALUE \
    --num-executors YOUR_VALUE \
    --conf spark.sql.shuffle.partitions=YOUR_VALUE \
    --conf spark.memory.fraction=0.6 \
    --conf spark.memory.storageFraction=0.5 \
    job.py
```

### Task 4: Document Your Rationale

Create `optimization_doc.md`:

```markdown
# Spark Optimization Configuration

## Cluster Resources
- Nodes: 10
- Cores per node: 16
- Memory per node: 64GB

## Chosen Configuration

### Executor Settings
- executor-memory: [value] - Rationale: [why]
- executor-cores: [value] - Rationale: [why]
- num-executors: [value] - Rationale: [why]

### Memory Settings
- memory.fraction: 0.6 - [explain what this means]
- memory.storageFraction: 0.5 - [explain what this means]

### Shuffle Settings
- shuffle.partitions: [value] - Rationale: [why]

## Expected Improvement
[What improvement do you expect and why?]
```

---

## Guidelines

**Executor Memory:**
- Leave 10% for YARN overhead
- 4-16g is typical range
- More memory = fewer executors, but handles larger shuffles

**Executor Cores:**
- 4-5 cores per executor is optimal
- More cores = more memory contention

**Shuffle Partitions:**
- Rule: 2-4x total cores
- Too few = OOM, too many = overhead

---

## Deliverables

1. `optimized_submit.sh` - Your configuration
2. `optimization_doc.md` - Documentation with rationale

---

## Definition of Done

- [ ] Calculated executor count correctly
- [ ] Memory settings account for overhead
- [ ] Shuffle partitions appropriate for cluster size
- [ ] All choices documented with rationale
- [ ] Configuration would run without errors

---

## Additional Resources
- Written Content: `configure-memory-driver-and-executors.md`
- Written Content: `executors.md`
- Demo: `demo_memory_configuration.py`
