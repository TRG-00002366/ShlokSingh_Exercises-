# Exercise: Scale Calculation

## Exercise Overview
- **Duration:** 30 minutes
- **Format:** Paper-based calculations
- **Materials:** Calculator (optional), scratch paper

## Learning Objective
Practice calculating processing time on single vs distributed machines to internalize why distribution is necessary.

---

## Instructions

For each scenario below:
1. Calculate the time on a single machine
2. Calculate the time on a distributed cluster
3. Identify the speedup factor
4. Answer the follow-up questions

---

## Scenario 1: Log File Processing

**The Setup:**
- Dataset: 500 GB of web server logs
- Single machine read speed: 500 MB/s
- Processing overhead: 2x reading time (for parsing and filtering)
- Cluster: 10 machines, each with the same specs

**Questions:**

1.1 How long does it take to READ the data on a single machine?

```
Your calculation:
_________________________________________________

Answer: _________ minutes
```

1.2 How long is the total processing time on a single machine (read + process)?

```
Your calculation:
_________________________________________________

Answer: _________ minutes
```

1.3 If we split the data evenly across 10 machines, what is the parallel processing time?

```
Your calculation:
_________________________________________________

Answer: _________ minutes
```

1.4 What is the speedup factor (single machine time / cluster time)?

```
Your calculation:
_________________________________________________

Answer: _________ x faster
```

---

## Scenario 2: Daily Data Growth

**The Setup:**
- Day 1 data: 100 GB
- Growth rate: Data doubles every month
- Processing must complete within 24 hours
- Single machine processing: 1 GB per minute

**Questions:**

2.1 Complete this table:

| Month | Data Size | Single Machine Time | Fits in 24 hours? |
|-------|-----------|---------------------|-------------------|
| 1 | 100 GB | _____ min = _____ hr | Yes / No |
| 2 | _____ GB | _____ min = _____ hr | Yes / No |
| 3 | _____ GB | _____ min = _____ hr | Yes / No |
| 4 | _____ GB | _____ min = _____ hr | Yes / No |
| 5 | _____ GB | _____ min = _____ hr | Yes / No |
| 6 | _____ GB | _____ min = _____ hr | Yes / No |

2.2 At which month does single-machine processing become impossible (exceeds 24 hours)?

```
Answer: Month _____
```

2.3 How many machines would you need in Month 6 to finish within 24 hours?

```
Your calculation:
_________________________________________________

Answer: _________ machines
```

---

## Scenario 3: Diminishing Returns

**The Setup:**
- Dataset: 1 TB
- Coordination overhead per machine: 30 seconds
- Base processing per machine: 600 seconds / number of machines

**Questions:**

3.1 Calculate the total time for different cluster sizes:

| Machines | Processing Time | Overhead | Total Time |
|----------|-----------------|----------|------------|
| 1 | 600 / 1 = _____ s | 30 s | _____ s |
| 2 | 600 / 2 = _____ s | 60 s | _____ s |
| 4 | 600 / 4 = _____ s | 120 s | _____ s |
| 10 | 600 / 10 = _____ s | 300 s | _____ s |
| 20 | 600 / 20 = _____ s | 600 s | _____ s |
| 100 | 600 / 100 = _____ s | 3000 s | _____ s |

3.2 At which cluster size does adding more machines stop helping (total time increases)?

```
Answer: _________ machines
```

3.3 What does this tell you about choosing cluster size?

```
Your answer (1-2 sentences):
_________________________________________________
_________________________________________________
```

---

## Reflection Questions

R1. A coworker says "Just buy a bigger server" when data processing is slow. What are two arguments against this approach?

```
Argument 1:
_________________________________________________

Argument 2:
_________________________________________________
```

R2. When would you choose a single powerful machine over a distributed cluster?

```
Your answer (1-2 sentences):
_________________________________________________
_________________________________________________
```

---

## Answer Key (for self-checking)

<details>
<summary>Click to reveal answers</summary>

**1.1:** 500 GB / 500 MB/s = 1000 seconds = 16.67 minutes

**1.2:** 16.67 * 3 (read + 2x processing) = 50 minutes

**1.3:** 50 minutes / 10 machines = 5 minutes

**1.4:** 50 / 5 = 10x faster

**2.1:**
- Month 1: 100 GB, 100 min = 1.67 hr, Yes
- Month 2: 200 GB, 200 min = 3.33 hr, Yes
- Month 3: 400 GB, 400 min = 6.67 hr, Yes
- Month 4: 800 GB, 800 min = 13.33 hr, Yes
- Month 5: 1600 GB, 1600 min = 26.67 hr, No
- Month 6: 3200 GB, 3200 min = 53.33 hr, No

**2.2:** Month 5

**2.3:** 3200 min / 1440 min (24 hr) = 2.22, so at least 3 machines

**3.1:**
- 1: 600s + 30s = 630s
- 2: 300s + 60s = 360s
- 4: 150s + 120s = 270s
- 10: 60s + 300s = 360s
- 20: 30s + 600s = 630s
- 100: 6s + 3000s = 3006s

**3.2:** After 4 machines, total time starts increasing

**3.3:** There is an optimal cluster size where benefits balance overhead. Adding more machines beyond this point is counterproductive.

</details>
