# Lab: Monitoring, Alerting, and Troubleshooting

## Overview
**Type:** Hybrid (Implementation + Analysis)  
**Duration:** 2-3 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Configure SLAs for task execution monitoring
- Set up failure callbacks for alerting
- Practice debugging failed tasks using logs
- Apply systematic troubleshooting techniques

## Prerequisites
- Completed ETL pipeline exercise
- Understanding of Airflow logging
- Running Airflow environment

---

## The Scenario

Your pipeline from the previous exercise is now in "production." You need to:
1. Add SLAs to ensure timely completion
2. Configure alerts for failures
3. Practice troubleshooting when things go wrong

This exercise teaches you how to operate Airflow pipelines at scale.

---

## Core Tasks

### Task 1: Configure SLAs (30 minutes)

Open `starter_code/dags/monitoring_exercise_dag.py`.

1. Add SLAs to critical tasks:
   - `extract`: Must complete within 5 minutes
   - `transform`: Must complete within 10 minutes
   - `load`: Must complete within 3 minutes

2. Implement `sla_miss_callback` that:
   - Logs the missed tasks
   - Prints the SLA details
   - Would send alert in production

### Task 2: Configure Failure Alerting (30 minutes)

1. Implement `on_failure_callback`:
   - Extract task and DAG information from context
   - Log the exception message
   - Include log URL for quick access

2. Add callback to default_args

### Task 3: Deploy the Buggy DAG (15 minutes)

The provided DAG has intentional bugs! Deploy it and observe:

1. Copy to Airflow dags folder
2. Trigger the DAG
3. Observe which tasks fail
4. Note the error types

### Task 4: Debug Using Logs (45 minutes)

For each failed task:

1. Click on the failed task in the UI
2. Open the "Log" tab
3. Read from the bottom up
4. Identify:
   - What error occurred?
   - Which line of code caused it?
   - What's the root cause?

Document your findings in `deliverables/debug_report.md`

### Task 5: Fix the Bugs (30 minutes)

Based on your analysis:
1. Fix each bug in the DAG
2. Clear the failed tasks
3. Re-run until successful
4. Document your fixes

---

## Intentional Bugs to Find

The DAG contains these bugs (find and fix them):

1. **Import Error** - A module is misspelled
2. **Data Error** - A column name is wrong
3. **Logic Error** - A condition is inverted
4. **Type Error** - Wrong data type used

---

## Definition of Done

- [ ] SLAs configured on 3 tasks
- [ ] SLA miss callback implemented
- [ ] Failure callback implemented
- [ ] All 4 bugs identified
- [ ] All 4 bugs fixed
- [ ] Debug report complete
- [ ] DAG runs successfully end-to-end

---

## Troubleshooting Checklist

Use this checklist when debugging:

- [ ] Read the error message (bottom of log)
- [ ] Identify the error type
- [ ] Find the line number
- [ ] Check the code at that line
- [ ] Look for typos
- [ ] Check data types
- [ ] Verify variable names
- [ ] Check imports

---

## Stretch Goals

1. **Add StatsD Metrics:** Configure metric collection
2. **Create Dashboard:** Design a Grafana dashboard (on paper)
3. **Health Monitoring:** Add a task that checks system health

---

## Submission

1. Fixed `monitoring_exercise_dag.py`
2. Completed `debug_report.md`
3. Screenshot of successful run
4. Screenshot of Browse -> SLA Misses (if any occur)

---

## Resources

- Written Content: `monitoring-and-alerting.md`, `troubleshooting-airflow.md`
- Demo Reference: `demo_monitoring/`
