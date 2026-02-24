# Lab: Building a Complete ETL Pipeline

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 3-4 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Build an end-to-end ETL pipeline with sensors
- Implement data validation and error handling
- Use multiple operator types in a production-like DAG
- Apply best practices for pipeline resilience

## Prerequisites
- Completed previous Airflow exercises
- Understanding of sensors and operators
- Running Airflow environment

---

## The Scenario

You're building a daily ETL pipeline for an e-commerce company. The pipeline must:
1. Wait for a source data file to appear
2. Extract and validate the data
3. Transform it (clean, aggregate, enrich)
4. Load to a destination
5. Send notifications on completion

This is a realistic production pattern you'll encounter frequently.

---

## Core Tasks

### Task 1: Add the File Sensor (30 minutes)

Open `starter_code/dags/etl_pipeline_dag.py`.

1. Import FileSensor from airflow.sensors.filesystem
2. Create a sensor that waits for `/opt/airflow/data/input/orders.csv`
3. Configure:
   - poke_interval: 30 seconds
   - timeout: 1 hour
   - mode: "poke"

### Task 2: Implement Extract Function (30 minutes)

Complete the `extract_data()` function:
- Read the CSV file using standard Python (or pandas)
- Validate minimum row count (at least 5 rows)
- Return metadata about extraction

### Task 3: Implement Validation (30 minutes)

Complete the `validate_data()` function:
- Check for required columns (order_id, amount, customer_id)
- Verify no null values in order_id
- Raise ValueError if validation fails

### Task 4: Implement Transform (45 minutes)

Complete the `transform_data()` function:
- Parse dates and amounts
- Calculate order totals
- Add processing timestamp
- Return transformed records

### Task 5: Implement Load (30 minutes)

Complete the `load_data()` function:
- Write results to JSON file
- Include metadata (record count, processing time)
- Return load summary

### Task 6: Add Error Handling (30 minutes)

1. Configure default_args with retries
2. Add on_failure_callback
3. Add success notification task

---

## Test Data

Create a test file at the expected location:

```csv
order_id,amount,customer_id,order_date
1001,150.00,C001,2024-01-15
1002,75.50,C002,2024-01-15
1003,200.00,C001,2024-01-15
1004,50.00,C003,2024-01-15
1005,125.75,C002,2024-01-15
```

---

## Definition of Done

- [ ] Sensor correctly waits for file
- [ ] Extract reads and returns data
- [ ] Validation catches bad data
- [ ] Transform processes all records
- [ ] Load writes output file
- [ ] Notifications trigger appropriately
- [ ] Pipeline handles failures gracefully

---

## Stretch Goals

1. **Add S3Sensor:** Replace FileSensor with S3KeySensor
2. **Add Branching:** Skip load if no new records
3. **Add Metrics:** Track execution time in XCom

---

## Submission

1. Completed `etl_pipeline_dag.py`
2. Screenshot of successful pipeline run
3. Output JSON file from load step

---

## Resources

- Written Content: `job-orchestration.md`, `sensors-and-triggers.md`
- Demo Reference: `demo_orchestration/`
