# Lab: Creating Your First DAG

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Write a complete DAG file from scratch
- Use BashOperator and PythonOperator
- Define task dependencies
- Trigger a DAG and observe execution in the UI

## Prerequisites
- Airflow environment running (from previous exercise)
- Basic Python knowledge
- Understanding of DAG concepts from written content

---

## The Scenario

You've been asked to create a simple data pipeline that runs daily. It should:
1. Print a start message
2. Execute a Python function that processes some data
3. Run a shell command to generate a report
4. Print an end message

Your task is to create this DAG, deploy it to Airflow, and verify it runs correctly.

---

## Core Tasks

### Task 1: Complete the DAG File (45 minutes)

Navigate to `starter_code/dags/` and open `my_first_dag.py`.

Complete the **TODO** sections:

1. **Import the required modules:**
   - DAG from airflow
   - BashOperator from airflow.operators.bash
   - PythonOperator from airflow.operators.python
   - datetime from datetime

2. **Define the Python functions:**
   - `process_data()`: Print "Processing data..." and return a dictionary
   - `generate_summary()`: Print a summary message

3. **Create the DAG:**
   - dag_id: "my_first_pipeline"
   - start_date: January 1, 2024
   - schedule: None (manual trigger only)
   - catchup: False

4. **Create the tasks:**
   - `start`: BashOperator that echoes "Pipeline starting..."
   - `process`: PythonOperator calling process_data
   - `report`: BashOperator that echoes "Generating report..."
   - `end`: PythonOperator calling generate_summary

5. **Define dependencies:**
   - start -> process -> report -> end

### Task 2: Deploy the DAG (15 minutes)

1. Copy your completed DAG file to the Airflow dags folder:
   ```bash
   cp dags/my_first_dag.py /path/to/airflow/dags/
   ```

2. Wait for Airflow to detect the DAG (up to 60 seconds)

3. Check for import errors:
   - In the UI, if your DAG doesn't appear, check the top banner for errors
   - Or run: `airflow dags list-import-errors`

### Task 3: Run and Observe (30 minutes)

1. **Enable the DAG:**
   - Find your DAG in the list
   - Toggle the switch to enable it

2. **Trigger the DAG:**
   - Click the "Play" button
   - Select "Trigger DAG"

3. **Watch Execution:**
   - Click on your DAG to see the Grid view
   - Watch tasks turn from gray -> light blue -> green

4. **Check Logs:**
   - Click on each task instance
   - Select "Log" tab
   - Verify your print statements appear

### Task 4: Document Your Work (20 minutes)

Complete `deliverables/execution_log.md`:

1. Screenshot of the successful DAG run (Grid view)
2. Screenshot of task logs showing your print statements
3. Answers to the reflection questions

---

## Stretch Goals

1. **Add Error Handling:**
   - Add `retries=2` to default_args
   - Add `retry_delay=timedelta(minutes=1)`

2. **Add a Branching Path:**
   - Use BranchPythonOperator to choose between two paths
   - One path for "success", another for "warning"

3. **Add Documentation:**
   - Add `doc_md` to your DAG with a description
   - View it in the UI (DAG docs tab)

---

## Definition of Done

- [ ] DAG file has no import errors
- [ ] DAG appears in the Airflow UI
- [ ] All 4 tasks execute successfully (green status)
- [ ] Task logs show your custom messages
- [ ] `execution_log.md` is complete with screenshots

---

## Submission

1. Your completed `my_first_dag.py` file
2. Completed `execution_log.md` with screenshots
3. (Optional) Any stretch goal implementations

---

## Common Mistakes to Avoid

| Mistake | Solution |
|---------|----------|
| Syntax error in Python | Run `python my_first_dag.py` to check |
| DAG not appearing | Check for import errors in UI |
| Tasks stuck in queued | Ensure scheduler is running |
| Wrong dependencies | Use `>>` operator, not function calls |

---

## Resources

- Written Content: `dags-operators-tasks.md`
- Demo Reference: `demo_first_dag/`
