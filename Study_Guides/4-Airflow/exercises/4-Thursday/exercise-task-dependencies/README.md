# Lab: Building Complex Task Dependencies

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Implement fan-out (parallel) and fan-in (aggregation) patterns
- Use BranchPythonOperator for conditional execution
- Apply trigger rules to control task execution
- Visualize and verify dependencies in the UI

## Prerequisites
- Completed "First DAG" exercise
- Understanding of dependency operators (`>>`, `<<`)
- Familiarity with basic Airflow operators

---

## The Scenario

You're building a data pipeline for a multi-region e-commerce company. The pipeline must:
1. Extract data from three regions in parallel (US, EU, APAC)
2. Aggregate results after all extractions complete
3. Choose a processing path based on data volume
4. Run cleanup regardless of success or failure

This mirrors real-world ETL patterns you'll encounter in production.

---

## Core Tasks

### Task 1: Complete the DAG Structure (45 minutes)

Navigate to `starter_code/dags/` and open `dependency_exercise_dag.py`.

Complete the following sections:

1. **Fan-Out Pattern:**
   Create three parallel extraction tasks that run after `start`:
   ```python
   start >> [extract_us, extract_eu, extract_apac]
   ```

2. **Fan-In Pattern:**
   Create an aggregation task that waits for ALL extractions:
   ```python
   [extract_us, extract_eu, extract_apac] >> aggregate
   ```

3. **Branching Logic:**
   Implement the `choose_path` function that:
   - Returns "heavy_processing" if total records > 5000
   - Returns "light_processing" otherwise

4. **Trigger Rules:**
   - Set `cleanup` to run with `trigger_rule=TriggerRule.ALL_DONE`
   - Set `end` to use `NONE_FAILED_MIN_ONE_SUCCESS`

### Task 2: Test the Dependencies (30 minutes)

1. Deploy your DAG to Airflow
2. Trigger the DAG and observe:
   - Do the three extract tasks run in parallel?
   - Does aggregate wait for all three?
   - Which branch is taken?
   - Does cleanup run even if a branch is skipped?

3. Verify in Graph view:
   - Take a screenshot showing the dependency structure
   - Identify which tasks are parallel

### Task 3: Simulate a Failure (20 minutes)

1. Modify one of the extract functions to raise an exception
2. Trigger the DAG again
3. Observe:
   - What happens to the aggregate task?
   - Does cleanup still run?
   - What color are the skipped tasks?

4. Clear the failed task and re-run

### Task 4: Diagram the Flow (20 minutes)

Create a Mermaid diagram in `deliverables/dag_diagram.mermaid` that shows:
- All tasks
- Dependencies (arrows)
- Which tasks are parallel
- The branching decision point

---

## Stretch Goals

1. **Add a Fourth Region:**
   Add LATAM extraction that runs in parallel with the others

2. **Nested Branching:**
   Add a second branching decision inside the heavy_processing path

3. **Dynamic Parallelism:**
   Use `expand()` to dynamically create extraction tasks from a list

---

## Definition of Done

- [ ] All three extract tasks run in parallel
- [ ] Aggregate waits for all extracts
- [ ] Branching correctly selects a path
- [ ] Cleanup runs regardless of upstream status
- [ ] Mermaid diagram is complete
- [ ] Failure scenario tested and documented

---

## Submission

1. Completed `dependency_exercise_dag.py`
2. `dag_diagram.mermaid` with your visualization
3. `execution_notes.md` with observations

---

## Resources

- Written Content: `task-dependencies.md`
- Demo Reference: `demo_task_dependencies/`
- [Trigger Rules Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#trigger-rules)
