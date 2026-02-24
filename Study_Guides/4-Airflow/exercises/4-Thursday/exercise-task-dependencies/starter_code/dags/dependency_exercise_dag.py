"""
Task Dependencies Exercise
===========================
Build a DAG with complex dependencies including:
- Fan-out (parallel execution)
- Fan-in (aggregation)
- Conditional branching
- Trigger rules

Complete the TODO sections to make this DAG work.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time


# ============================================================
# Task Functions
# ============================================================

def extract_region(region: str, **context):
    """
    Simulate extracting data from a region.
    Returns a record count.
    """
    print(f"Extracting data from {region}...")
    time.sleep(random.uniform(1, 2))
    
    records = random.randint(1000, 3000)
    print(f"Extracted {records} records from {region}")
    
    return {"region": region, "records": records}


def aggregate_data(**context):
    """
    Aggregate results from all regional extractions.
    Pull data from upstream tasks using XCom.
    """
    ti = context["ti"]
    
    # TODO: Pull results from all three extract tasks
    # Hint: Use ti.xcom_pull(task_ids="task_name")
    
    us_result = ti.xcom_pull(task_ids="extract_us")
    eu_result = ti.xcom_pull(task_ids="extract_eu")
    apac_result = ti.xcom_pull(task_ids="extract_apac")
    
    total = us_result["records"] + eu_result["records"] + apac_result["records"]
    
    print(f"Aggregated {total} total records")
    return {"total_records": total}


def choose_processing_path(**context):
    """
    TODO: Implement branching logic.
    
    Return "heavy_processing" if total_records > 5000
    Return "light_processing" otherwise
    
    Hint: Use ti.xcom_pull to get the aggregate result
    """
    ti = context["ti"]
    
    # YOUR CODE HERE
    # 1. Pull the result from the "aggregate" task
    # 2. Check if total_records > 5000
    # 3. Return the appropriate task_id as a string
    
    pass  # Remove this and add your implementation


def heavy_processing(**context):
    """Process large dataset with partitioning."""
    print("Performing heavy processing...")
    print("  - Partitioning data")
    print("  - Running distributed computation")
    time.sleep(2)
    return {"processing": "heavy"}


def light_processing(**context):
    """Process small dataset with simple logic."""
    print("Performing light processing...")
    print("  - Simple in-memory processing")
    time.sleep(1)
    return {"processing": "light"}


def cleanup(**context):
    """
    Cleanup task - should run regardless of upstream success/failure.
    """
    print("Running cleanup...")
    print("  - Removing temporary files")
    print("  - Releasing resources")
    return {"cleanup": "complete"}


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="dependency_exercise",
    description="Exercise: Complex task dependencies",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "dependencies"],
) as dag:
    
    # Start task
    start = EmptyOperator(task_id="start")
    
    # TODO: Create extraction tasks for each region
    # These should run in PARALLEL after start
    
    extract_us = PythonOperator(
        task_id="extract_us",
        python_callable=extract_region,
        op_kwargs={"region": "US"}
    )
    
    extract_eu = PythonOperator(
        task_id="extract_eu",
        python_callable=extract_region,
        op_kwargs={"region": "EU"}
    )
    
    extract_apac = PythonOperator(
        task_id="extract_apac",
        python_callable=extract_region,
        op_kwargs={"region": "APAC"}
    )
    
    # Aggregation task - waits for ALL extracts
    aggregate = PythonOperator(
        task_id="aggregate",
        python_callable=aggregate_data
    )
    
    # TODO: Create the branching task
    # Use BranchPythonOperator with choose_processing_path
    
    branch = BranchPythonOperator(
        task_id="choose_path",
        python_callable=choose_processing_path
    )
    
    # Processing paths
    heavy = PythonOperator(
        task_id="heavy_processing",
        python_callable=heavy_processing
    )
    
    light = PythonOperator(
        task_id="light_processing",
        python_callable=light_processing
    )
    
    # Join after branch
    # TODO: Add the correct trigger_rule for after branching
    join = EmptyOperator(
        task_id="join",
        # YOUR CODE HERE: Set trigger_rule to NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # Cleanup task
    # TODO: Add trigger_rule=ALL_DONE so it runs regardless of upstream status
    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        # YOUR CODE HERE: Set trigger_rule to ALL_DONE
    )
    
    # End task
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # ============================================================
    # TODO: Define Dependencies
    # ============================================================
    
    # 1. Fan-out: start triggers all extracts in parallel
    # Hint: start >> [extract_us, extract_eu, extract_apac]
    
    # YOUR CODE HERE
    
    
    # 2. Fan-in: aggregate waits for all extracts
    # Hint: [extract_us, extract_eu, extract_apac] >> aggregate
    
    # YOUR CODE HERE
    
    
    # 3. Branching: aggregate leads to branch, which leads to both paths
    # Hint: aggregate >> branch >> [heavy, light]
    
    # YOUR CODE HERE
    
    
    # 4. Join and cleanup
    # Hint: [heavy, light] >> join >> cleanup_task >> end
    
    # YOUR CODE HERE
