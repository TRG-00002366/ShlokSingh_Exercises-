"""
Complete ETL Pipeline Exercise
==============================
Build an end-to-end ETL pipeline with:
- File sensor
- Data validation
- Transformation
- Loading
- Error handling and notifications

Complete the TODO sections.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# TODO: Import FileSensor
# from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import os


# ============================================================
# Configuration
# ============================================================

INPUT_PATH = "/opt/airflow/data/input/orders.csv"
OUTPUT_PATH = "/opt/airflow/data/output/processed_orders.json"
MIN_RECORDS = 5


# ============================================================
# Callbacks
# ============================================================

def on_failure(context):
    """
    TODO: Implement failure callback.
    
    Log the failure details:
    - DAG ID
    - Task ID
    - Exception message
    """
    task = context.get("task_instance")
    exception = context.get("exception")
    
    print("=" * 50)
    print("PIPELINE FAILURE!")
    # YOUR CODE HERE
    print("=" * 50)


# ============================================================
# Task Functions
# ============================================================

def extract_data(**context):
    """
    TODO: Extract data from the CSV file.
    
    Steps:
    1. Read the CSV file
    2. Parse into list of dictionaries
    3. Validate minimum row count
    4. Push data to XCom
    5. Return extraction metadata
    """
    print(f"Extracting from {INPUT_PATH}...")
    
    # YOUR CODE HERE
    
    pass


def validate_data(**context):
    """
    TODO: Validate the extracted data.
    
    Steps:
    1. Pull data from extract task via XCom
    2. Check required columns exist
    3. Check for null order_ids
    4. Raise ValueError if validation fails
    5. Return validation summary
    """
    ti = context["ti"]
    
    print("Validating data...")
    
    # YOUR CODE HERE
    
    pass


def transform_data(**context):
    """
    TODO: Transform the data.
    
    Steps:
    1. Pull data from XCom
    2. Parse amounts as floats
    3. Add processing timestamp
    4. Calculate any aggregations
    5. Push transformed data to XCom
    """
    ti = context["ti"]
    
    print("Transforming data...")
    
    # YOUR CODE HERE
    
    pass


def load_data(**context):
    """
    TODO: Load data to destination.
    
    Steps:
    1. Pull transformed data from XCom
    2. Create output structure with metadata
    3. Write to JSON file
    4. Return load summary
    """
    ti = context["ti"]
    
    print(f"Loading to {OUTPUT_PATH}...")
    
    # YOUR CODE HERE
    
    pass


def send_notification(**context):
    """
    TODO: Send success notification.
    
    Pull results from load task and print summary.
    In production, this would send email/Slack/etc.
    """
    ti = context["ti"]
    
    print("=" * 50)
    print("PIPELINE SUCCESS!")
    # YOUR CODE HERE
    print("=" * 50)


# ============================================================
# DAG Definition
# ============================================================

# TODO: Configure default_args with retries and callbacks
default_args = {
    "owner": "data_team",
    # YOUR CODE HERE: Add retries, retry_delay, on_failure_callback
}

with DAG(
    dag_id="etl_exercise",
    description="Complete ETL pipeline exercise",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "etl"],
) as dag:
    
    # TODO: Create FileSensor to wait for input file
    # wait_for_file = FileSensor(
    #     task_id="wait_for_file",
    #     filepath=INPUT_PATH,
    #     poke_interval=30,
    #     timeout=3600,
    #     mode="poke"
    # )
    
    wait_for_file = EmptyOperator(task_id="wait_for_file")  # Replace with FileSensor
    
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )
    
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_data
    )
    
    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )
    
    load = PythonOperator(
        task_id="load",
        python_callable=load_data
    )
    
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=send_notification,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    
    # Dependencies
    wait_for_file >> extract >> validate >> transform >> load >> notify >> end
