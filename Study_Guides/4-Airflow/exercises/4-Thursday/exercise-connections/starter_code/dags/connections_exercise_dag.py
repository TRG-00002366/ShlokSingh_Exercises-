"""
Connections and Hooks Exercise
==============================
Practice using Airflow Connections and Hooks to
securely interact with external systems.

Prerequisites:
- Create a connection named 'exercise_postgres' in Admin -> Connections

Complete the TODO sections.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# TODO: Import the required hooks
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.hooks.base import BaseHook

# YOUR IMPORTS HERE


# Connection ID - must match what you created in the UI
CONNECTION_ID = "exercise_postgres"


def query_database(**context):
    """
    TODO: Query the Airflow metadata database.
    
    Steps:
    1. Create a PostgresHook using CONNECTION_ID
    2. Execute a query to count DAG runs
    3. Print and return the results
    
    Example query: "SELECT COUNT(*) FROM dag_run"
    
    Hint:
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        result = hook.get_first("SELECT ...")
    """
    # YOUR CODE HERE
    
    pass


def show_connection_info(**context):
    """
    TODO: Display connection information (without the password!).
    
    Steps:
    1. Use BaseHook.get_connection(CONNECTION_ID)
    2. Print the conn_id, conn_type, host, schema, login
    3. Do NOT print the password!
    
    Hint:
        conn = BaseHook.get_connection(CONNECTION_ID)
        print(f"Host: {conn.host}")
    """
    # YOUR CODE HERE
    
    pass


def query_task_instances(**context):
    """
    TODO: Query recent task instances.
    
    Steps:
    1. Create a PostgresHook
    2. Query for recent task instances (limit 10)
    3. Print each task's dag_id, task_id, and state
    4. Return the count
    
    Example query:
        SELECT dag_id, task_id, state 
        FROM task_instance 
        ORDER BY start_date DESC 
        LIMIT 10
    """
    # YOUR CODE HERE
    
    pass


# ============================================================
# DAG Definition
# ============================================================

with DAG(
    dag_id="connections_exercise",
    description="Exercise: Using Connections and Hooks",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "connections", "hooks"],
) as dag:
    
    query_task = PythonOperator(
        task_id="query_database",
        python_callable=query_database
    )
    
    show_info_task = PythonOperator(
        task_id="show_connection_info",
        python_callable=show_connection_info
    )
    
    query_ti_task = PythonOperator(
        task_id="query_task_instances",
        python_callable=query_task_instances
    )
    
    # Run in sequence
    query_task >> show_info_task >> query_ti_task


dag.doc_md = """
## Connections Exercise

This DAG demonstrates using Airflow Connections and Hooks.

### Prerequisites:
Create a connection in Admin -> Connections:
- Conn Id: `exercise_postgres`
- Conn Type: Postgres
- Host: `postgres`
- Schema: `airflow`
- Login: `airflow`
- Password: `airflow`
- Port: `5432`
"""
