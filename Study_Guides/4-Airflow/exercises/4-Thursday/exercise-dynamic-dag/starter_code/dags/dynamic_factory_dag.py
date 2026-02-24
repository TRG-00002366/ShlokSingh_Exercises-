"""
Dynamic DAG Factory - Pair Programming Exercise
================================================
Build a DAG that generates tasks dynamically from a JSON configuration.

This exercise is designed for pair programming:
- Driver: Writes the code
- Navigator: Reviews and guides

Switch roles at each phase!

Complete the TODO sections together.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from pathlib import Path
import json
import time


# ============================================================
# Phase 1: Configuration Loading
# ============================================================

# TODO: Define the path to the config file
# Hint: Use Path(__file__).parent to get the dags folder
CONFIG_PATH = Path(__file__).parent / "config" / "pipelines.json"


def load_configuration():
    """
    TODO: Load and validate the JSON configuration.
    
    Steps:
    1. Check if the file exists
    2. Load the JSON content
    3. Validate that 'clients' key exists
    4. Return the clients list
    
    Handle errors gracefully!
    """
    # YOUR CODE HERE
    
    try:
        with open(CONFIG_PATH) as f:
            config = json.load(f)
        
        if "clients" not in config:
            raise ValueError("Config must contain 'clients' key")
        
        return config["clients"]
    
    except FileNotFoundError:
        print(f"Config file not found: {CONFIG_PATH}")
        return []
    except json.JSONDecodeError as e:
        print(f"Invalid JSON: {e}")
        return []


# Load configuration at parse time
CLIENTS = load_configuration()


# ============================================================
# Phase 3: Task Functions
# ============================================================

def extract_table(client_name: str, table_name: str, **context):
    """
    TODO: Simulate extracting data from a table.
    
    Steps:
    1. Print extraction message
    2. Simulate work (time.sleep)
    3. Return record count
    """
    print(f"[{client_name}] Extracting table: {table_name}")
    
    # YOUR CODE HERE
    
    pass


def transform_client_data(client_name: str, tables: list, **context):
    """
    TODO: Transform data from all tables for a client.
    
    Steps:
    1. Pull XCom data from each extract task
    2. Aggregate the results
    3. Print summary
    4. Return aggregated data
    
    Hint: Task IDs follow pattern "{client_name}.extract_{table}"
    """
    ti = context["ti"]
    
    print(f"[{client_name}] Transforming data from {len(tables)} tables")
    
    # YOUR CODE HERE
    
    pass


def load_client_data(client_name: str, **context):
    """
    TODO: Load transformed data for a client.
    
    Steps:
    1. Pull transform result from XCom
    2. Simulate loading
    3. Print completion message
    """
    print(f"[{client_name}] Loading data to destination")
    
    # YOUR CODE HERE
    
    pass


# ============================================================
# Phase 2: DAG Structure
# ============================================================

with DAG(
    dag_id="dynamic_factory",
    description="Dynamic DAG generated from JSON configuration",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "dynamic", "pair-programming"],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id="start")
    
    # End marker
    end = EmptyOperator(task_id="end")
    
    # TODO: Generate TaskGroups for each client
    # 
    # For each client in CLIENTS:
    #   1. Create a TaskGroup with the client name
    #   2. Create extract tasks for each table
    #   3. Create transform task
    #   4. Create load task
    #   5. Connect dependencies within the group
    #   6. Connect start -> group -> end
    
    for client in CLIENTS:
        client_name = client["name"]
        tables = client.get("tables", [])
        
        # TODO: Create TaskGroup for this client
        with TaskGroup(group_id=client_name) as client_group:
            
            # TODO: Create extract tasks for each table
            extract_tasks = []
            for table in tables:
                # YOUR CODE HERE
                pass
            
            # TODO: Create transform task
            # YOUR CODE HERE
            
            # TODO: Create load task
            # YOUR CODE HERE
            
            # TODO: Set dependencies within the group
            # All extracts -> transform -> load
            # YOUR CODE HERE
            pass
        
        # TODO: Connect to start and end
        # start >> client_group >> end
        # YOUR CODE HERE


# Documentation
dag.doc_md = f"""
## Dynamic DAG Factory

This DAG is generated from `config/pipelines.json`.

### Current Clients:
{chr(10).join([f"- **{c['name']}**: {len(c.get('tables', []))} tables" for c in CLIENTS]) if CLIENTS else "No clients configured"}

### Pair Programming Exercise:
- Phase 1: Configuration (Partner A drives)
- Phase 2: DAG Structure (Partner B drives)
- Phase 3: Task Functions (Partner A drives)
- Phase 4: Testing (Partner B drives)
"""
