# Lab: Configuring Connections and Using Hooks

## Overview
**Type:** Implementation (Code Lab)  
**Duration:** 2-3 hours  
**Mode:** Individual

## Learning Objectives
By completing this exercise, you will:
- Create and configure Airflow Connections via the UI
- Use PostgresHook to query a database from a DAG
- Access connection details programmatically with BaseHook
- Apply security best practices for credentials

## Prerequisites
- Running Airflow environment with PostgreSQL
- Understanding of Airflow Connections concept
- Completed previous exercises

---

## The Scenario

Your team needs to build a DAG that queries data from a PostgreSQL database. Instead of hardcoding credentials (a security risk), you'll use Airflow's Connections feature to store credentials securely and Hooks to access them in your code.

---

## Core Tasks

### Task 1: Create a Connection (20 minutes)

1. Open the Airflow UI
2. Navigate to Admin -> Connections
3. Click "+" to add a new connection
4. Fill in:
   - **Conn Id:** `exercise_postgres`
   - **Conn Type:** Postgres
   - **Host:** `postgres` (or your database host)
   - **Schema:** `airflow`
   - **Login:** `airflow`
   - **Password:** `airflow`
   - **Port:** `5432`

5. Click "Test" to verify the connection works
6. Save the connection

### Task 2: Complete the DAG (45 minutes)

Navigate to `starter_code/dags/connections_exercise_dag.py`.

Complete the TODO sections:

1. **Import the required hooks:**
   - PostgresHook from airflow.providers.postgres.hooks.postgres
   - BaseHook from airflow.hooks.base

2. **Implement `query_database()`:**
   - Create a PostgresHook using your connection ID
   - Execute a query to count DAG runs
   - Print and return the results

3. **Implement `show_connection_info()`:**
   - Use BaseHook.get_connection() to retrieve connection details
   - Print the host, schema, and login (NOT the password!)

4. **Implement `query_task_instances()`:**
   - Use the hook to query recent task instances
   - Return the count

### Task 3: Deploy and Test (30 minutes)

1. Copy your DAG to the Airflow dags folder
2. Wait for it to appear in the UI
3. Trigger the DAG
4. Check the logs for each task:
   - Verify the database query results appear
   - Verify connection info is printed (without password)

### Task 4: Security Analysis (20 minutes)

Complete `deliverables/security_analysis.md`:

1. Why is it bad to hardcode passwords in DAG files?
2. What other methods could you use to store secrets?
3. What would happen if someone deleted the connection?

---

## Stretch Goals

1. **Add HTTP Connection:**
   Create a connection for an HTTP API and use HttpHook

2. **Connection from Environment:**
   Configure a connection using environment variables instead of the UI

3. **Custom Hook Wrapper:**
   Create a utility function that wraps common queries

---

## Definition of Done

- [ ] Connection created in Airflow UI
- [ ] Connection test passes
- [ ] DAG executes without errors
- [ ] Query results visible in task logs
- [ ] Password NOT printed in logs
- [ ] Security analysis complete

---

## Submission

1. Completed `connections_exercise_dag.py`
2. Screenshot of successful connection in UI
3. Completed `security_analysis.md`

---

## Resources

- Written Content: `connections-and-hooks.md`
- Demo Reference: `demo_connections_hooks/`
