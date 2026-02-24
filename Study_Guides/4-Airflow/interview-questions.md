# Interview Questions: Week 4 - Apache Airflow

This question bank prepares trainees for technical interviews covering Apache Airflow fundamentals, DAG design, task orchestration, connections, sensors, and production operations.

---

## Beginner (Foundational)

### Q1: What is Apache Airflow, and what problem does it solve?
**Keywords:** Workflow orchestration, Pipelines as code, Scheduling, Monitoring, Dependencies

<details>
<summary>Click to Reveal Answer</summary>

Apache Airflow is an open-source **workflow orchestration platform** that allows you to programmatically author, schedule, and monitor workflows. It solves the problems of:
- **Dependency management:** Defining which tasks must complete before others
- **Monitoring:** Providing visibility into pipeline state through a web UI
- **Failure handling:** Automatic retries and alerting
- **Scheduling:** Time-based and dependency-based execution
- **Backfills:** Automatically running missed intervals

Unlike cron jobs which only offer time-based scheduling with no built-in dependency tracking, Airflow provides a complete orchestration solution.
</details>

---

### Q2: What does DAG stand for, and why must it be "acyclic"?
**Keywords:** Directed Acyclic Graph, No cycles, Termination, Dependencies

<details>
<summary>Click to Reveal Answer</summary>

DAG stands for **Directed Acyclic Graph**:
- **Directed:** Each edge has a direction (Task A -> Task B means A runs before B)
- **Acyclic:** There are no cycles (you cannot return to a node you have already visited)
- **Graph:** A collection of nodes (tasks) connected by edges (dependencies)

It must be acyclic because if a workflow had cycles, it would never complete. For example: A triggers B, B triggers C, C triggers A, A triggers B... creating an infinite loop. Airflow enforces the acyclic property to guarantee workflows terminate.
</details>

---

### Q3: What is the difference between an Operator and a Task?
**Keywords:** Template, Instance, Class, Operator type, Task ID

<details>
<summary>Click to Reveal Answer</summary>

- **Operator:** A template (class) that defines *what* action to perform. Examples include `PythonOperator`, `BashOperator`, and `PostgresOperator`.
- **Task:** A specific instance of an Operator within a DAG, with a unique `task_id`.

Think of it like object-oriented programming:
- `PythonOperator` is the class
- `extract_data_task` is an instance

Each task has its own configuration, dependencies, and execution history.
</details>

---

### Q4: Name three common Operators in Airflow and their purposes.
**Keywords:** PythonOperator, BashOperator, EmailOperator, Providers

<details>
<summary>Click to Reveal Answer</summary>

1. **PythonOperator:** Executes a Python callable (function)
2. **BashOperator:** Executes a bash command or script
3. **EmailOperator:** Sends an email notification

Other common operators include:
- **EmptyOperator (DummyOperator):** No-op for structuring DAGs
- **PostgresOperator:** Executes SQL against PostgreSQL
- **S3Operator:** Interacts with AWS S3
- **SparkSubmitOperator:** Submits Spark jobs

Airflow has hundreds of provider operators for external systems (AWS, GCP, Snowflake, etc.).
</details>

---

### Q5: What is the Airflow Scheduler, and what does it do?
**Keywords:** Parse DAGs, Create runs, Queue tasks, Heartbeat, Task instances

<details>
<summary>Click to Reveal Answer</summary>

The **Scheduler** is Airflow's heartbeat. It continuously:
1. **Parses DAG files** to discover new workflows
2. **Checks schedules** to determine what should run
3. **Creates DagRuns** for scheduled intervals
4. **Creates TaskInstances** for each task in a DagRun
5. **Queues tasks** whose dependencies are satisfied
6. **Monitors state** and handles retries

The Scheduler determines *what* and *when* tasks run, while the Executor handles *how* and *where*.
</details>

---

### Q6: What are the four main Executor types in Airflow?
**Keywords:** SequentialExecutor, LocalExecutor, CeleryExecutor, KubernetesExecutor

<details>
<summary>Click to Reveal Answer</summary>

| Executor | Description | Best For |
|----------|-------------|----------|
| **SequentialExecutor** | Runs one task at a time; no parallelism | Development only |
| **LocalExecutor** | Runs tasks in parallel using local subprocesses | Single-machine, small teams |
| **CeleryExecutor** | Distributes tasks across multiple workers via Celery | Production, horizontal scaling |
| **KubernetesExecutor** | Launches each task as a Kubernetes pod | Cloud-native, resource isolation |

Choose based on scale: Sequential (dev) -> Local (small) -> Celery/K8s (production).
</details>

---

### Q7: How do you define task dependencies in Airflow?
**Keywords:** Bitshift operators, >>, <<, set_upstream, set_downstream

<details>
<summary>Click to Reveal Answer</summary>

Dependencies are defined using **bitshift operators** (recommended):

```python
# Task A runs before Task B
task_a >> task_b

# Task A runs after Task B
task_a << task_b

# Chaining multiple tasks
task_a >> task_b >> task_c

# Parallel dependencies
task_a >> [task_b, task_c]  # A before B and C
[task_a, task_b] >> task_c   # A and B before C
```

Alternative method syntax:
```python
task_b.set_upstream(task_a)
task_a.set_downstream(task_b)
```
</details>

---

### Q8: What is a Connection in Airflow?
**Keywords:** Credentials, External systems, Encrypted, Conn ID, Hooks

<details>
<summary>Click to Reveal Answer</summary>

A **Connection** is Airflow's way of storing credentials and connection parameters for external systems. They are stored in the metadata database (encrypted) and accessed by ID.

Connection components:
- **Conn Id:** Unique identifier (e.g., `my_postgres_db`)
- **Conn Type:** Type of connection (Postgres, AWS, HTTP, etc.)
- **Host, Schema, Login, Password, Port:** Standard connection fields
- **Extra:** JSON for additional parameters

Connections can be created via the Airflow UI, CLI, or environment variables. Hooks use connections to interact with external systems.
</details>

---

### Q9: What is a Sensor in Airflow?
**Keywords:** Wait, External condition, Poke, Timeout, FileSensor

<details>
<summary>Click to Reveal Answer</summary>

A **Sensor** is a special type of operator that waits for a specific condition to be true before succeeding. Sensors repeatedly check ("poke") until the condition is met or a timeout occurs.

Common sensors:
- **FileSensor:** Wait for a file to exist
- **S3KeySensor:** Wait for an S3 object
- **ExternalTaskSensor:** Wait for another DAG's task
- **SqlSensor:** Wait for a database condition
- **HttpSensor:** Wait for an API endpoint

Sensors ensure downstream tasks do not run until their required conditions are met.
</details>

---

### Q10: What are XComs in Airflow?
**Keywords:** Cross-communication, Pass data, Tasks, Push, Pull

<details>
<summary>Click to Reveal Answer</summary>

**XComs** (Cross-Communications) pass data between tasks within a DAG run.

**Pushing data:**
```python
def my_task(**context):
    return {"count": 100}  # Auto-pushed as 'return_value'
    # Or explicitly: context["ti"].xcom_push(key="my_key", value=data)
```

**Pulling data:**
```python
def downstream_task(**context):
    data = context["ti"].xcom_pull(task_ids="my_task")
```

**Limitations:**
- Default size limit: ~48KB (stored in database)
- For large data, use external storage (S3) and pass references
</details>

---

## Intermediate (Application)

### Q11: Explain the difference between "poke" and "reschedule" sensor modes.
**Keywords:** Worker slot, Resource efficiency, Long waits, poke_interval
**Hint:** Think about what happens between checks.

<details>
<summary>Click to Reveal Answer</summary>

| Mode | Behavior | Best For |
|------|----------|----------|
| **Poke** (default) | Sensor holds a worker slot while waiting | Short waits, frequent checks |
| **Reschedule** | Sensor releases worker slot between pokes | Long waits, limited workers |

**Poke mode:**
- Continuously occupies a worker
- Lower latency at detecting condition
- Can exhaust worker pool if many sensors running

**Reschedule mode:**
- Releases worker between poke intervals
- More resource-efficient for long waits
- Slight overhead in rescheduling

Use reschedule mode when sensors might wait hours and worker slots are limited.
</details>

---

### Q12: What are Trigger Rules, and when would you use them?
**Keywords:** ALL_SUCCESS, ALL_DONE, ONE_FAILED, Conditional execution
**Hint:** Think about cleanup tasks and error handling.

<details>
<summary>Click to Reveal Answer</summary>

**Trigger Rules** control when a task runs based on upstream task states. The default is `ALL_SUCCESS` (all parents succeeded).

| Rule | Behavior |
|------|----------|
| `ALL_SUCCESS` | All parents succeeded (default) |
| `ALL_FAILED` | All parents failed |
| `ALL_DONE` | All parents completed (success or failure) |
| `ONE_SUCCESS` | At least one parent succeeded |
| `ONE_FAILED` | At least one parent failed |
| `NONE_FAILED` | No parent failed |
| `ALWAYS` | Run regardless of parent states |

**Common use cases:**
- **Cleanup task:** Use `ALL_DONE` to run cleanup even when upstream fails
- **Alert on failure:** Use `ONE_FAILED` to send alerts only when something fails
- **Branching resume:** Use `NONE_FAILED_MIN_ONE_SUCCESS` after branch operators
</details>

---

### Q13: How do you implement conditional branching in an Airflow DAG?
**Keywords:** BranchPythonOperator, Task ID, Skipped, Trigger rule
**Hint:** The branch operator returns the task_id to execute.

<details>
<summary>Click to Reveal Answer</summary>

Use **BranchPythonOperator** to execute different paths based on conditions:

```python
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    if context["execution_date"].weekday() == 0:  # Monday
        return "full_load"
    else:
        return "incremental_load"

branch = BranchPythonOperator(
    task_id="check_day",
    python_callable=choose_branch
)

full_load = PythonOperator(task_id="full_load", ...)
incremental_load = PythonOperator(task_id="incremental_load", ...)
merge = PythonOperator(
    task_id="merge",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Important!
)

branch >> [full_load, incremental_load] >> merge
```

The non-selected branch is marked "skipped." Use `NONE_FAILED_MIN_ONE_SUCCESS` for the join task.
</details>

---

### Q14: What is the difference between Connections and Hooks?
**Keywords:** Credentials, Interface, Methods, PostgresHook, BaseHook
**Hint:** Think about storage vs. usage.

<details>
<summary>Click to Reveal Answer</summary>

| Concept | Purpose | Example |
|---------|---------|---------|
| **Connection** | Stores credentials securely in database | `my_postgres` with host, user, password |
| **Hook** | Python interface that uses a Connection | `PostgresHook(postgres_conn_id="my_postgres")` |

**Connection:** The "what" - stores credentials (host, username, password, port)
**Hook:** The "how" - provides methods to interact with the system (query, insert, download)

```python
# Connection: stored in Airflow (UI/CLI/env var)
# Hook: uses connection to perform actions
hook = PostgresHook(postgres_conn_id="my_postgres")
records = hook.get_records("SELECT * FROM users")
```

Many operators use hooks internally.
</details>

---

### Q15: What are Airflow Variables, and how do they differ from XComs?
**Keywords:** Global configuration, Runtime, Key-value, DAG-specific
**Hint:** Think about scope and lifetime.

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | Variables | XComs |
|--------|-----------|-------|
| **Scope** | Global across all DAGs | Within a single DAG run |
| **Lifetime** | Persistent until deleted | Cleared after DAG run retention |
| **Purpose** | Configuration (environment, thresholds) | Passing data between tasks |
| **Storage** | Metadata database | Metadata database |
| **Size** | Typically small values | ~48KB limit |

**Variables:** Store global settings like environment names, API endpoints, batch sizes
```python
environment = Variable.get("environment")
```

**XComs:** Pass task outputs to downstream tasks within the same run
```python
data = ti.xcom_pull(task_ids="extract")
```
</details>

---

### Q16: Explain the concept of "idempotency" in Airflow tasks. Why is it important?
**Keywords:** Same result, Retries, Backfills, Safe re-execution
**Hint:** Think about what happens when a task is run multiple times.

<details>
<summary>Click to Reveal Answer</summary>

**Idempotency** means a task produces the same result whether run once or multiple times. This is important because:

1. **Retries:** Failed tasks are automatically retried; non-idempotent tasks may corrupt data
2. **Backfills:** Running historical intervals should not duplicate data
3. **Manual reruns:** Operators often manually rerun failed tasks

**Examples:**

Non-idempotent (bad):
```python
def insert_data():
    db.execute("INSERT INTO logs VALUES ('entry')")  # Duplicates on rerun
```

Idempotent (good):
```python
def insert_data(**context):
    ds = context["ds"]
    db.execute("DELETE FROM logs WHERE date = %s", ds)
    db.execute("INSERT INTO logs SELECT * FROM staging WHERE date = %s", ds)
```

Key patterns: Use date partitions, delete-then-insert, or upserts.
</details>

---

### Q17: What is the `catchup` parameter in a DAG definition?
**Keywords:** Backfill, Missed runs, Historical intervals, start_date
**Hint:** Consider what happens when a DAG is paused for days.

<details>
<summary>Click to Reveal Answer</summary>

The `catchup` parameter controls whether Airflow runs DAG runs for intervals between `start_date` and now.

```python
with DAG(
    dag_id="my_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True   # Run all intervals from Jan 1 to today
    # catchup=False  # Only run from today forward
) as dag:
```

**`catchup=True` (default):**
- Creates DagRuns for every missed interval
- Useful for backfilling historical data
- Can create many queued runs

**`catchup=False`:**
- Only schedules runs from now forward
- Useful when historical data is not needed
- Common for notification/alert DAGs

Most production DAGs use `catchup=False` to avoid unexpected backfills when deploying.
</details>

---

### Q18: How do you use Jinja templating in Airflow?
**Keywords:** Execution date, ds, params, var.value, Runtime substitution
**Hint:** Templates are rendered at runtime, not parse time.

<details>
<summary>Click to Reveal Answer</summary>

Airflow uses Jinja2 templating for dynamic value injection at runtime:

**Built-in variables:**
```python
# SQL with execution date
sql_task = PostgresOperator(
    task_id="query",
    sql="SELECT * FROM orders WHERE date = '{{ ds }}'"
)

# Bash with parameters
bash_task = BashOperator(
    task_id="process",
    bash_command="python process.py --date {{ ds }} --env {{ var.value.environment }}"
)
```

**Common template variables:**
- `{{ ds }}`: Execution date (YYYY-MM-DD)
- `{{ ds_nodash }}`: Execution date (YYYYMMDD)
- `{{ params.key }}`: DAG or task parameters
- `{{ var.value.key }}`: Airflow Variables
- `{{ ti.xcom_pull(...) }}`: XCom values

Templates are rendered at **runtime**, not when the DAG file is parsed.
</details>

---

### Q19: What is an ExternalTaskSensor, and when would you use it?
**Keywords:** Cross-DAG dependency, Wait, execution_delta, Upstream DAG
**Hint:** Think about coordinating pipelines that run on different schedules.

<details>
<summary>Click to Reveal Answer</summary>

**ExternalTaskSensor** waits for a task in another DAG to complete before proceeding.

```python
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

wait_for_extraction = ExternalTaskSensor(
    task_id="wait_for_extraction",
    external_dag_id="extraction_dag",
    external_task_id="final_task",
    execution_delta=timedelta(hours=2),  # Upstream runs 2 hours earlier
    poke_interval=60,
    timeout=3600
)
```

**Use cases:**
- Downstream DAG depends on upstream DAG completing
- Different teams own different DAGs
- Pipelines with different schedules need coordination

**Key parameter:** `execution_delta` aligns execution dates when DAGs run at different times.
</details>

---

### Q20: How do you implement a cleanup task that always runs, even when upstream tasks fail?
**Keywords:** Trigger rule, ALL_DONE, Error handling, Resource cleanup
**Hint:** Consider the trigger rule that runs after all parents complete regardless of state.

<details>
<summary>Click to Reveal Answer</summary>

Use the `ALL_DONE` trigger rule to run a task after all upstream tasks complete, regardless of success or failure:

```python
from airflow.utils.trigger_rule import TriggerRule

extract = PythonOperator(task_id="extract", ...)
transform = PythonOperator(task_id="transform", ...)
load = PythonOperator(task_id="load", ...)

cleanup = PythonOperator(
    task_id="cleanup",
    trigger_rule=TriggerRule.ALL_DONE,  # Runs even if upstream fails
    python_callable=cleanup_temp_files
)

extract >> transform >> load >> cleanup
```

Common patterns:
- Cleanup temporary files or staging tables
- Release database connections
- Send completion notifications
- Log final status
</details>

---

## Advanced (Deep Dive)

### Q21: Design a production-ready ETL DAG that orchestrates Kafka consumers and Spark jobs. What components would you include?
**Keywords:** Sensor, SparkSubmitOperator, Error handling, SLA, Monitoring
**Hint:** Think about the complete pipeline from data arrival to notification.

<details>
<summary>Click to Reveal Answer</summary>

**Production ETL DAG structure:**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    "owner": "data_team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["team@company.com"],
    "sla": timedelta(hours=2)
}

with DAG(
    dag_id="kafka_spark_etl",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # 6 AM daily
    catchup=False
) as dag:
    
    # 1. Wait for data to land
    wait_for_file = FileSensor(
        task_id="wait_for_landing_file",
        filepath="/data/landing/{{ ds }}/",
        mode="reschedule",
        timeout=7200
    )
    
    # 2. Trigger Spark job
    spark_job = BashOperator(
        task_id="run_spark_transform",
        bash_command="""
            spark-submit \
                --master yarn \
                --deploy-mode cluster \
                /opt/jobs/etl_job.py \
                --date {{ ds }}
        """
    )
    
    # 3. Validate output
    validate = PythonOperator(
        task_id="validate_output",
        python_callable=validate_data
    )
    
    # 4. Cleanup (always runs)
    cleanup = PythonOperator(
        task_id="cleanup",
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=cleanup_staging
    )
    
    # 5. Notify (only on failure)
    alert = EmailOperator(
        task_id="send_alert",
        trigger_rule=TriggerRule.ONE_FAILED,
        to="team@company.com",
        subject="Pipeline Failed",
        html_content="Check Airflow for details."
    )
    
    wait_for_file >> spark_job >> validate >> [cleanup, alert]
```

**Key components:** Sensor for data arrival, retries with backoff, SLAs, cleanup with `ALL_DONE`, alerts with `ONE_FAILED`.
</details>

---

### Q22: Explain the trade-offs between LocalExecutor, CeleryExecutor, and KubernetesExecutor for a production deployment.
**Keywords:** Scalability, Complexity, Resource isolation, Horizontal scaling
**Hint:** Consider team size, infrastructure, and workload characteristics.

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | LocalExecutor | CeleryExecutor | KubernetesExecutor |
|--------|---------------|----------------|-------------------|
| **Parallelism** | Single machine | Distributed | Distributed |
| **Scaling** | Vertical only | Horizontal (add workers) | Horizontal (pods) |
| **Complexity** | Low | Medium (requires broker) | High (requires K8s) |
| **Resource Isolation** | None (shared process) | Per-worker | Per-task (pod) |
| **Cost Efficiency** | Fixed cost | Always-on workers | Scale to zero |
| **Best For** | Small teams, dev/test | Production clusters | Cloud-native, variable load |

**Recommendations:**

- **LocalExecutor:** Small teams (<5 DAGs running concurrently), single-machine deployments, POCs
- **CeleryExecutor:** Traditional production deployments, predictable workloads, existing Redis/RabbitMQ infrastructure
- **KubernetesExecutor:** Cloud environments, unpredictable workloads (scale to zero), strong isolation requirements, mixed resource needs (some tasks need GPU)

Many organizations start with LocalExecutor, move to CeleryExecutor, then migrate to KubernetesExecutor as they mature.
</details>

---

### Q23: A DAG that normally takes 30 minutes is now taking 4 hours. How would you diagnose and fix this?
**Keywords:** Task duration, Dependencies, Resources, Logs, Scheduler lag
**Hint:** Check multiple levels: scheduler, executor, individual tasks.

<details>
<summary>Click to Reveal Answer</summary>

**Diagnosis approach:**

1. **Check Gantt chart:** Identify which tasks are slow vs. waiting
   - Tasks "queued" for long? -> Scheduler or executor bottleneck
   - Individual tasks slow? -> Task-level issue

2. **Review task logs:** Look for specific errors or slowdowns
   - Database connection timeouts
   - External API rate limiting
   - Data volume increase

3. **Check scheduler metrics:**
   - Is `dag_dir_list_interval` too long?
   - Is the DAG file parsing slow?
   - Pool exhaustion?

4. **Check executor:**
   - Are workers available?
   - Memory/CPU exhaustion on workers?
   - CeleryExecutor broker issues?

5. **Check dependencies:**
   - External services slow/down?
   - Database under load?
   - Network latency?

**Common fixes:**
- Increase worker count/parallelism
- Optimize slow tasks (better queries, smaller batches)
- Reduce pool contention
- Fix external service issues
- Increase resources (memory, CPU)
- Consider dynamic DAG optimization
</details>

---

### Q24: How would you implement dynamic DAG generation where tasks are created based on a configuration file?
**Keywords:** Loop, Configuration, Task factory, Maintainability
**Hint:** DAG files are Python; you can use loops and external data sources.

<details>
<summary>Click to Reveal Answer</summary>

**Dynamic DAG generation pattern:**

```python
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Load configuration
with open("/opt/airflow/config/tables.json") as f:
    tables = json.load(f)  # ["users", "orders", "products"]

def process_table(table_name):
    def _process(**context):
        print(f"Processing {table_name}")
    return _process

with DAG(
    dag_id="dynamic_table_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    # Dynamically create tasks from config
    tasks = []
    for table in tables:
        task = PythonOperator(
            task_id=f"process_{table}",
            python_callable=process_table(table)
        )
        tasks.append(task)
    
    start >> tasks >> end
```

**Best practices:**
- Keep config files version-controlled
- Validate config before DAG parsing
- Use consistent naming conventions
- Consider factory functions for complex task creation
- Test with different config sizes

**Caution:** Many dynamic tasks can slow scheduler parsing.
</details>

---

### Q25: Design an alerting and monitoring strategy for Airflow DAGs in production. What SLAs, alerts, and dashboards would you configure?
**Keywords:** SLA, Email alerts, Prometheus, Logging, Failure thresholds
**Hint:** Think about proactive vs. reactive monitoring.

<details>
<summary>Click to Reveal Answer</summary>

**Monitoring strategy:**

**1. SLAs (Service Level Agreements):**
```python
default_args = {
    "sla": timedelta(hours=2),  # Alert if task runs longer than 2 hours
}

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_slack_alert(f"SLA miss: {dag.dag_id}")

with DAG(
    dag_id="critical_etl",
    sla_miss_callback=sla_miss_callback,
    ...
)
```

**2. Task-level alerts:**
```python
default_args = {
    "email_on_failure": True,
    "email_on_retry": False,  # Avoid noise
    "email": ["oncall@company.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}
```

**3. DAG-level failure callbacks:**
```python
def dag_failure_callback(context):
    send_pagerduty_alert(context["dag"].dag_id)

with DAG(
    on_failure_callback=dag_failure_callback,
    ...
)
```

**4. Metrics export (Prometheus):**
- Configure StatsD exporter
- Monitor: `airflow_task_duration_seconds`, `airflow_dagrun_duration_seconds`
- Alert on: task failure rate, scheduler lag, pool slots exhausted

**5. Dashboard components:**
- DAG run success/failure rates
- Average task duration trends
- Active DAG runs count
- Scheduler heartbeat
- Worker utilization

**6. Proactive monitoring:**
- Daily health check DAG that validates dependencies
- Alerting on stuck tasks (running > X hours)
- Monitoring queue depth
</details>

---

## Study Tips

1. **Know the component hierarchy:** DAG > Task > Operator
2. **Understand scheduling:** Data intervals, catchup, execution dates
3. **Practice trigger rules:** Know when to use each one
4. **Draw DAG diagrams:** Visualize fan-out, fan-in, and branching patterns
5. **Explain real scenarios:** "Tell me about a time you debugged a failed DAG run"

---

*Generated by Quality Assurance Agent based on Airflow Week 4 curriculum content.*
