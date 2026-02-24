# Interview Questions: Week 5 - Snowflake and dbt

This question bank prepares trainees for technical interviews covering Snowflake architecture, data warehouse concepts, Medallion architecture, dimensional modeling, Streams/Tasks, and dbt transformation workflows.

---

## Beginner (Foundational)

### Q1: What are the key differences between a data warehouse, data lake, and lakehouse?
**Keywords:** Schema-on-write, Schema-on-read, Structured, ACID, Flexibility

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | Data Warehouse | Data Lake | Lakehouse |
|--------|---------------|-----------|-----------|
| **Data Types** | Structured only | All types | All types |
| **Schema** | Schema-on-write | Schema-on-read | Flexible |
| **Cost** | Higher | Lower | Moderate |
| **Query Performance** | Excellent | Variable | Good to Excellent |
| **Primary Users** | Business Analysts | Data Scientists | Both |
| **ACID Support** | Yes | Limited | Yes |

- **Data warehouse:** Centralized, structured, optimized for analytics with ETL
- **Data lake:** Raw data in native format, flexible schema, cost-effective
- **Lakehouse:** Unified platform combining lake economics with warehouse reliability
</details>

---

### Q2: What is the Medallion architecture, and what are its three layers?
**Keywords:** Bronze, Silver, Gold, Data quality, Progressive refinement

<details>
<summary>Click to Reveal Answer</summary>

The **Medallion architecture** organizes data into three layers with progressive quality refinement:

| Layer | Purpose | Characteristics |
|-------|---------|-----------------|
| **Bronze** | Raw data landing | Append-only, full fidelity, original format |
| **Silver** | Cleansed data | Deduplicated, validated, typed, conformed |
| **Gold** | Business-ready | Aggregated, denormalized, consumption-optimized |

Data quality increases as it flows from Bronze (raw) to Gold (analytics-ready). This pattern enables auditing, reprocessing, and maintainable pipelines.
</details>

---

### Q3: Describe Snowflake's three-layer architecture.
**Keywords:** Database Storage, Query Processing, Cloud Services, Separation of compute

<details>
<summary>Click to Reveal Answer</summary>

Snowflake has three independent layers:

1. **Database Storage Layer:**
   - Stores all data centrally in cloud object storage
   - Uses columnar, compressed micro-partitions (50-500 MB each)
   - Automatic compression and optimization

2. **Query Processing Layer (Virtual Warehouses):**
   - Independent compute clusters that execute queries
   - Can be started, stopped, and resized on demand
   - Multiple warehouses can query the same data

3. **Cloud Services Layer:**
   - Authentication, access control, metadata management
   - Query parsing, optimization, and result caching
   - Transaction management and infrastructure coordination

**Key benefit:** Compute separation enables workload isolation, independent scaling, and cost optimization.
</details>

---

### Q4: What is a virtual warehouse in Snowflake?
**Keywords:** Compute cluster, On-demand, Scaling, Auto-suspend, Credits

<details>
<summary>Click to Reveal Answer</summary>

A **virtual warehouse** is an independent compute cluster in Snowflake that executes queries. Key characteristics:

- **On-demand:** Start, stop, suspend, and resize instantly
- **Independent:** Warehouses don't share resources; multiple can query the same data
- **Sized by T-shirt sizes:** X-Small (1 credit/hour) through 6X-Large
- **Auto-features:** AUTO_SUSPEND (stop when idle), AUTO_RESUME (start when queried)

```sql
CREATE WAREHOUSE my_warehouse
    WITH WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300         -- Suspend after 5 minutes
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;
```

Cost is based on credits per hour, and each size doubles compute/cost.
</details>

---

### Q5: What is a micro-partition in Snowflake?
**Keywords:** Columnar storage, Immutable, Pruning, Time Travel, Compression

<details>
<summary>Click to Reveal Answer</summary>

A **micro-partition** is Snowflake's storage unit. Key characteristics:

- Contains 50-500 MB of uncompressed data
- Stored in a compressed, columnar format
- Immutable (never modified, only replaced)
- Automatically organized based on ingestion order
- Enables efficient query pruning (skip irrelevant partitions)
- Supports Time Travel by retaining historical versions

Snowflake automatically manages partitioning; you don't create partitions manually like in traditional databases.
</details>

---

### Q6: What is a Snowflake Stream, and what does it track?
**Keywords:** Change data capture, CDC, INSERT, UPDATE, DELETE, Incremental

<details>
<summary>Click to Reveal Answer</summary>

A **Stream** is a Snowflake object that tracks changes (INSERT, UPDATE, DELETE) to a table, enabling change data capture (CDC).

**Key characteristics:**
- Records changes since the last consumption
- Lightweight; no data duplication
- Includes metadata columns: `METADATA$ACTION`, `METADATA$ISUPDATE`, `METADATA$ROW_ID`
- Consumed when used in DML transactions

```sql
CREATE STREAM orders_stream ON TABLE orders;

-- Query stream to see changes
SELECT order_id, METADATA$ACTION, METADATA$ISUPDATE
FROM orders_stream;
```

Updates appear as a DELETE followed by INSERT (both with `METADATA$ISUPDATE = TRUE`).
</details>

---

### Q7: What is a Snowflake Task, and how is it used?
**Keywords:** Scheduled, Cron, Automation, SQL execution, WHEN clause

<details>
<summary>Click to Reveal Answer</summary>

A **Task** is a scheduled Snowflake object that executes SQL statements on a defined schedule or when triggered.

**Key characteristics:**
- Cron-based or interval scheduling
- Can depend on other tasks (DAG chains)
- Serverless or warehouse-based compute
- Integrates with Streams for event-driven processing

```sql
CREATE TASK hourly_summary
    WAREHOUSE = compute_wh
    SCHEDULE = '60 MINUTE'
AS
    INSERT INTO hourly_metrics SELECT ...;

-- Event-driven: only run when stream has data
CREATE TASK process_changes
    WAREHOUSE = compute_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('orders_stream')
AS
    MERGE INTO silver.orders ...;
```

Tasks are created in suspended state; you must `ALTER TASK ... RESUME` to activate.
</details>

---

### Q8: What is dbt, and what problem does it solve?
**Keywords:** Transformation, ELT, SQL, Version control, Testing, Documentation

<details>
<summary>Click to Reveal Answer</summary>

**dbt (data build tool)** is an open-source tool that transforms data in your warehouse using SQL. It handles the "T" in ELT (Extract, Load, Transform).

**Key capabilities:**
- Transform data using SQL SELECT statements
- Manage dependencies between transformations (DAG)
- Enable testing and documentation
- Integrate with version control (Git)
- Support modular, reusable models

**Why dbt matters:**
- Brings software engineering practices to analytics (version control, testing, modularity)
- Transforms raw data (Bronze/Silver) into analytics-ready data (Gold)
- Generates documentation and lineage graphs automatically
</details>

---

### Q9: What is the difference between a fact table and a dimension table?
**Keywords:** Measures, Attributes, Foreign keys, Star schema, Grain

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | Fact Table | Dimension Table |
|--------|------------|-----------------|
| **Contains** | Measures (numeric values) | Attributes (descriptive text) |
| **Keys** | Foreign keys to dimensions | Surrogate key (primary key) |
| **Size** | Largest tables (events/transactions) | Smaller tables |
| **Purpose** | "What happened and how much" | "Who, what, where, when, why" |
| **Example** | `fact_sales` (quantity, amount) | `dim_customer` (name, segment) |

**Fact table:** One row per business event (e.g., sale, order line)
**Dimension table:** Descriptive context for facts (e.g., customer info, product details)
</details>

---

### Q10: What is a star schema?
**Keywords:** Fact table, Dimension tables, Denormalized, Joins, Performance

<details>
<summary>Click to Reveal Answer</summary>

A **star schema** is a dimensional modeling design with a central fact table surrounded by dimension tables, forming a star shape.

```
            dim_date
               |
dim_product --[fact_sales]-- dim_customer
               |
           dim_store
```

**Characteristics:**
- Dimensions directly connected to the fact table
- Dimensions are denormalized (all attributes in one table)
- Simple, fast queries with minimal joins
- Preferred for most analytical workloads

Star schema is simpler and faster than snowflake schema (which normalizes dimensions into sub-tables).
</details>

---

## Intermediate (Application)

### Q11: Explain how to combine Streams and Tasks to build an incremental pipeline in Snowflake.
**Keywords:** CDC, MERGE, SYSTEM$STREAM_HAS_DATA, Event-driven, Bronze to Silver
**Hint:** Think about the WHEN clause and consuming the stream.

<details>
<summary>Click to Reveal Answer</summary>

**Pattern for incremental Bronze-to-Silver pipeline:**

```sql
-- 1. Stream on Bronze table
CREATE STREAM bronze.orders_stream ON TABLE bronze.raw_orders;

-- 2. Task to process stream (event-driven)
CREATE TASK bronze_to_silver_task
    WAREHOUSE = etl_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.orders_stream')
AS
    MERGE INTO silver.orders t
    USING (
        SELECT order_id, customer_id, amount
        FROM bronze.orders_stream
        WHERE METADATA$ACTION = 'INSERT'
    ) s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...;

-- 3. Resume the task
ALTER TASK bronze_to_silver_task RESUME;
```

**Key points:**
- `SYSTEM$STREAM_HAS_DATA()` prevents unnecessary runs when no changes exist
- Stream is consumed (reset) after the DML commits
- Task chains can create multi-step pipelines (parent -> child tasks)
</details>

---

### Q12: What are the dbt materializations, and when would you use each?
**Keywords:** View, Table, Incremental, Ephemeral, Performance, Data volume
**Hint:** Consider data size and refresh frequency.

<details>
<summary>Click to Reveal Answer</summary>

| Materialization | Description | Use Case |
|-----------------|-------------|----------|
| **view** | Creates SQL view | Staging models, simple transforms, low query frequency |
| **table** | Creates full table (drops/recreates) | Mart models, dimension tables, small-medium data |
| **incremental** | Appends/merges new data only | Large fact tables, event logs, high volume |
| **ephemeral** | CTE only, not materialized | Intermediate calculations, DRY logic |

**Configuration:**
```sql
{{ config(materialized='incremental', unique_key='order_id') }}

SELECT * FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**Decision factors:** Data volume, refresh frequency, query patterns, compute cost
</details>

---

### Q13: How does Snowflake's compute separation benefit workload management?
**Keywords:** Workload isolation, Concurrency, Cost optimization, Scaling
**Hint:** Think about different teams and use cases.

<details>
<summary>Click to Reveal Answer</summary>

Compute separation (storage separate from compute) enables:

**1. Workload Isolation:**
```sql
CREATE WAREHOUSE etl_warehouse       WITH WAREHOUSE_SIZE = 'LARGE';
CREATE WAREHOUSE analytics_warehouse WITH WAREHOUSE_SIZE = 'MEDIUM';
CREATE WAREHOUSE datascience_warehouse WITH WAREHOUSE_SIZE = 'SMALL';
```
Different teams/workloads don't compete for resources.

**2. Cost Optimization:**
- Use larger warehouses for ETL (faster completion)
- Use smaller warehouses for ad-hoc queries
- Suspend warehouses when not in use (no compute cost)

**3. Concurrency Without Contention:**
- Multiple warehouses can query the same tables simultaneously
- No locking or resource contention between workloads

**4. Independent Scaling:**
- Scale up: Increase warehouse size for faster individual queries
- Scale out: Add multi-cluster warehouses for more concurrent queries
</details>

---

### Q14: Explain Slowly Changing Dimensions (SCD) Types 1, 2, and 3.
**Keywords:** Historical data, Overwrite, New row, Previous column, Tracking changes
**Hint:** Think about what history you need to preserve.

<details>
<summary>Click to Reveal Answer</summary>

Slowly Changing Dimensions handle how dimension attributes change over time:

**SCD Type 1: Overwrite**
- Replace old value with new value
- No history preserved
- Simple but loses context
```sql
UPDATE dim_customer SET address = '456 New St' WHERE customer_id = 'C001';
```

**SCD Type 2: Add New Row**
- Create new row for each change
- Full history with effective dates and current flag
- Most common for analytics
```sql
-- Expire current row
UPDATE dim_customer SET current_flag = FALSE, end_date = CURRENT_DATE()
WHERE customer_id = 'C001' AND current_flag = TRUE;
-- Insert new row
INSERT INTO dim_customer (..., current_flag, start_date) VALUES (..., TRUE, CURRENT_DATE());
```

**SCD Type 3: Add New Column**
- Add column for previous value
- Limited history (typically one previous value)
```sql
UPDATE dim_customer SET previous_address = address, address = '456 New St';
```
</details>

---

### Q15: How do you use the `ref()` and `source()` functions in dbt?
**Keywords:** Dependencies, DAG, Source tables, Model references, Lineage
**Hint:** Think about where the data comes from.

<details>
<summary>Click to Reveal Answer</summary>

**`source()`** references raw tables (Bronze layer):
```sql
-- Define sources in sources.yml
-- models/staging/stg_orders.sql
SELECT * FROM {{ source('raw', 'orders') }}
-- Compiles to: SELECT * FROM raw.orders
```

**`ref()`** references other dbt models:
```sql
-- models/marts/fct_orders.sql
SELECT * FROM {{ ref('stg_orders') }}
-- Compiles to: SELECT * FROM analytics.stg_orders
```

**Why this matters:**
- dbt builds a dependency graph (DAG) from these references
- Models run in correct order (dependencies first)
- Lineage is automatically documented
- Changing a referenced model's name updates all references
</details>

---

### Q16: What is the difference between additive, semi-additive, and non-additive facts?
**Keywords:** Aggregation, Measures, Time dimension, SUM, AVG
**Hint:** Think about whether summing makes sense across all dimensions.

<details>
<summary>Click to Reveal Answer</summary>

| Type | Description | Example | Allowed Operations |
|------|-------------|---------|-------------------|
| **Additive** | Can be summed across all dimensions | Revenue, Quantity | SUM across any dimension |
| **Semi-Additive** | Can be summed across some dimensions (not time) | Account Balance, Inventory | SUM across non-time dimensions; use latest/AVG for time |
| **Non-Additive** | Cannot be summed; must use other aggregations | Unit Price, Ratio, Percentage | AVG, COUNT, weighted average |

**Example:**
- Revenue: Summing across products, dates, and customers gives meaningful total (additive)
- Account Balance: Summing balances across customers is meaningful, but summing across dates is not (semi-additive)
- Average Order Value: Summing averages doesn't give a meaningful average; must recalculate (non-additive)
</details>

---

### Q17: What is Snowpipe, and how does it differ from COPY INTO?
**Keywords:** Continuous loading, Event-driven, Auto-ingest, Micro-batches, Serverless
**Hint:** Think about when data arrives vs. scheduled loading.

<details>
<summary>Click to Reveal Answer</summary>

| Aspect | COPY INTO | Snowpipe |
|--------|-----------|----------|
| **Trigger** | Manual or scheduled | Event-driven (file arrival) |
| **Latency** | Batch (minutes/hours) | Near real-time (seconds/minutes) |
| **Compute** | Uses virtual warehouse | Serverless (Snowflake-managed) |
| **Best For** | Scheduled batch loads | Continuous streaming data |

**Snowpipe workflow:**
1. File lands in cloud storage (S3, Azure Blob, GCS)
2. Event notification triggers Snowpipe
3. Snowpipe loads data automatically (serverless)
4. Data available in near real-time

**COPY INTO:** Manual or scheduled loading from stage to table
```sql
COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = 'CSV');
```

**Snowpipe:** Continuous, event-driven loading
```sql
CREATE PIPE my_pipe AUTO_INGEST = TRUE AS
    COPY INTO my_table FROM @my_stage FILE_FORMAT = (TYPE = 'CSV');
```
</details>

---

### Q18: How do you implement an incremental model in dbt?
**Keywords:** is_incremental(), unique_key, full_refresh, Merge, Append
**Hint:** Think about identifying new vs. existing rows.

<details>
<summary>Click to Reveal Answer</summary>

**Incremental model pattern:**
```sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

SELECT
    order_id,
    customer_id,
    order_amount,
    processed_at
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    -- Only process rows newer than the last run
    WHERE processed_at > (SELECT MAX(processed_at) FROM {{ this }})
{% endif %}
```

**How it works:**
1. **First run:** Full table build (like table materialization)
2. **Subsequent runs:** Only new rows (WHERE clause filters)
3. **Merge/append:** Based on `unique_key` setting

**Incremental strategies:**
- `append`: Insert only, no updates
- `delete+insert`: Delete matching keys, then insert
- `merge`: MERGE statement (update or insert)

**Full refresh override:**
```bash
dbt run --full-refresh --select fct_orders
```
</details>

---

### Q19: What are surrogate keys, and why use them instead of natural keys?
**Keywords:** Synthetic key, Performance, SCD, Source changes, Integer
**Hint:** Think about what happens when source systems change.

<details>
<summary>Click to Reveal Answer</summary>

**Natural Key:** Business identifier from source system (e.g., `customer_id = 'CUST001'`)
**Surrogate Key:** Synthetic, system-generated identifier (e.g., `customer_key = 12345`)

**Why use surrogate keys:**

1. **Handle source key changes:** If source renames keys, your warehouse is unaffected
2. **Support SCD Type 2:** Multiple rows for same natural key (different versions)
3. **Improve performance:** Integer joins are faster than string joins
4. **Protect against source issues:** Source key reuse, gaps, or format changes

**Generation in dbt:**
```sql
{{ dbt_utils.generate_surrogate_key(['customer_id', 'start_date']) }} AS customer_key
```

**Best practice:** Keep both surrogate key (for joins) and natural key (for matching to source).
</details>

---

### Q20: Explain result caching in Snowflake. When is it used, and when is it bypassed?
**Keywords:** Cloud Services, Query results, No compute cost, Cache invalidation
**Hint:** Consider what causes the cache to be invalidated.

<details>
<summary>Click to Reveal Answer</summary>

**Result caching** stores query results in the Cloud Services layer. If the same query runs again and data hasn't changed, results return instantly without warehouse compute cost.

**When cache is used:**
- Exact same query text
- Same user role
- Underlying data unchanged
- Within 24-hour cache retention

**When cache is bypassed:**
- Data has been modified (INSERT, UPDATE, DELETE)
- Query text differs (even whitespace changes)
- Different user role
- USE_CACHED_RESULT = FALSE
- Query includes non-deterministic functions (CURRENT_TIMESTAMP)

**Example:**
```sql
-- First execution: Uses warehouse (costs credits)
SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-01';
-- Cached

-- Second execution: Returns from cache (no compute cost)
SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-01';
```
</details>

---

## Advanced (Deep Dive)

### Q21: Design a complete data pipeline using Snowflake Streams, Tasks, and the Medallion architecture.
**Keywords:** Bronze, Silver, Gold, CDC, Task DAG, Incremental processing
**Hint:** Think about the flow from raw data landing to business-ready analytics.

<details>
<summary>Click to Reveal Answer</summary>

**Complete pipeline architecture:**

```sql
-- 1. BRONZE: Raw data landing (loaded via Snowpipe or COPY)
CREATE TABLE bronze.raw_orders (
    ingestion_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    raw_data VARIANT
);

CREATE STREAM bronze.orders_stream ON TABLE bronze.raw_orders;

-- 2. SILVER: Cleansed, typed data
CREATE TABLE silver.orders (
    order_id STRING PRIMARY KEY,
    customer_id STRING,
    order_date DATE,
    amount DECIMAL(12,2),
    processed_at TIMESTAMP_NTZ
);

CREATE STREAM silver.orders_stream ON TABLE silver.orders;

-- 3. GOLD: Aggregated metrics
CREATE TABLE gold.daily_order_metrics (
    order_date DATE PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(14,2),
    avg_order_value DECIMAL(10,2)
);

-- 4. TASKS: Bronze -> Silver -> Gold pipeline
CREATE TASK bronze_to_silver
    WAREHOUSE = etl_wh
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('bronze.orders_stream')
AS
    INSERT INTO silver.orders
    SELECT 
        raw_data:order_id::STRING,
        raw_data:customer_id::STRING,
        raw_data:order_date::DATE,
        raw_data:amount::DECIMAL(12,2),
        CURRENT_TIMESTAMP()
    FROM bronze.orders_stream
    WHERE METADATA$ACTION = 'INSERT';

CREATE TASK silver_to_gold
    WAREHOUSE = etl_wh
    AFTER bronze_to_silver  -- Runs after parent completes
AS
    MERGE INTO gold.daily_order_metrics t
    USING (
        SELECT order_date, COUNT(*) total, SUM(amount) revenue, AVG(amount) avg_val
        FROM silver.orders_stream WHERE METADATA$ACTION = 'INSERT'
        GROUP BY order_date
    ) s
    ON t.order_date = s.order_date
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...;

-- 5. Resume tasks (child first, then parent)
ALTER TASK silver_to_gold RESUME;
ALTER TASK bronze_to_silver RESUME;
```
</details>

---

### Q22: Compare and contrast approaches for handling large-scale historical data loading in Snowflake.
**Keywords:** COPY INTO, Batch size, Parallel loading, Warehouse sizing, File optimization
**Hint:** Consider file sizing, parallelism, and resource allocation.

<details>
<summary>Click to Reveal Answer</summary>

**Best practices for large-scale loading:**

**1. File Optimization:**
- Split files into 100-250 MB compressed chunks
- Use columnar formats (Parquet) when possible
- Avoid single mega-files or thousands of tiny files

**2. Warehouse Sizing:**
- Larger warehouses = more parallel threads
- Cost is same (larger = faster but shorter duration)
- Use Large/X-Large for initial bulk loads

**3. Loading Approach:**

| Approach | Best For | Configuration |
|----------|----------|---------------|
| **COPY INTO (parallel)** | Initial bulk load | Multiple files, large warehouse |
| **Snowpipe** | Continuous streaming | Auto-ingest from cloud events |
| **External tables** | Query-in-place, delayed load | Access without loading |

**4. COPY INTO optimization:**
```sql
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (TYPE = 'PARQUET')
PATTERN = '.*[.]parquet'
ON_ERROR = 'CONTINUE';  -- Don't fail entire load on bad rows
```

**5. Monitoring:**
- Check `COPY_HISTORY` for load performance
- Monitor warehouse utilization
- Validate row counts after load

**6. Clustering:**
- For very large tables, consider clustering keys
- Essential for tables > 1TB with selective queries
</details>

---

### Q23: Design a dbt project structure for a medium-sized analytics team. How would you organize models, tests, and documentation?
**Keywords:** Staging, Intermediate, Marts, ref(), Sources, Testing, Documentation
**Hint:** Think about the Medallion architecture and team collaboration.

<details>
<summary>Click to Reveal Answer</summary>

**Recommended dbt project structure:**

```
my_dbt_project/
├── dbt_project.yml
├── models/
│   ├── sources.yml                    # Source definitions (Bronze)
│   ├── staging/                       # Silver layer
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── _staging__models.yml       # Tests & docs
│   ├── intermediate/                  # Optional transformations
│   │   └── int_orders_enriched.sql
│   └── marts/                         # Gold layer (by domain)
│       ├── finance/
│       │   ├── fct_revenue.sql
│       │   └── _finance__models.yml
│       └── marketing/
│           ├── dim_customers.sql
│           └── _marketing__models.yml
├── macros/
│   └── cents_to_dollars.sql
├── tests/
│   └── assert_revenue_positive.sql    # Singular tests
├── seeds/
│   └── country_codes.csv
└── analysis/
    └── ad_hoc_queries.sql
```

**Key conventions:**
- **sources.yml:** Define Bronze layer tables
- **stg_** prefix: Staging models (Silver)
- **int_** prefix: Intermediate models
- **fct_/dim_** prefix: Marts (Gold)
- **_*__models.yml:** Tests and documentation per folder
- **Materializations:** views for staging, tables for marts, incremental for large facts
</details>

---

### Q24: How would you troubleshoot a Snowflake Stream that has become "stale"?
**Keywords:** Time Travel, Retention, Offset, Consumed, Staleness
**Hint:** Think about the relationship between streams and Time Travel.

<details>
<summary>Click to Reveal Answer</summary>

**Stream staleness** occurs when the stream's offset points to data older than the table's Time Travel retention period.

**Diagnosis:**
```sql
-- Check stream status
SHOW STREAMS LIKE 'orders_stream';

-- Check staleness
SELECT SYSTEM$STREAM_GET_TABLE_TIMESTAMP('orders_stream');
```

**Causes:**
1. Stream not consumed within retention period (default 1-90 days)
2. Time Travel period reduced on source table
3. Infrequent task execution or suspended tasks

**Resolution:**
1. **If data is recoverable:** Recreate stream at current offset
```sql
DROP STREAM orders_stream;
CREATE STREAM orders_stream ON TABLE orders;
```
Note: This loses uncommitted changes.

2. **Use Time Travel for recovery:**
```sql
CREATE STREAM orders_stream ON TABLE orders
AT (TIMESTAMP => 'YYYY-MM-DD HH:MI:SS'::TIMESTAMP);
```

**Prevention:**
- Consume streams regularly (scheduled tasks)
- Set appropriate DATA_RETENTION_TIME_IN_DAYS on source tables
- Monitor streams for staleness
- Ensure tasks are resumed and running
</details>

---

### Q25: Design an analytics architecture that uses Snowflake for storage/compute and dbt for transformations. How would you orchestrate this with Airflow?
**Keywords:** ELT, Orchestration, dbt run, dbt test, Dependencies, Monitoring
**Hint:** Think about the complete flow from extraction to consumption.

<details>
<summary>Click to Reveal Answer</summary>

**End-to-end architecture:**

```
[Extraction]     [Loading]       [Transformation]      [BI/Analytics]
Fivetran/API --> Snowflake   --> dbt (via Airflow) --> Looker/Tableau
                   (Bronze)       (Silver/Gold)
```

**Airflow DAG structure:**
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="elt_pipeline",
    schedule="0 6 * * *",
    default_args=default_args
) as dag:
    
    # 1. Extract and load (example: COPY INTO)
    load_data = SnowflakeOperator(
        task_id="load_bronze",
        sql="COPY INTO bronze.raw_orders FROM @landing_stage..."
    )
    
    # 2. dbt transformations
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && dbt run --profiles-dir ."
    )
    
    # 3. dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && dbt test --profiles-dir ."
    )
    
    # 4. Refresh BI dashboards
    refresh_bi = PythonOperator(
        task_id="refresh_dashboards",
        python_callable=trigger_looker_refresh
    )
    
    load_data >> dbt_run >> dbt_test >> refresh_bi
```

**Key considerations:**
- Separate dbt run and dbt test for clear failure identification
- Use Airflow sensors if depending on file arrivals
- Implement SLAs and alerting
- Store dbt artifacts for debugging
- Consider dbt Cloud for managed scheduling as alternative
</details>

---

## Study Tips

1. **Know the Snowflake architecture:** The three-layer model is a common interview topic
2. **Understand Medallion architecture:** Be able to explain Bronze, Silver, Gold and their purposes
3. **Practice dimensional modeling:** Star schema, facts vs dimensions, SCD patterns
4. **Master dbt concepts:** ref(), source(), materializations, incremental models
5. **Demonstrate end-to-end thinking:** Connect extraction, loading, transformation, and consumption

---

*Generated by Quality Assurance Agent based on Snowflake Week 5 curriculum content.*
