# Pair Programming Exercise: dbt Project Build

## Overview
**Day:** 4-Thursday  
**Duration:** 4-5 hours  
**Mode:** Collaborative (Pair Programming)  
**Prerequisites:** Wednesday exercises completed, dbt installed

---

## Required Reading

Before starting this exercise, review the following content:

| Topic | File | Focus Areas |
|-------|------|-------------|
| dbt Introduction | [dbt-introduction.md](../../content/4-Thursday/dbt-introduction.md) | Analytics engineering workflow |
| dbt Sources and Staging | [dbt-sources-staging.md](../../content/4-Thursday/dbt-sources-staging.md) | source(), ref(), staging patterns |
| dbt Project Structure | [dbt-project-structure.md](../../content/4-Thursday/dbt-project-structure.md) | Project organization, YAML configs |

---

## Learning Objectives
By the end of this exercise, you will be able to:
1. Initialize and configure a dbt project for Snowflake
2. Define sources and build staging models
3. Create intermediate and mart models using ref()
4. Implement schema tests for data quality
5. Practice pair programming with Driver/Navigator roles

---

## Pair Programming Protocol

### Role Definitions

**Driver:**
- Has hands on the keyboard
- Writes the code
- Explains thought process out loud

**Navigator:**
- Reviews code as it's written
- References documentation
- Suggests improvements and catches errors
- Thinks about edge cases and testing

### Rotation Schedule

| Phase | Duration | Driver Task |
|-------|----------|-------------|
| 1 | 45 mins | Project setup + Sources |
| 2 | 45 mins | Staging models |
| 3 | 45 mins | Intermediate model |
| 4 | 45 mins | Mart model + Tests |
| 5 | 30 mins | Documentation + Review |

**Switch roles after each phase!**

---

## The Scenario

Your team needs to build a dbt project that transforms raw event data from the Bronze layer into business-ready analytics in the Gold layer. The data flow is:

```
BRONZE.RAW_EVENTS (source)
        |
        v
   stg_events (staging)
        |
        v
  int_events_daily (intermediate)
        |
        v
  fct_event_metrics (mart)
```

---

## Phase 1: Project Setup and Sources (45 mins)

**Driver Task:** Initialize the dbt project

### Step 1.1: Create Project Structure

```bash
# Create project directory
mkdir dbt_training
cd dbt_training

# Initialize dbt project (or copy from sample_data/dbt_skeleton)
dbt init snowflake_analytics
```

### Step 1.2: Configure profiles.yml

Create `~/.dbt/profiles.yml`:

```yaml
snowflake_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <YOUR_ACCOUNT>
      user: <YOUR_USERNAME>
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      database: <YOUR_NAME>_DEV_DB
      schema: DBT_<PAIR_NAMES>  # e.g., DBT_JOHN_JANE
      warehouse: COMPUTE_WH
      role: ACCOUNTADMIN
      threads: 4
```

### Step 1.3: Test Connection

```bash
export SNOWFLAKE_PASSWORD="your_password"
dbt debug
```

### Step 1.4: Define Sources

Create `models/staging/sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    database: <YOUR_NAME>_DEV_DB
    schema: BRONZE
    tables:
      - name: raw_events
        description: "Raw event data from streaming pipeline"
        columns:
          - name: event_id
            description: "Unique event identifier"
          - name: event_type
            description: "Type of event"
          - name: payload
            description: "JSON event data"
          - name: created_at
            description: "Event timestamp"
```

### Step 1.5: Verify Source

```bash
dbt source freshness  # May not work without timestamp config
dbt compile
```

**Checkpoint:** Switch roles! Navigator becomes Driver.

---

## Phase 2: Staging Model (45 mins)

**Driver Task:** Create the staging model

### Step 2.1: Create Staging Model

Create `models/staging/stg_events.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_events') }}
),

transformed AS (
    SELECT
        event_id,
        UPPER(event_type) AS event_type,
        payload:user::STRING AS user_id,
        payload:page::STRING AS page_url,
        payload:product::STRING AS product_id,
        payload:amount::DECIMAL(10,2) AS amount,
        created_at AS event_timestamp,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
    WHERE event_id IS NOT NULL
)

SELECT * FROM transformed
```

### Step 2.2: Run and Verify

```bash
dbt run --select stg_events
```

In Snowflake, verify the view was created:

```sql
SELECT * FROM DBT_<PAIR_NAMES>.STG_EVENTS LIMIT 10;
```

### Step 2.3: Add Staging Schema Tests

Create `models/staging/staging.yml`:

```yaml
version: 2

models:
  - name: stg_events
    description: "Staged events with extracted fields"
    columns:
      - name: event_id
        tests:
          - unique
          - not_null
      - name: event_type
        tests:
          - not_null
          - accepted_values:
              values: ['CLICK', 'VIEW', 'PURCHASE', 'SEARCH', 'CART_UPDATE', 'ORDER', 'LOGOUT']
```

```bash
dbt test --select stg_events
```

**Checkpoint:** Switch roles!

---

## Phase 3: Intermediate Model (45 mins)

**Driver Task:** Create an intermediate aggregation

### Step 3.1: Create Intermediate Model

Create `models/intermediate/int_events_daily.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

WITH staged_events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

daily_aggregates AS (
    SELECT
        DATE_TRUNC('day', event_timestamp)::DATE AS event_date,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        SUM(COALESCE(amount, 0)) AS total_amount,
        MIN(event_timestamp) AS first_event,
        MAX(event_timestamp) AS last_event
    FROM staged_events
    GROUP BY 1, 2
)

SELECT * FROM daily_aggregates
```

### Step 3.2: Run and Verify

```bash
dbt run --select int_events_daily
```

### Step 3.3: Update dbt_project.yml

```yaml
models:
  snowflake_analytics:
    staging:
      +materialized: view
    intermediate:
      +materialized: view
    marts:
      +materialized: table
```

**Checkpoint:** Switch roles!

---

## Phase 4: Mart Model and Tests (45 mins)

**Driver Task:** Create the final mart

### Step 4.1: Create Mart Model

Create `models/marts/fct_event_metrics.sql`:

```sql
{{
    config(
        materialized='table'
    )
}}

WITH daily_events AS (
    SELECT * FROM {{ ref('int_events_daily') }}
),

enriched AS (
    SELECT
        event_date,
        event_type,
        event_count,
        unique_users,
        total_amount,
        first_event,
        last_event,
        -- Derived metrics
        ROUND(total_amount / NULLIF(event_count, 0), 2) AS avg_event_value,
        -- Running totals (optional)
        SUM(event_count) OVER (
            PARTITION BY event_type 
            ORDER BY event_date 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_events
    FROM daily_events
)

SELECT 
    *,
    CURRENT_TIMESTAMP() AS _refreshed_at
FROM enriched
```

### Step 4.2: Run Full Pipeline

```bash
dbt run
```

### Step 4.3: Add Mart Tests

Create `models/marts/marts.yml`:

```yaml
version: 2

models:
  - name: fct_event_metrics
    description: "Daily event metrics by type"
    columns:
      - name: event_date
        tests:
          - not_null
      - name: event_type
        tests:
          - not_null
      - name: event_count
        tests:
          - not_null
```

```bash
dbt test
```

**Checkpoint:** Switch roles!

---

## Phase 5: Documentation and Review (30 mins)

**Driver Task:** Generate documentation

### Step 5.1: Add Descriptions

Update your YAML files with detailed descriptions for all models and columns.

### Step 5.2: Generate Docs

```bash
dbt docs generate
dbt docs serve --port 8081
```

### Step 5.3: Review the Lineage Graph

- Open http://localhost:8081
- Click the lineage graph icon
- Verify the DAG shows: source -> staging -> intermediate -> marts

### Step 5.4: Pair Review

Together, review:
1. Does the DAG look correct?
2. Are all tests passing?
3. Is the documentation complete?
4. What would you improve?

---

## Deliverables

As a pair, submit:

1. **dbt Project:** Complete project folder (zipped or repo link)
2. **Lineage Screenshot:** Screenshot of the dbt docs lineage graph
3. **Test Results:** Output of `dbt test`
4. **Pair Reflection:** Brief document answering:
   - What worked well in pair programming?
   - What was challenging?
   - What would you do differently?

---

## Definition of Done

- [ ] dbt project initialized and connected
- [ ] Sources defined correctly
- [ ] Staging model created and tested
- [ ] Intermediate model created
- [ ] Mart model created and tested
- [ ] All tests passing
- [ ] Documentation generated
- [ ] Lineage graph verified
- [ ] Both partners drove at least twice
- [ ] Pair reflection completed

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| dbt debug fails | Check account format, password env var |
| Source not found | Verify database/schema names in sources.yml |
| Model not found | Ensure file is in models/ directory with .sql extension |
| ref() error | Check model name matches filename (without .sql) |
| Tests fail | Run the generated SQL in Snowflake to debug |
