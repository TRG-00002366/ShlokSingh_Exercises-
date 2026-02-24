# dbt Project Skeleton for Week 5 Training

This is a pre-configured dbt project that trainees can copy and use immediately.

## Quick Start

### 1. Copy the Skeleton
```bash
cp -r dbt_skeleton ~/snowflake_training
cd ~/snowflake_training
```

### 2. Set Up Python Environment
```bash
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install dbt-snowflake
```

### 3. Configure Credentials
```bash
# Copy the template to your dbt config directory
cp profiles.yml.template ~/.dbt/profiles.yml

# Edit ~/.dbt/profiles.yml and replace:
# - <YOUR_ACCOUNT_IDENTIFIER> with your Snowflake account
# - <YOUR_USERNAME> with your username
# - <YOUR_NAME> with your name (e.g., DBT_JOHN)

# Set password environment variable
export SNOWFLAKE_PASSWORD="your_password"
```

### 4. Test Connection
```bash
dbt debug
```

### 5. Run Models
```bash
# Run all models
dbt run

# Run with tests
dbt build

# Generate documentation
dbt docs generate
dbt docs serve
```

## Project Structure

```
snowflake_training/
|-- dbt_project.yml           # Project configuration
|-- profiles.yml.template     # Credentials template (copy to ~/.dbt/)
|-- models/
|   |-- staging/
|   |   |-- sources.yml       # Source definitions (bronze layer)
|   |   |-- staging.yml       # Model tests
|   |   |-- stg_events.sql    # Staging model
|   |-- marts/
|       |-- fct_event_counts.sql  # Mart model
```

## Prerequisites

Before running this project, ensure:
1. `DEV_DB.BRONZE.RAW_EVENTS` table exists (run week5_setup.sql)
2. Your Snowflake warehouse is running
3. You have CREATE privileges on your target schema
