# Pair Programming: Job Submission

## Overview
Work in pairs to package and submit a Spark application with proper configuration. Practice the complete workflow from code to submission.

**Duration:** 2 hours  
**Mode:** Pair Programming (Driver/Navigator)

---

## The Scenario

Your team needs to prepare a data processing job for "production" deployment. You will package the application, write submission scripts, and test different configurations.

---

## Core Tasks

### Phase 1: Create the Application (Driver: Person A)

**Task 1:** Create `sales_processor.py`

```python
#!/usr/bin/env python3
"""
Sales Data Processor
Processes sales data and generates summary reports.
"""

import argparse
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Sales Processor")
    parser.add_argument("--input", required=True, help="Input data path")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--date", required=True, help="Processing date")
    return parser.parse_args()

def main():
    args = parse_args()
    
    spark = SparkSession.builder \
        .appName("SalesProcessor") \
        .getOrCreate()
    
    # YOUR CODE: Implement data processing
    # 1. Read input data (create sample if needed)
    # 2. Filter by date
    # 3. Calculate totals by category
    # 4. Save results
    
    spark.stop()

if __name__ == "__main__":
    main()
```

**Task 2:** Create a helper module `utils.py`

```python
"""Utility functions for sales processing."""

def calculate_revenue(price, quantity):
    return price * quantity

def format_currency(amount):
    return f"${amount:,.2f}"
```

### Phase 2: Package the Application (Navigator guides, Driver implements)

**Task 3:** Create `requirements.txt`

```
pyspark>=3.5.0
```

**Task 4:** Create `package.sh` to bundle dependencies

```bash
#!/bin/bash
# Package the application for submission

# Create a zip of Python modules
zip -r app.zip sales_processor.py utils.py

echo "Package created: app.zip"
```

### Phase 3: Create Submission Scripts (Switch roles)

**Task 5:** Create `submit_local.sh`

```bash
#!/bin/bash
# Local mode submission for testing

spark-submit \
    --master local[*] \
    --driver-memory 2g \
    --py-files utils.py \
    sales_processor.py \
    --input ./data/sales.csv \
    --output ./output \
    --date 2024-01-15
```

**Task 6:** Create `submit_yarn.sh` (for production)

```bash
#!/bin/bash
# YARN cluster submission

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 8g \
    --executor-cores 4 \
    --num-executors 10 \
    --py-files utils.py \
    --conf spark.sql.shuffle.partitions=200 \
    sales_processor.py \
    --input s3://bucket/data/sales/ \
    --output s3://bucket/output/ \
    --date 2024-01-15
```

### Phase 4: Test and Document

**Task 7:** Test locally

Run `submit_local.sh` and verify output

**Task 8:** Create `README.md`

```markdown
# Sales Processor Application

## Description
[Describe what the application does]

## Files
- `sales_processor.py` - Main application
- `utils.py` - Helper functions
- `submit_local.sh` - Local testing
- `submit_yarn.sh` - Production submission

## Usage

### Local Testing
```bash
./submit_local.sh
```

### Production
```bash
./submit_yarn.sh
```

## Configuration Options
[Document key configurations]
```

---

## Deliverables

1. `sales_processor.py` - Main application
2. `utils.py` - Helper module
3. `submit_local.sh` - Local submission script
4. `submit_yarn.sh` - Production submission script
5. `README.md` - Documentation

---

## Definition of Done

- [ ] Application accepts command-line arguments
- [ ] Helper module is properly imported
- [ ] Local submission script runs successfully
- [ ] Production script has appropriate configuration
- [ ] README documents the application
- [ ] Both partners contributed

---

## Additional Resources
- Written Content: `spark-submit.md`
- Demo: `demo_spark_submit.sh`
