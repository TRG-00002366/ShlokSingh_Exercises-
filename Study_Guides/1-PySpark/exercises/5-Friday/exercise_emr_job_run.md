# Exercise: EMR Job Submission

## Overview
Submit a Spark job to your EMR cluster and monitor its execution.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Prerequisites

- Running EMR cluster (from previous exercise)
- AWS CLI configured
- S3 bucket for scripts and data

---

## Core Tasks

### Task 1: Prepare Your Spark Job

Create `emr_word_count.py`:

```python
from pyspark.sql import SparkSession
import sys

def main():
    if len(sys.argv) < 3:
        print("Usage: emr_word_count.py <input> <output>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    spark = SparkSession.builder \
        .appName("EMR-WordCount") \
        .getOrCreate()
    
    # Read text file
    text = spark.read.text(input_path)
    
    # Word count
    from pyspark.sql.functions import explode, split, lower
    words = text.select(explode(split(lower(text.value), "\\s+")).alias("word"))
    counts = words.groupBy("word").count().orderBy("count", ascending=False)
    
    # Save results
    counts.write.mode("overwrite").csv(output_path)
    
    print("Job completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
```

### Task 2: Upload to S3

```bash
# Create sample input
echo "Apache Spark is fast Spark is distributed" > sample.txt

# Upload script and data
aws s3 cp emr_word_count.py s3://your-bucket/scripts/
aws s3 cp sample.txt s3://your-bucket/data/input/
```

### Task 3: Submit Job to EMR

```bash
aws emr add-steps \
    --cluster-id j-XXXXX \
    --steps '[
        {
            "Type": "Spark",
            "Name": "WordCount",
            "ActionOnFailure": "CONTINUE",
            "Args": [
                "--deploy-mode", "cluster",
                "s3://your-bucket/scripts/emr_word_count.py",
                "s3://your-bucket/data/input/sample.txt",
                "s3://your-bucket/data/output/"
            ]
        }
    ]'
```

### Task 4: Monitor Execution

```bash
# Get step status
aws emr describe-step \
    --cluster-id j-XXXXX \
    --step-id s-XXXXX

# Watch until COMPLETED
```

### Task 5: Verify Results

```bash
# List output files
aws s3 ls s3://your-bucket/data/output/

# View results
aws s3 cp s3://your-bucket/data/output/part-00000-*.csv - | head -10
```

---

## Deliverables

1. `emr_word_count.py` - Your job script
2. Screenshot or log of successful step completion
3. Sample of output results

---

## Definition of Done

- [ ] Script uploaded to S3
- [ ] Step submitted successfully
- [ ] Step completed (COMPLETED status)
- [ ] Output verified in S3
- [ ] Cluster terminated when done

---

## Additional Resources
- Written Content: `running-spark-job-on-emr.md`
- Demo: `demo_emr_job_submission.py`
