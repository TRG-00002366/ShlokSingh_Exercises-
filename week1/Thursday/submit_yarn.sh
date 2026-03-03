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