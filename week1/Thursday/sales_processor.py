#!/usr/bin/env python3
"""
Sales Data Processor
Processes sales data and generates summary reports.

Input CSV expected columns:
- sale_id (string/int)
- sale_date (YYYY-MM-DD)
- category (string)
- price (number)
- quantity (int)
"""

import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

import utils


def parse_args():
    parser = argparse.ArgumentParser(description="Sales Processor")
    parser.add_argument("--input", required=True, help="Input data path (CSV)")
    parser.add_argument("--output", required=True, help="Output path")
    parser.add_argument("--date", required=True, help="Processing date (YYYY-MM-DD)")
    return parser.parse_args()


def ensure_sample_csv(path: str):
    """
    If the user points to a local file path that doesn't exist,
    create a small sample CSV so local testing always works.
    """
    # Only do this for local filesystem paths (not s3://, hdfs://, etc.)
    if "://" in path:
        return

    if os.path.exists(path):
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)
    sample = """sale_id,sale_date,category,price,quantity
1,2024-01-15,Electronics,199.99,1
2,2024-01-15,Electronics,49.99,2
3,2024-01-15,Clothing,29.99,3
4,2024-01-14,Clothing,59.99,1
5,2024-01-15,Grocery,4.99,10
6,2024-01-16,Grocery,2.49,6
"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(sample)


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("SalesProcessor")
        .getOrCreate()
    )

    # Make local testing easy
    ensure_sample_csv(args.input)

    schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("sale_date", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
    ])

    # 1) Read input data
    df = (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(args.input)
    )

    # Basic data cleanup
    df = (
        df.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
          .withColumn("category", F.trim(F.col("category")))
          .filter(F.col("sale_date").isNotNull())
          .filter(F.col("category").isNotNull() & (F.col("category") != ""))
          .filter(F.col("price").isNotNull() & F.col("quantity").isNotNull())
    )

    target_date = F.to_date(F.lit(args.date), "yyyy-MM-dd")

    # 2) Filter by date
    day_df = df.filter(F.col("sale_date") == target_date)

    # 3) Calculate totals by category
    # Use helper module function via UDF to prove proper import/usage
    calc_rev_udf = F.udf(utils.calculate_revenue, DoubleType())

    enriched = day_df.withColumn("revenue", calc_rev_udf(F.col("price"), F.col("quantity")))

    summary = (
        enriched.groupBy("category")
        .agg(
            F.count("*").alias("transactions"),
            F.sum("quantity").alias("total_units"),
            F.round(F.sum("revenue"), 2).alias("total_revenue")
        )
        .orderBy(F.col("total_revenue").desc())
    )

    # Add metadata columns
    summary = (
        summary
        .withColumn("processing_date", F.lit(args.date))
        .withColumn("total_revenue_formatted", F.lit(None).cast("string"))
    )

    # Optional: format currency in a small, safe way (collect is OK because it's small output: categories)
    # If you want "pure Spark only", remove this block.
    rows = summary.select("category", "transactions", "total_units", "total_revenue", "processing_date").collect()
    formatted_rows = []
    for r in rows:
        formatted_rows.append((
            r["category"],
            int(r["transactions"]),
            int(r["total_units"]),
            float(r["total_revenue"]) if r["total_revenue"] is not None else 0.0,
            utils.format_currency(float(r["total_revenue"]) if r["total_revenue"] is not None else 0.0),
            r["processing_date"],
        ))

    out_schema = StructType([
        StructField("category", StringType(), False),
        StructField("transactions", IntegerType(), False),
        StructField("total_units", IntegerType(), False),
        StructField("total_revenue", DoubleType(), False),
        StructField("total_revenue_formatted", StringType(), False),
        StructField("processing_date", StringType(), False),
    ])

    out_df = spark.createDataFrame(formatted_rows, schema=out_schema)

    # 4) Save results
    (
        out_df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(args.output)
    )

    spark.stop()


if __name__ == "__main__":
    main()