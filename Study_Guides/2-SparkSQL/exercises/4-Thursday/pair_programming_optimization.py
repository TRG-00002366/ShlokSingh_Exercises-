"""
Pair Programming: Pipeline Optimization Challenge
=================================================
Week 2, Thursday - Collaborative Exercise

ROLES: Switch Driver/Navigator every 20 minutes!

This pipeline is SLOW. Your job is to optimize it using:
- Caching
- Partitioning  
- Bucketing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, avg, count, year, month
import time

# =============================================================================
# SETUP
# =============================================================================

spark = SparkSession.builder \
    .appName("Pipeline Optimization") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "spark-warehouse") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Generate sample data (simulating a larger dataset)
print("Generating sample data...")

# Sales transactions
sales_data = []
for i in range(50000):
    sales_data.append((
        i,
        i % 1000,           # customer_id
        i % 50,             # product_id
        100 + (i % 900),    # amount
        f"2023-{1 + (i % 12):02d}-{1 + (i % 28):02d}"  # date
    ))

sales = spark.createDataFrame(sales_data, ["txn_id", "customer_id", "product_id", "amount", "date"])

# Customers
customer_data = [(i, f"Customer_{i}", ["NY", "CA", "TX", "FL", "WA"][i % 5]) for i in range(1000)]
customers = spark.createDataFrame(customer_data, ["customer_id", "name", "state"])

# Products
product_data = [(i, f"Product_{i}", ["Electronics", "Clothing", "Home", "Sports"][i % 4]) for i in range(50)]
products = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])

print(f"Sales: {sales.count()} rows")
print(f"Customers: {customers.count()} rows")
print(f"Products: {products.count()} rows")

# =============================================================================
# BASELINE PIPELINE (UNOPTIMIZED)
# 
# This pipeline generates multiple reports. It is INTENTIONALLY SLOW.
# Your job is to optimize it!
# =============================================================================

def run_baseline_pipeline():
    """The original, unoptimized pipeline."""
    print("\n" + "="*60)
    print("RUNNING BASELINE PIPELINE")
    print("="*60)
    
    start_time = time.time()
    
    # Report 1: Sales by Customer
    print("\nGenerating Report 1: Sales by Customer...")
    report1 = sales.join(customers, "customer_id") \
        .groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))
    report1.count()  # Force computation
    
    # Report 2: Sales by Product
    print("Generating Report 2: Sales by Product...")
    report2 = sales.join(products, "product_id") \
        .groupBy("product_id", "product_name", "category") \
        .agg(spark_sum("amount").alias("total_sales"))
    report2.count()
    
    # Report 3: Sales by State
    print("Generating Report 3: Sales by State...")
    report3 = sales.join(customers, "customer_id") \
        .groupBy("state") \
        .agg(
            spark_sum("amount").alias("total_sales"),
            count("*").alias("num_transactions")
        )
    report3.count()
    
    # Report 4: Top Customers per State
    print("Generating Report 4: Top Customers per State...")
    customer_totals = sales.join(customers, "customer_id") \
        .groupBy("customer_id", "name", "state") \
        .agg(spark_sum("amount").alias("total_spend"))
    
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number, desc
    
    window = Window.partitionBy("state").orderBy(desc("total_spend"))
    report4 = customer_totals.withColumn("rank", row_number().over(window)) \
        .filter(col("rank") <= 5)
    report4.count()
    
    # Report 5: Monthly Trend
    print("Generating Report 5: Monthly Trend...")
    report5 = sales.join(products, "product_id") \
        .groupBy("category", "date") \
        .agg(spark_sum("amount").alias("daily_sales"))
    report5.count()
    
    end_time = time.time()
    baseline_time = end_time - start_time
    
    print(f"\nBASELINE COMPLETED in {baseline_time:.2f} seconds")
    return baseline_time


# =============================================================================
# PHASE 1: ANALYZE THE BASELINE
# Driver: Partner A | Navigator: Partner B
# 
# TODO 1a: Run the baseline and record the time
# TODO 1b: Use .explain() to examine execution plans
# TODO 1c: Identify issues:
#   - How many times is sales.join(customers) computed?
#   - How many times is sales.join(products) computed?
#   - What is the partition count?
# =============================================================================

print("\n" + "="*60)
print("PHASE 1: ANALYZE THE BASELINE")
print("="*60)

baseline_time = run_baseline_pipeline()

# TODO: Analyze the execution plan
# sales.join(customers, "customer_id").explain()

# TODO: Check partition counts
# print(f"Sales partitions: {sales.rdd.getNumPartitions()}")


# =============================================================================
# PHASE 2: APPLY CACHING
# Driver: Partner B | Navigator: Partner A
#
# TODO 2a: Identify DataFrames used multiple times
# TODO 2b: Cache the joined DataFrames that are reused
# TODO 2c: Measure the improvement
# =============================================================================

print("\n" + "="*60)
print("PHASE 2: APPLY CACHING")
print("="*60)

def run_cached_pipeline():
    """Pipeline with caching optimization."""
    start_time = time.time()
    
    # TODO: Add caching for frequently used DataFrames
    # HINT: sales.join(customers) is used 3 times
    # HINT: sales.join(products) is used 2 times
    
    # Your optimized code here...
    
    end_time = time.time()
    return end_time - start_time

# cached_time = run_cached_pipeline()
# print(f"CACHED pipeline time: {cached_time:.2f}s")
# print(f"Improvement: {(baseline_time - cached_time) / baseline_time * 100:.1f}%")


# =============================================================================
# PHASE 3: OPTIMIZE PARTITIONING
# Driver: Partner A | Navigator: Partner B
#
# TODO 3a: Reduce shuffle partitions (200 is too many for this data size)
# TODO 3b: Use coalesce before write operations
# TODO 3c: Repartition by join key for better data locality
# =============================================================================

print("\n" + "="*60)
print("PHASE 3: OPTIMIZE PARTITIONING")
print("="*60)

def run_partitioned_pipeline():
    """Pipeline with partitioning optimization."""
    start_time = time.time()
    
    # TODO: Optimize partitioning
    # HINT: Set spark.conf.set("spark.sql.shuffle.partitions", "8")
    # HINT: Consider repartitioning sales by customer_id or product_id
    
    # Your optimized code here...
    
    end_time = time.time()
    return end_time - start_time


# =============================================================================
# PHASE 4: IMPLEMENT BUCKETING
# Driver: Partner B | Navigator: Partner A
#
# TODO 4a: Create bucketed versions of sales, customers, products
# TODO 4b: Use the same bucket count and column for tables that are joined
# TODO 4c: Verify with explain() that shuffle is eliminated
# =============================================================================

print("\n" + "="*60)
print("PHASE 4: IMPLEMENT BUCKETING")
print("="*60)

def setup_bucketed_tables():
    """Create bucketed tables for optimized joins."""
    
    # TODO: Save sales bucketed by customer_id
    # sales.write.bucketBy(16, "customer_id").sortBy("customer_id") \
    #     .mode("overwrite").saveAsTable("sales_bucketed")
    
    # TODO: Save customers bucketed by customer_id
    
    # TODO: Save products bucketed by product_id
    
    pass

def run_bucketed_pipeline():
    """Pipeline using bucketed tables."""
    start_time = time.time()
    
    # TODO: Read from bucketed tables and verify no shuffle in joins
    
    end_time = time.time()
    return end_time - start_time


# =============================================================================
# PHASE 5: FINAL OPTIMIZATION
# Both Partners Together
#
# Combine all optimizations into the final pipeline
# =============================================================================

print("\n" + "="*60)
print("PHASE 5: FINAL OPTIMIZED PIPELINE")
print("="*60)

def run_optimized_pipeline():
    """The fully optimized pipeline with all techniques applied."""
    
    # Optimize shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    
    start_time = time.time()
    
    # TODO: Combine caching, partitioning, and bucketing
    # Create the most efficient version of the pipeline
    
    # Your final optimized code here...
    
    end_time = time.time()
    return end_time - start_time


# =============================================================================
# PERFORMANCE COMPARISON
# =============================================================================

print("\n" + "="*60)
print("PERFORMANCE COMPARISON")
print("="*60)

# Uncomment after implementing optimizations:
# optimized_time = run_optimized_pipeline()
# improvement = (baseline_time - optimized_time) / baseline_time * 100

# print(f"Baseline time:  {baseline_time:.2f}s")
# print(f"Optimized time: {optimized_time:.2f}s")
# print(f"Improvement:    {improvement:.1f}%")


# =============================================================================
# CLEANUP
# =============================================================================

# Clean up bucketed tables
# spark.sql("DROP TABLE IF EXISTS sales_bucketed")
# spark.sql("DROP TABLE IF EXISTS customers_bucketed")
# spark.sql("DROP TABLE IF EXISTS products_bucketed")

spark.stop()
