"""
Pair Programming: JSON Analysis Challenge
==========================================
Week 2, Thursday - Collaborative Exercise

ROLES: Switch Driver/Navigator every 20 minutes!

Load, flatten, and analyze complex nested JSON data.
Apply filtering, aggregations, and SQL queries.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import tempfile
import os

# =============================================================================
# SETUP
# =============================================================================

spark = SparkSession.builder \
    .appName("JSON Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Create sample nested JSON data (simulating a real-world API response)
temp_dir = tempfile.mkdtemp()

# Complex nested JSON with orders, products, and customer details
json_data = """{"order_id": 1, "customer": {"id": 101, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "items": [{"product": "Laptop", "qty": 1, "price": 1200}, {"product": "Mouse", "qty": 2, "price": 25}], "date": "2023-01-15"}
{"order_id": 2, "customer": {"id": 102, "name": "Bob", "address": {"city": "LA", "zip": "90001"}}, "items": [{"product": "Phone", "qty": 1, "price": 800}], "date": "2023-01-16"}
{"order_id": 3, "customer": {"id": 101, "name": "Alice", "address": {"city": "NYC", "zip": "10001"}}, "items": [{"product": "Keyboard", "qty": 1, "price": 100}, {"product": "Monitor", "qty": 1, "price": 300}], "date": "2023-01-17"}
{"order_id": 4, "customer": {"id": 103, "name": "Charlie", "address": {"city": "Chicago", "zip": "60601"}}, "items": [{"product": "Tablet", "qty": 2, "price": 500}], "date": "2023-01-18"}"""

json_path = os.path.join(temp_dir, "orders.json")
with open(json_path, "w") as f:
    f.write(json_data)

print("=== Pair Programming: JSON Analysis ===")
print(f"\nLoading JSON from: {json_path}")

# =============================================================================
# PHASE 1: LOAD AND EXPLORE
# Driver: Partner A | Navigator: Partner B
# Time: 30 minutes
# =============================================================================

print("\n" + "="*60)
print("PHASE 1: LOAD AND EXPLORE")
print("="*60)

# TODO 1a: Load the JSON file
orders = None  # spark.read.json(json_path)


# TODO 1b: Print the schema to understand the structure


# TODO 1c: Show the data


# TODO 1d: Answer these questions in comments:
# Q1: How many levels of nesting are there?
# Q2: What is the data type of the "items" field?
# Q3: How would you access the customer's city?


# =============================================================================
# PHASE 2: FLATTEN NESTED STRUCTURES
# Driver: Partner B | Navigator: Partner A
# Time: 30 minutes
# =============================================================================

print("\n" + "="*60)
print("PHASE 2: FLATTEN NESTED STRUCTURES")
print("="*60)

# TODO 2a: Extract customer information into flat columns
# Create: customer_id, customer_name, customer_city, customer_zip
flat_orders = None


# TODO 2b: Explode the items array into separate rows
# Each order-item combination should be a separate row
exploded_orders = None


# TODO 2c: Create a fully flat DataFrame with columns:
# order_id, customer_id, customer_name, city, zip, date,
# product, qty, price
fully_flat = None


# TODO 2d: Add a calculated column: line_total = qty * price


# =============================================================================
# PHASE 3: AGGREGATIONS
# Driver: Partner A | Navigator: Partner B
# Time: 30 minutes
# =============================================================================

print("\n" + "="*60)
print("PHASE 3: AGGREGATIONS")
print("="*60)

# TODO 3a: Calculate order totals (sum of line_totals per order)


# TODO 3b: Find the most popular products (by total quantity sold)


# TODO 3c: Find total spending per customer


# TODO 3d: Find the city with the highest total order value


# =============================================================================
# PHASE 4: SQL QUERIES
# Driver: Partner B | Navigator: Partner A
# Time: 30 minutes
# =============================================================================

print("\n" + "="*60)
print("PHASE 4: SQL QUERIES")
print("="*60)

# TODO 4a: Register the flat DataFrame as a temp view
# fully_flat.createOrReplaceTempView("orders_flat")


# TODO 4b: Write a SQL query to find customers who spent more than $1000 total


# TODO 4c: Write a SQL query with a window function to rank products by revenue


# TODO 4d: Write a SQL query to find the average order value by city


# =============================================================================
# PHASE 5: ADVANCED TRANSFORMATIONS
# Both Partners Together
# Time: 30 minutes
# =============================================================================

print("\n" + "="*60)
print("PHASE 5: ADVANCED TRANSFORMATIONS")
print("="*60)

# TODO 5a: Convert the flat DataFrame back to a nested JSON structure
# Result should have: order_id, customer (nested), total_amount


# TODO 5b: Write the result as a JSON file


# TODO 5c: Create a summary report DataFrame with:
# - Total orders
# - Total revenue
# - Unique customers
# - Top product by quantity
# - Average order value


# =============================================================================
# CHALLENGE: Error Handling
# Both Partners
# =============================================================================

print("\n" + "="*60)
print("CHALLENGE: Error Handling")
print("="*60)

# Malformed JSON data
malformed_json = """{"order_id": 5, "customer": {"id": 104, "name": "Diana"}, "items": [{"product": "Book", "qty": 1, "price": 20}]}
{"order_id": "not_a_number", "customer": {"id": 105}, "items": []}
{"order_id": 6, "bad_field": true}"""

malformed_path = os.path.join(temp_dir, "malformed.json")
with open(malformed_path, "w") as f:
    f.write(malformed_json)

# TODO 6a: Read with PERMISSIVE mode and show results


# TODO 6b: Read with DROPMALFORMED mode and show results


# TODO 6c: Discuss: When would you use each mode?
# Answer in comments:


# =============================================================================
# DELIVERABLES
# =============================================================================

"""
ANALYSIS REPORT
===============
Complete this section with your findings:

1. Data Structure:
   - Number of orders: ____
   - Number of unique customers: ____
   - Number of unique products: ____

2. Key Insights:
   - Top customer by spending: ____
   - Most popular product: ____
   - City with highest revenue: ____

3. SQL Queries Written:
   - [list your queries]

4. Challenges Encountered:
   - [describe any issues]

5. Partner Contributions:
   - Partner A: [describe]
   - Partner B: [describe]
"""


# =============================================================================
# CLEANUP
# =============================================================================

import shutil
shutil.rmtree(temp_dir)

spark.stop()
