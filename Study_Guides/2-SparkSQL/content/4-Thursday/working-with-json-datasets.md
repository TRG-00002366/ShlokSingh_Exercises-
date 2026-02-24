# Working with JSON Datasets

## Learning Objectives
- Master reading and writing JSON data in Spark
- Understand schema inference and explicit schema definition for JSON
- Learn to handle nested and complex JSON structures
- Apply techniques for flattening and querying JSON data

## Why This Matters

JSON (JavaScript Object Notation) is ubiquitous in modern data systems:
- API responses and webhooks
- Application logs and event streams  
- Configuration files and metadata
- NoSQL database exports

Data engineers frequently need to ingest, transform, and analyze JSON data. Understanding how Spark handles JSON, especially nested structures, is essential for building robust data pipelines as part of our Weekly Epic.

## The Concept

### JSON Formats in Spark

Spark supports two JSON formats:

**1. JSON Lines (default)**: One JSON object per line
```json
{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
```

**2. Multi-line JSON**: Entire file is one JSON object or array
```json
[
  {"name": "Alice", "age": 30},
  {"name": "Bob", "age": 25}
]
```

### Schema Handling

| Approach | Pros | Cons |
|----------|------|------|
| Infer Schema | Easy, automatic | Slow for large files, inconsistent |
| Explicit Schema | Fast, predictable | Requires upfront knowledge |
| Sampling | Balance of both | May miss fields in sample |

### Nested Data Structures

JSON often contains nested objects and arrays:

```json
{
  "user": {
    "name": "Alice",
    "address": {
      "city": "NYC",
      "zip": "10001"
    }
  },
  "orders": [
    {"id": 1, "amount": 100},
    {"id": 2, "amount": 200}
  ]
}
```

Spark represents this as:
- **StructType** for nested objects
- **ArrayType** for arrays

## Code Example

### Reading JSON Files

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("JSON Reading").getOrCreate()

# Read JSON Lines (one object per line) - default
df = spark.read.json("users.json")
df.printSchema()
df.show()

# Read multi-line JSON
df_multi = spark.read.option("multiLine", "true").json("config.json")

# Read with explicit schema (recommended for production)
user_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True)
])

df_typed = spark.read.schema(user_schema).json("users.json")
df_typed.printSchema()

# Useful read options
df_options = spark.read \
    .option("multiLine", "true") \
    .option("allowComments", "true") \
    .option("allowUnquotedFieldNames", "true") \
    .option("mode", "PERMISSIVE") \
    .json("relaxed.json")

# Mode options:
# PERMISSIVE (default): Sets malformed fields to null
# DROPMALFORMED: Drops malformed records
# FAILFAST: Throws exception on malformed records
```

### Working with Nested JSON

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Nested JSON").getOrCreate()

# Sample nested data
nested_data = """
{"id": 1, "user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}}
{"id": 2, "user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}}
"""

# Create temporary file and read
import tempfile
import os

with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
    f.write(nested_data)
    temp_path = f.name

df = spark.read.json(temp_path)

print("Nested Schema:")
df.printSchema()
# root
#  |-- id: long
#  |-- user: struct
#  |    |-- address: struct
#  |    |    |-- city: string
#  |    |    |-- zip: string
#  |    |-- name: string

# Access nested fields with dot notation
print("Accessing nested fields:")
df.select(
    col("id"),
    col("user.name").alias("name"),
    col("user.address.city").alias("city"),
    col("user.address.zip").alias("zip")
).show()

# Clean up
os.unlink(temp_path)
```

### Flattening Nested Structures

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, explode_outer

spark = SparkSession.builder.appName("Flatten JSON").getOrCreate()

# Data with arrays
data = [
    (1, "Alice", [{"product": "Laptop", "price": 1000}, {"product": "Phone", "price": 500}]),
    (2, "Bob", [{"product": "Tablet", "price": 300}]),
    (3, "Charlie", None)  # No orders
]

schema = "id INT, name STRING, orders ARRAY<STRUCT<product: STRING, price: INT>>"
df = spark.createDataFrame(data, schema)

print("Original (with array):")
df.show(truncate=False)
df.printSchema()

# Explode array into rows
print("Exploded (one row per order):")
df_exploded = df.select(
    col("id"),
    col("name"),
    explode("orders").alias("order")
)
df_exploded.show(truncate=False)

# Access struct fields after explode
print("Flattened:")
df_flat = df.select(
    col("id"),
    col("name"),
    explode("orders").alias("order")
).select(
    col("id"),
    col("name"),
    col("order.product"),
    col("order.price")
)
df_flat.show()

# explode_outer keeps rows with null arrays
print("Explode outer (keeps nulls):")
df.select(
    col("id"),
    col("name"),
    explode_outer("orders").alias("order")
).show()
```

### Complex JSON Processing

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json, to_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

spark = SparkSession.builder.appName("Complex JSON").getOrCreate()

# JSON as string column (common in streaming/logs)
raw_data = [
    (1, '{"event": "click", "data": {"page": "home", "time": 100}}'),
    (2, '{"event": "purchase", "data": {"page": "cart", "time": 200}}')
]

df = spark.createDataFrame(raw_data, ["id", "json_string"])

# Parse JSON string with from_json
event_schema = StructType([
    StructField("event", StringType(), True),
    StructField("data", StructType([
        StructField("page", StringType(), True),
        StructField("time", IntegerType(), True)
    ]), True)
])

df_parsed = df.withColumn("parsed", from_json(col("json_string"), event_schema))
print("Parsed JSON:")
df_parsed.printSchema()
df_parsed.show(truncate=False)

# Access parsed fields
df_final = df_parsed.select(
    col("id"),
    col("parsed.event"),
    col("parsed.data.page"),
    col("parsed.data.time")
)
df_final.show()

# Extract single field with get_json_object (simpler but slower)
df.select(
    col("id"),
    get_json_object(col("json_string"), "$.event").alias("event"),
    get_json_object(col("json_string"), "$.data.page").alias("page")
).show()
```

### Writing JSON

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Write JSON").getOrCreate()

data = [("Alice", 30), ("Bob", 25)]
df = spark.createDataFrame(data, ["name", "age"])

# Write as JSON Lines (default)
df.write.mode("overwrite").json("output/users_json")

# Write with compression
df.write \
    .option("compression", "gzip") \
    .mode("overwrite") \
    .json("output/users_compressed")

# Convert to single JSON column
from pyspark.sql.functions import to_json, struct

df_json_col = df.withColumn("json_data", to_json(struct("name", "age")))
df_json_col.show(truncate=False)
```

## Summary

- Spark reads **JSON Lines** (one object per line) by default; use `multiLine=true` for other formats
- **Nested objects** become StructType; access with dot notation (`col("user.name")`)
- **Arrays** become ArrayType; use `explode()` to flatten into rows
- Use **explicit schemas** in production for performance and consistency
- **from_json()** parses JSON strings into structured columns
- **get_json_object()** extracts single fields from JSON strings (simpler but slower)
- **to_json()** converts structs back to JSON strings
- Set appropriate error handling mode: PERMISSIVE, DROPMALFORMED, or FAILFAST

## Additional Resources

- [Spark SQL JSON Data Source](https://spark.apache.org/docs/latest/sql-data-sources-json.html)
- [PySpark JSON Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#json-functions)
- [Handling Complex Data Types](https://docs.databricks.com/en/sql/language-manual/data-types/complex-types.html)
