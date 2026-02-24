# Exercise: Your First PySpark Job

## Overview
Write and run your first complete PySpark application in local mode. You will create a SparkSession, perform basic data operations, and see results.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Prerequisites
- Completed `exercise_spark_installation.md`
- Spark and PySpark installed and working
- A text editor or IDE

---

## Core Tasks

### Task 1: Create Your First PySpark Script

Create a file named `my_first_job.py` with the following structure:

```python
from pyspark.sql import SparkSession

def main():
    # Step 1: Create SparkSession
    spark = SparkSession.builder \
        .appName("MyFirstJob") \
        .master("local[*]") \
        .getOrCreate()
    
    # Step 2: YOUR CODE HERE - Create some data
    
    # Step 3: YOUR CODE HERE - Perform transformations
    
    # Step 4: YOUR CODE HERE - Show results
    
    # Step 5: Clean up
    spark.stop()

if __name__ == "__main__":
    main()
```

### Task 2: Work with a Simple Dataset

In your script, create a list of sales data and convert it to a DataFrame:

```python
# Sample data: (product, category, price, quantity)
sales_data = [
    ("Laptop", "Electronics", 999.99, 5),
    ("Mouse", "Electronics", 29.99, 50),
    ("Desk", "Furniture", 199.99, 10),
    ("Chair", "Furniture", 149.99, 20),
    ("Monitor", "Electronics", 299.99, 15),
]

# Create DataFrame with column names
df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])
```

### Task 3: Implement These Operations

Add code to perform the following and print results:

1. **Show the DataFrame:**
   ```python
   df.show()
   ```

2. **Count total records:**
   Print the total number of products

3. **Calculate total revenue per product:**
   Add a column `revenue = price * quantity`

4. **Filter by category:**
   Show only Electronics products

5. **Aggregate by category:**
   Calculate total revenue per category

### Task 4: Run Your Job

Execute your script:

```bash
python my_first_job.py
```

Or using spark-submit:

```bash
spark-submit my_first_job.py
```

### Task 5: Experiment with Local Mode Settings

Modify the master setting and observe differences:

```python
# Try these different configurations
.master("local")      # 1 thread
.master("local[2]")   # 2 threads
.master("local[*]")   # All cores
```

Run with each setting and compare execution behavior.

---

## Expected Output

Your script should produce output similar to:

```
+-------+-----------+------+--------+
|product|   category| price|quantity|
+-------+-----------+------+--------+
| Laptop|Electronics|999.99|       5|
|  Mouse|Electronics| 29.99|      50|
|   Desk|  Furniture|199.99|      10|
|  Chair|  Furniture|149.99|      20|
|Monitor|Electronics|299.99|      15|
+-------+-----------+------+--------+

Total products: 5

Revenue per product:
+-------+-----------+------+--------+--------+
|product|   category| price|quantity| revenue|
+-------+-----------+------+--------+--------+
| Laptop|Electronics|999.99|       5| 4999.95|
...

Electronics only:
+-------+-----------+------+--------+
|product|   category| price|quantity|
+-------+-----------+------+--------+
| Laptop|Electronics|999.99|       5|
|  Mouse|Electronics| 29.99|      50|
|Monitor|Electronics|299.99|      15|
+-------+-----------+------+--------+

Revenue by category:
+-----------+-------------+
|   category|total_revenue|
+-----------+-------------+
|Electronics|       ...   |
|  Furniture|       ...   |
+-----------+-------------+
```

---

## Deliverables

1. `my_first_job.py` - Your complete script
2. `output.txt` - Save the console output from running your job

---

## Definition of Done

- [ ] Script creates SparkSession with appropriate configuration
- [ ] DataFrame is created from sample data
- [ ] All 5 operations are implemented and produce correct output
- [ ] Script runs without errors using `python` or `spark-submit`
- [ ] Experimented with different local mode settings
- [ ] Both deliverable files are created

---

## Stretch Goals

1. Add a 6th operation: Find the product with highest revenue
2. Add error handling with try/except
3. Add command-line arguments for the master URL

---

## Additional Resources
- Written Content: `introduction-to-spark-and-pyspark.md`
- Written Content: `local-vs-cluster-mode.md`
- Demo: `demo_spark_ecosystem.py`
