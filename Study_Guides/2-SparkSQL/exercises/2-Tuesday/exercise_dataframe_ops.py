"""
Exercise: DataFrame Operations
==============================
Week 2, Tuesday

Practice DataFrame creation, inspection, and basic operations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, upper

# =============================================================================
# SETUP - Do not modify
# =============================================================================

spark = SparkSession.builder.appName("Exercise: DataFrame Ops").master("local[*]").getOrCreate()

# Sample employee data
employees = [
    (1, "Alice", "Engineering", 75000, "2020-01-15"),
    (2, "Bob", "Marketing", 65000, "2019-06-01"),
    (3, "Charlie", "Engineering", 80000, "2021-03-20"),
    (4, "Diana", "Sales", 55000, "2018-11-10"),
    (5, "Eve", "Marketing", 70000, "2020-09-05"),
    (6, "Frank", "Engineering", 72000, "2022-01-10"),
    (7, "Grace", "Sales", 58000, "2021-07-15")
]

df = spark.createDataFrame(employees, ["id", "name", "department", "salary", "hire_date"])

print("=== Exercise: DataFrame Operations ===")
print("\nEmployee Data:")
df.show()

# =============================================================================
# TASK 1: Schema Inspection (10 mins)
# =============================================================================

print("\n--- Task 1: Schema Inspection ---")

# TODO 1a: Print the schema using printSchema()


# TODO 1b: Print just the column names


# TODO 1c: Print the data types as a list of tuples


# TODO 1d: Print the total row count and column count


# =============================================================================
# TASK 2: Column Selection (15 mins)
# =============================================================================

print("\n--- Task 2: Column Selection ---")

# TODO 2a: Select only name and salary columns


# TODO 2b: Select all columns EXCEPT id (dynamically, not hardcoding column names)


# TODO 2c: Use selectExpr to create a new column "monthly_salary" = salary / 12


# =============================================================================
# TASK 3: Adding and Modifying Columns (20 mins)
# =============================================================================

print("\n--- Task 3: Adding and Modifying Columns ---")

# TODO 3a: Add a column "country" with value "USA" for all rows


# TODO 3b: Add a column "salary_tier" based on salary:
#    - "Entry" if salary < 60000
#    - "Mid" if salary >= 60000 and < 75000
#    - "Senior" if salary >= 75000


# TODO 3c: Add a column "name_upper" with uppercase version of name


# TODO 3d: Modify salary to increase by 5% (replace the column)


# =============================================================================
# TASK 4: Filtering Rows (20 mins)
# =============================================================================

print("\n--- Task 4: Filtering Rows ---")

# TODO 4a: Filter employees with salary > 70000


# TODO 4b: Filter employees in Engineering department


# TODO 4c: Filter employees in Engineering OR Marketing


# TODO 4d: Filter employees hired after 2020-01-01 with salary > 60000
# HINT: You can compare date strings directly


# =============================================================================
# TASK 5: Sorting (10 mins)
# =============================================================================

print("\n--- Task 5: Sorting ---")

# TODO 5a: Sort by salary ascending


# TODO 5b: Sort by department ascending, then salary descending


# =============================================================================
# TASK 6: Combining Operations (15 mins)
# =============================================================================

print("\n--- Task 6: Complete Pipeline ---")

# TODO 6: Create a complete pipeline that:
# 1. Filters to employees hired after 2020-01-01
# 2. Adds a 10% bonus column
# 3. Selects only name, department, salary, and bonus
# 4. Sorts by bonus descending
# 5. Shows the result

result = None  # Your pipeline here

# result.show()


# =============================================================================
# CLEANUP
# =============================================================================

spark.stop()
