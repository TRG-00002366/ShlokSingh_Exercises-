# Create SparkSession
spark = SparkSession.builder \
    .appName("ColumnsManagement") \
    .getOrCreate()

# Sample data
data = [
    (1, " John Doe ", 60000, "john@company.com"),
    (2, " Diana Prince ", 70000, "diana@company.com"),
    (3, " Bruce Wayne ", 90000, "bruce@company.com")
]

columns = ["id", "name", "salary", "email"]

df = spark.createDataFrame(data, columns)

# 1️⃣ Add High Earner column
df = df.withColumn("High Earner", col("salary") > 70000)

# 2️⃣ Add Salary Tier column (conditional logic)
df = df.withColumn(
    "Salary Tier",
    when(col("salary") <= 65000, "Entry")
    .when(col("salary") < 75000, "Mid")
    .otherwise("Senior")
)

# 3️⃣ Clean name column (trim spaces)
clean_df = df.withColumn("name", trim(col("name")))

# 4️⃣ Extract First Name
clean_df = clean_df.withColumn(
    "First_Name",
    split(col("name"), " ")[0]
)

# 5️⃣ Replace email domain
clean_df = clean_df.withColumn(
    "email",
    regexp_replace(col("email"), "@company.com", "@mycompany.com")
)

# Show result
clean_df.show()

# Stop Spark
spark.stop()