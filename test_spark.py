from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("InstallationTest") \
    .master("local[*]") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Display the DataFrame
df.show()

# Print Spark version
print(f"Spark Version: {spark.version}")

# Stop the session
spark.stop()