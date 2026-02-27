from pyspark.sql import SparkSession

# Create SparkSession (recommended entry point)
spark = SparkSession.builder \
    .appName("HybridApp") \
    .getOrCreate()

# For DataFrame operations, use spark
df = spark.read.csv("sales_data2.csv", header=True)

# For RDD operations, access SparkContext
sc = spark.sparkContext
rdd = sc.parallelize([1, 2, 3])

# You can convert between RDD and DataFrame
df_from_rdd = rdd.map(lambda x: (x,)).toDF(["value"])
rdd_from_df = df.show()