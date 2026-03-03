from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SlowJob") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Generate sample data (simulating large dataset)
data = [(f"product_{i % 100}", f"category_{i % 10}", i * 1.5, i % 50) 
        for i in range(100000)]

rdd = sc.parallelize(data)

# Problematic Pattern 1: Using groupByKey
grouped = rdd.map(lambda x: (x[1], x[2])) \
             .groupByKey() \
             .mapValues(lambda values: sum(values))

# Problematic Pattern 2: Collecting large data
all_data = rdd.collect()

# Problematic Pattern 3: Multiple actions on same RDD
count1 = rdd.count()
count2 = rdd.filter(lambda x: x[3] > 25).count()
count3 = rdd.map(lambda x: x[2]).reduce(lambda a, b: a + b)

print(f"Grouped: {grouped.take(5)}")
print(f"Counts: {count1}, {count2}, {count3}")

spark.stop()

# Issue: groupByKey() 
# Why it is bad: shuffles all values for each key across the network, can blow memory and is slower. You only need sums, so aggregate during shuffle.
# Issue: rdd.collect()
# Why it is bad: pulls the entire dataset to the driver, which can crash driver memory and adds big overhead.
# Issue: Multiple actions on same RDD without caching (count, filter().count, map().reduce)
# Why it is bad: Spark recomputes the whole lineage each time, so you pay the full cost 3 times.