from pyspark.sql import SparkSession
from pyspark import StorageLevel


def main():
    spark = (
        SparkSession.builder
        .appName("OptimizedJob")
        .getOrCreate()
    )

    sc = spark.sparkContext

    # sample data 
    data = [(f"product_{i % 100}", f"category_{i % 10}", i * 1.5, i % 50)
            for i in range(100000)]

    # Parallelize with explicit partitions 

    num_partitions = max(sc.defaultParallelism, 4)
    rdd = sc.parallelize(data, numSlices=num_partitions)

    rdd = rdd.persist(StorageLevel.MEMORY_ONLY)

    grouped = (
        rdd.map(lambda x: (x[1], x[2]))
           .reduceByKey(lambda a, b: a + b)
    )

    # FIX 2: remove collect()
    sample = rdd.take(5)

    # FIX 3: Multiple actions:
    count1 = rdd.count()
    count2 = rdd.filter(lambda x: x[3] > 25).count()
    count3 = rdd.map(lambda x: x[2]).sum()  # clearer than reduce

    print("Sample rows:", sample)
    print("Grouped (first 5):", grouped.take(5))
    print(f"Counts: {count1}, {count2}, {count3}")

    spark.stop()


if __name__ == "__main__":
    main()