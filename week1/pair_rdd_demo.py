from pyspark import SparkContext

sc=SparkContext ("local[*]", "Pair Rdd Example")
fruits = ["apple", "banana", "mango", "peaches"]

fruits_rdd = sc.parallelize(fruits)

pair_rdd=fruits_rdd.map(lambda fruits:(fruits,1))
print(pair_rdd.collect())