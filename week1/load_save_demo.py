from pyspark import SparkContext

sc = SparkContext("local[*]", "Save and Load")

file_rdd = sc.textFile("1661-0.txt")
print(file_rdd.take(3))

sherlock_rdd = file_rdd.filter(lambda line: "Sherlock" in line)
print(sherlock_rdd.take(4))

# If output already exists, delete it first or change the name

sherlock_rdd.saveAsTextFile("output")

sc.stop()