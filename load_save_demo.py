from pyspark import SparkContext

sc = SparkContext("local[*]", "Save and Load")

file_rdd=sc.textFile("ShlokSingh_Exercises-/week1/")

print(file_rdd.take(3))

sherlock_rdd=file_rdd.filter(lambda line: "Sherlock" in line)
print(sherlock_rdd.take(4))

sherlock_rdd.saveAsTextFile("ShlokSingh_Exercises-/week1/output")

input("Press any key to continue...")

sc.stop()