from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "PairRDDs")

    try:

        # Task 1: Classic Word Count

        text = sc.parallelize([
            "Apache Spark is a fast and general engine",
            "Spark provides APIs in Python Java and Scala",
            "Spark is used for big data processing",
            "PySpark is the Python API for Spark"
        ])

        words = text.flatMap(lambda line: line.split())

        pairs = words.map(lambda w: (w.lower(), 1))  
        counts = pairs.reduceByKey(lambda a, b: a + b)

        word_counts = counts.sortBy(lambda kv: kv[1], ascending=False)

        print("Word Counts (top 10):")
        for word, count in word_counts.take(10):
            print(f"  {word}: {count}")

        # Task 2: Join Operations
        products = sc.parallelize([
            ("P001", "Laptop"),
            ("P002", "Mouse"),
            ("P003", "Keyboard"),
            ("P004", "Monitor")
        ])

        prices = sc.parallelize([
            ("P001", 999),
            ("P002", 29),
            ("P003", 79),
            ("P005", 199)  
        ])

        inner = products.join(prices)
        print(f"\nInner join: {inner.collect()}")

        left = products.leftOuterJoin(prices)
        print(f"Left join: {left.collect()}")

        right = products.rightOuterJoin(prices)
        print(f"Right join: {right.collect()}")

        full = products.fullOuterJoin(prices)
        print(f"Full join: {full.collect()}")

        # Task 4: Aggregation Operations

        employees = sc.parallelize([
            ("Engineering", ("Alice", 90000)),
            ("Engineering", ("Bob", 85000)),
            ("Sales", ("Charlie", 70000)),
            ("Engineering", ("Diana", 95000)),
            ("Sales", ("Eve", 75000)),
            ("HR", ("Frank", 60000))
        ])

        dept_counts = employees.mapValues(lambda _: 1).reduceByKey(lambda a, b: a + b)
        print(f"\nEmployee counts: {dept_counts.collect()}")

        dept_salaries = employees.mapValues(lambda x: x[1]).reduceByKey(lambda a, b: a + b)
        print(f"Total salaries: {dept_salaries.collect()}")

        sum_count = employees.mapValues(lambda x: (x[1], 1)).reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1])
        )
        dept_avg_salary = sum_count.mapValues(lambda scnt: scnt[0] / scnt[1])
        print(f"Average salaries: {dept_avg_salary.collect()}")

        # Task 5: sortByKey

        alphabetical = counts.sortByKey()
        print(f"\nAlphabetical (first 10): {alphabetical.take(10)}")

        reverse = counts.sortByKey(ascending=False)
        print(f"Reverse (first 10): {reverse.take(10)}")


    finally:
        sc.stop()


if __name__ == "__main__":
    main()