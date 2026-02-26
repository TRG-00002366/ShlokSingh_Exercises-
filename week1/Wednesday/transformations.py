from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "Transformations")

    try:
        
        # Task 1: Narrow Transformations

        logs = sc.parallelize([
            "2024-01-15 10:00:00 INFO User login: alice",
            "2024-01-15 10:01:00 ERROR Database connection failed",
            "2024-01-15 10:02:00 INFO User login: bob",
            "2024-01-15 10:03:00 WARN Memory usage high",
            "2024-01-15 10:04:00 ERROR Timeout occurred",
            "2024-01-15 10:05:00 INFO Data processed: 1000 records",
            "2024-01-15 10:06:00 DEBUG Cache hit rate: 95%"
        ])

        errors = logs.filter(lambda line: " ERROR " in line)
        print(f"Errors: {errors.collect()}")

        levels = logs.map(lambda line: line.split()[2])
        print(f"Levels: {levels.collect()}")
        error_messages = (
            logs.filter(lambda line: " ERROR " in line)
                .map(lambda line: line.split(" ERROR ", 1)[1])
        )
        print(f"Error messages: {error_messages.collect()}")

        print("-" * 40)

        # Task 2: Wide Transformations

        words = sc.parallelize([
            "spark", "hadoop", "spark", "data", "hadoop",
            "spark", "python", "data", "spark", "scala"
        ])

        unique = words.distinct()
        print(f"Unique words: {unique.collect()}")

        word_counts = words.map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
        print(f"Word counts: {word_counts.collect()}")

        sorted_counts = word_counts.sortBy(lambda kv: kv[1], ascending=False)
        print(f"Sorted: {sorted_counts.collect()}")

        print("-" * 40)

        # Task 3: Set Operations

        set1 = sc.parallelize([1, 2, 3, 4, 5])
        set2 = sc.parallelize([4, 5, 6, 7, 8])

        combined = set1.union(set2)
        print(f"Union: {combined.collect()}")

        common = set1.intersection(set2)
        print(f"Intersection: {common.collect()}")

        difference = set1.subtract(set2)
        print(f"Difference: {difference.collect()}")

        print("-" * 40)

        # Task 4: Practical Pipeline

        web_logs = sc.parallelize([
            "192.168.1.1 GET /home 200 150ms",
            "192.168.1.2 GET /products 200 230ms",
            "192.168.1.1 POST /login 200 180ms",
            "192.168.1.3 GET /home 404 50ms",
            "192.168.1.2 GET /products 200 210ms",
            "192.168.1.1 GET /home 200 120ms",
            "192.168.1.4 GET /admin 403 30ms"
        ])

        endpoint_counts = (
            web_logs
            .filter(lambda line: line.split()[3] == "200")
            .map(lambda line: (line.split()[2], 1))
            .reduceByKey(lambda a, b: a + b)
            .sortBy(lambda kv: kv[1], ascending=False)
        )

        print(f"Endpoint counts (sorted): {endpoint_counts.collect()}")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()