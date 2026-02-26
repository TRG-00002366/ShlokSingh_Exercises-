from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "RDDBasics")

    try:

        # Task 1: Create RDDs from Different Sources

        numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
        print(f"Numbers: {numbers.collect()}")
        print(f"Partitions: {numbers.getNumPartitions()}")
        print("-" * 40)

        numbers_4 = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 4)
        print(f"Numbers (4 partitions): {numbers_4.collect()}")
        print(f"Partitions (explicit): {numbers_4.getNumPartitions()}")
        print("-" * 40)

        one_to_100 = sc.parallelize(range(1, 101))
        print(f"Range 1..100 count: {one_to_100.count()}")
        print(f"Range 1..100 first 10: {one_to_100.take(10)}")
        print("-" * 40)

        # Task 2: Apply map() Transformation

        squared = numbers.map(lambda x: x * x)
        print(f"Squared: {squared.collect()}")

        prefixed = numbers.map(lambda x: f"num_{x}")
        print(f"Prefixed: {prefixed.collect()}")
        print("-" * 40)

        # Task 3: Apply filter() Transformation

        evens = numbers.filter(lambda x: x % 2 == 0)
        print(f"Evens: {evens.collect()}")

        greater_than_5 = numbers.filter(lambda x: x > 5)
        print(f"> 5: {greater_than_5.collect()}")

        combined = numbers.filter(lambda x: (x % 2 == 0) and (x > 5))
        print(f"Even and > 5: {combined.collect()}")
        print("-" * 40)

        # Task 4: Apply flatMap() Transformation

        sentences = sc.parallelize([
            "Hello World",
            "Apache Spark is Fast",
            "PySpark is Python plus Spark"
        ])

        words = sentences.flatMap(lambda line: line.split())
        print(f"Words: {words.collect()}")

        word_lengths = words.map(lambda w: (w, len(w)))
        print(f"Word lengths: {word_lengths.collect()}")
        print("-" * 40)

        # Task 5: Chain Transformations (Pipeline)

        logs = sc.parallelize([
            "INFO: User logged in",
            "ERROR: Connection failed",
            "INFO: Data processed",
            "ERROR: Timeout occurred",
            "DEBUG: Cache hit"
        ])

        error_words = (
            logs.filter(lambda line: line.startswith("ERROR:"))
                .flatMap(lambda line: line.split())
                .map(lambda word: word.upper())
        )

        print(f"Error words: {error_words.collect()}")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()