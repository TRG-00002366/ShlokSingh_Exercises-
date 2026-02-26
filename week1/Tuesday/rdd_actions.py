from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "RDDActions")

    try:
        numbers = sc.parallelize([10, 5, 8, 3, 15, 12, 7, 20, 1, 9])

        # Task 1: Basic Retrieval Actions
    
        all_nums = numbers.collect()
        print(f"All numbers: {all_nums}")

        count = numbers.count()
        print(f"Count: {count}")

        first = numbers.first()
        print(f"First: {first}")

        first_three = numbers.take(3)
        print(f"First 3: {first_three}")

        top_three = numbers.top(3)
        print(f"Top 3: {top_three}")

        smallest_three = numbers.takeOrdered(3)
        print(f"Smallest 3: {smallest_three}")

        # Task 2: Aggregation Actions

        total = numbers.reduce(lambda a, b: a + b)
        print(f"Sum: {total}")

        maximum = numbers.reduce(lambda a, b: a if a > b else b)
        print(f"Max: {maximum}")

        minimum = numbers.reduce(lambda a, b: a if a < b else b)
        print(f"Min: {minimum}")

        folded_sum = numbers.fold(0, lambda a, b: a + b)
        print(f"Folded sum: {folded_sum}")

        # Task 3: countByValue()

        colors = sc.parallelize(["red", "blue", "red", "green", "blue", "red", "yellow"])

        color_counts = colors.countByValue()
        print(f"Color counts: {dict(color_counts)}")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()