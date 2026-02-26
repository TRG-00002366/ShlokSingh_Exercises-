from pyspark import SparkContext


def main():
    sc = SparkContext("local[*]", "AccumulatorExercise")

    try:

        # Task 1: Basic Counter Accumulator

        record_counter = sc.accumulator(0)

        data = sc.parallelize(range(1, 101))

        def count_record(x):
            record_counter.add(1)
            return x

        data.map(count_record).collect()

        print(f"Records processed: {record_counter.value}")  # Expected: 100
        print("-" * 40)

        # Task 2: Error Counting Pattern
        
        records = sc.parallelize([
            "100,Alice,Engineering",
            "200,Bob,Sales",
            "INVALID_RECORD",
            "300,Charlie,Marketing",
            "",
            "400,Diana,Engineering",
            "BAD_DATA_HERE",
            "500,Eve,Sales"
        ])

        total_records = sc.accumulator(0)
        valid_records = sc.accumulator(0)
        invalid_records = sc.accumulator(0)

        def validate_record(record: str):
            total_records.add(1)

            if record is None or record.strip() == "":
                invalid_records.add(1)
                return None

            parts = record.split(",")

            if len(parts) != 3:
                invalid_records.add(1)
                return None
            
            if any(p.strip() == "" for p in parts):
                invalid_records.add(1)
                return None

            valid_records.add(1)
            return record

        valid_data = records.map(validate_record).filter(lambda x: x is not None)
        valid_data.collect()  # action

        print(f"Total records: {total_records.value}")     # Expected: 8
        print(f"Valid records: {valid_records.value}")     # Expected: 5
        print(f"Invalid records: {invalid_records.value}") # Expected: 3

        error_rate = (invalid_records.value / total_records.value * 100) if total_records.value else 0.0
        print(f"Error rate: {error_rate:.1f}%")            # Expected: 37.5%
        print("-" * 40)

        # Task 3: Category Counter

        sales = sc.parallelize([
            ("Electronics", 999),
            ("Clothing", 50),
            ("Electronics", 299),
            ("Food", 25),
            ("Clothing", 75),
            ("Electronics", 149),
            ("Food", 30)
        ])

        electronics_count = sc.accumulator(0)
        clothing_count = sc.accumulator(0)
        food_count = sc.accumulator(0)

        def count_by_category(record):
            category, _ = record
            if category == "Electronics":
                electronics_count.add(1)
            elif category == "Clothing":
                clothing_count.add(1)
            elif category == "Food":
                food_count.add(1)
            return record

        sales.foreach(count_by_category)

        print(f"Electronics: {electronics_count.value}")  # Expected: 3
        print(f"Clothing: {clothing_count.value}")        # Expected: 2
        print(f"Food: {food_count.value}")                # Expected: 2
        print("-" * 40)

        # Task 4: Sum Accumulator

        total_sales = sc.accumulator(0)

        def sum_sales(record):
            _, amount = record
            total_sales.add(amount)

        sales.foreach(sum_sales)  # action

        print(f"Total sales: ${total_sales.value}")  # Expected: $1627

    finally:
        sc.stop()


if __name__ == "__main__":
    main()