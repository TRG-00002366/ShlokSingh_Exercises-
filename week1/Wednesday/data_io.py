from pyspark import SparkContext
import os
import shutil


def parse_record(line: str):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4]),
    }


def delete_path_if_exists(path: str):
    
    if os.path.exists(path):
        shutil.rmtree(path)


def main():
    sc = SparkContext("local[*]", "DataIO")

    try:

        # Task 1: Load Data with textFile

        lines = sc.textFile("sales_data.csv")

        header = lines.first()
        data = lines.filter(lambda line: line != header)

        print(f"Header: {header}")
        print(f"Data records: {data.count()}")
        print(f"First record: {data.first()}")
        print("-" * 40)

        # Task 2: Parse CSV Records

        parsed = data.map(parse_record)

        print("First 3 parsed records:")
        for record in parsed.take(3):
            print(record)
        print("-" * 40)

        # Task 3: Process and Save Results

        revenue = parsed.map(
            lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}"
        )

        out_dir = "output_revenue"
        delete_path_if_exists(out_dir)

        
        revenue.saveAsTextFile(out_dir)
        print(f"Saved revenue output to: {out_dir}/")
        print("-" * 40)

        # Task 4: Load Multiple Files (wildcards)

        all_lines = sc.textFile("sales_data*.csv")

        all_header = all_lines.first()  
        all_data = all_lines.filter(lambda line: line != all_header)

        all_parsed = all_data.map(parse_record)
        print(f"Total records across sales_data*.csv (excluding header): {all_parsed.count()}")
        print("Sample from combined files:")
        for record in all_parsed.take(3):
            print(record)
        print("-" * 40)

        # Task 5: Coalesce Output (single part file)

        revenue_all = all_parsed.map(
            lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}"
        )

        out_dir_single = "output_revenue_single"
        delete_path_if_exists(out_dir_single)

        revenue_all.coalesce(1).saveAsTextFile(out_dir_single)
        print(f"Saved SINGLE-FILE revenue output to: {out_dir_single}/ (look for part-00000...)")

    finally:
        sc.stop()


if __name__ == "__main__":
    main()
