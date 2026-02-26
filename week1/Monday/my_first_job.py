from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse


def main():
    parser = argparse.ArgumentParser(description="My first PySpark job (local mode).")
    parser.add_argument(
        "--master",
        default="local[*]",
        help='Spark master URL. Examples: "local", "local[2]", "local[*]"',
    )
    args = parser.parse_args()

    spark = None
    try:
        # Step 1: Create SparkSession
        spark = (
            SparkSession.builder
            .appName("MyFirstJob")
            .master(args.master)
            .getOrCreate()
        )

        # Step 2: Create some data
        sales_data = [
            ("Laptop", "Electronics", 999.99, 5),
            ("Mouse", "Electronics", 29.99, 50),
            ("Desk", "Furniture", 199.99, 10),
            ("Chair", "Furniture", 149.99, 20),
            ("Monitor", "Electronics", 299.99, 15),
        ]
        df = spark.createDataFrame(sales_data, ["product", "category", "price", "quantity"])

        # Step 3: Perform transformations / operations

        
        print("\nOriginal DataFrame:")
        df.show()

        total_products = df.count()
        print(f"Total products: {total_products}\n")

        df_rev = df.withColumn("revenue", F.round(F.col("price") * F.col("quantity"), 2))
        print("Revenue per product:")
        df_rev.show()

        print("Electronics only:")
        df.filter(F.col("category") == "Electronics").show()

        print("Revenue by category:")
        (
            df_rev.groupBy("category")
            .agg(F.round(F.sum("revenue"), 2).alias("total_revenue"))
            .orderBy("category")
            .show()
        )

        # Stretch Goal: product with highest revenue
        print("Highest revenue product:")
        df_rev.orderBy(F.col("revenue").desc()).select("product", "category", "revenue").show(1)

    except Exception as e:
        print("Job failed with error:")
        print(e)
        raise
    finally:
        # Step 5: Clean up
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()