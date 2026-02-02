import pyspark.sql.functions as F

from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Storage_Formats_Lab").getOrCreate()

    # 1. Read the raw CSV data
    df = spark.read.csv("data/raw/equipment_logs.csv", header=True, inferSchema=True)

    # 2. Write to Parquet (The Gold Standard)
    # Notice: This will create a directory named 'equipment_logs.parquet'
    output_path = "data/processed/equipment_logs"

    # Using 'overwrite' mode to replace existing data
    df.write.mode("overwrite").parquet(output_path)
    print(f"✅ Data successfully written to {output_path}")

    # 3. Demonstrate Predicate Pushdown
    # When reading Parquet, Spark uses metadata to skip irrelevant data blocks
    parquet_df = spark.read.parquet(output_path)

    # Filter for a specific device - Spark won't scan the whole file if metadata helps
    filtered_df = parquet_df.filter(F.col("device_id") == "B-58301")

    print("--- Reading from Parquet (Filtered) ---")
    filtered_df.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
