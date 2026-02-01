import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


def main():
    spark = SparkSession.builder.appName("Basic_Operations_PoC").getOrCreate()

    # Define schema
    schema = StructType(
        [
            StructField("log_id", IntegerType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    # Load Data
    file_path = "data/raw/equipment_logs.csv"
    df = spark.read.csv(file_path, header=True, schema=schema)

    # 1. Filter: Find devices that are "Running" AND have a value > 50
    filtered_df = df.filter((F.col("status") == "Running") & (F.col("value") > 50))

    # 2. WithColumn(): Create a new column 'flag_high_value' based on condition
    # Using F.when() which is similar to CASE WHEN in SQL
    processed_df = filtered_df.withColumn(
        colName="is_high_value",
        col=F.when(F.col("value") > 80, "High").otherwise("Normal"),
    )

    # 3. Select: Keep only specific columns and rename one for clarity
    final_df = processed_df.select(
        F.col("log_id"),
        F.col("device_id"),
        F.col("timestamp"),
        F.col("status"),
        F.col("value").alias("sensor_reading"),
        F.col("is_high_value"),
    )

    print("--- Processed Data (First 5 rows) ---")
    final_df.show(5)

    # Verify Immutability: The original df schema remains unchanged
    print(f"Original columns: {len(df.columns)}")
    print(f"Final columns: {len(final_df.columns)}")

    spark.stop()


if __name__ == "__main__":
    main()
