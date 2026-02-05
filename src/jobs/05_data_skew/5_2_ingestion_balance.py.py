import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("Skew_Detection_Job")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    # Read the skewed CSV we just generated
    # Using inferSchema to force a data scan (triggers more work)
    print("⏳ Reading skewed dataset...")
    df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )

    # Trigger a Shuffle (The most common place for skew to appear)
    # We group by device_id and calculate the average sensor value
    print("🚀 Running Shuffle with 12 partitions...")

    result = df.groupBy("device_id").agg(
        F.count("log_id").alias("event_count"), F.avg(F.col("value")).alias("avg_value")
    )

    print("\n--- JOB EXECUTED ---")
    final_data = result.collect()

    print(f"✅ Processed {len(final_data)} groups.")
    input("Check Spark UI now! Press Enter to finish...")

    spark.stop()


if __name__ == "__main__":
    main()
