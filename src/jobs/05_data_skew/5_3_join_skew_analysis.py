import os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("Join_Skew_Explosion")
        .config("spark.sql.shuffle.partitions", "12")
        .getOrCreate()
    )

    # 1. Read Skewed Log Data (5M rows)
    log_df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )

    # 2. Read existing data from Postgres
    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    db_data = spark.read.jdbc(
        url=db_url, table="fact_maintenance_logs", properties=properties
    )

    aircraft_lookup = db_data.select("device_id", "aircraft_type").distinct()

    print("🚀 Triggering the Skewed Join (Large CSV x Small DB Table)...")

    # 3. This Shuffle Hash Join will force all 4.5M 'B-58201' rows to one executor
    skewed_joined_df = log_df.join(aircraft_lookup, on="device_id", how="inner")

    # 4. Action: Force full computation to observe Spark UI
    print("⏳ Processing total count (this might take a moment due to skew)...")
    final_count = skewed_joined_df.count()

    print(f"✅ Join finished. Total records: {final_count}")
    print("\n👉 Check Spark UI: Look for the Task with massive 'Shuffle Read Size'.")
    input("Press Enter to finish and stop Spark...")

    spark.stop()


if __name__ == "__main__":
    main()
