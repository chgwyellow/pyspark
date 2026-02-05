import os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("Join_Skew_Explosion")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Turn off auto broadcast
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

    aircraft_master_df = spark.read.jdbc(
        url=db_url, table="dim_aircraft", properties=properties
    )

    print("🚀 Triggering a Raw Shuffle Join (No Pre-aggregation)...")

    # This join will trigger a Shuffle.
    # Because B-58201 has 4.5M rows, one specific partition will be 100x larger than others.
    skewed_joined_df = log_df.join(
        aircraft_master_df,
        on=log_df.device_id == aircraft_master_df.tail_number,
        how="inner",
    )

    # 4. Action: Force full computation to observe Spark UI
    # Turn off map-side optimization
    print("⏳ Processing total records via RDD count to bypass Optimizer...")
    final_count = skewed_joined_df.rdd.count()

    print(f"✅ Join finished. Total records: {final_count}")
    input("Press Enter to finish and stop Spark...")

    spark.stop()


if __name__ == "__main__":
    main()
