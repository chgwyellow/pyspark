import os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder.appName("Salting_Optimization_Demo")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Turn off auto broadcast
        .config("spark.sql.shuffle.partitions", "13")
        .getOrCreate()
    )

    SALT_FACTOR = 24  # We will split the skewed key

    # 1. Read Skewed Log Data (5M rows)
    log_df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )

    # 2. Read Aircraft Master Data from DB
    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    aircraft_df = spark.read.jdbc(
        url=db_url, table="dim_aircraft", properties=properties
    )

    print(f"🚀 Applying Salting with factor {SALT_FACTOR}...")

    # 3. Add Salt to the Skewed Table (Left)
    # We append a random number from 0 to SALT_FACTOR-1 to device_id
    salted_log_df = log_df.withColumn(
        "salted_id",
        F.concat(F.col("device_id"), F.lit("_"), F.floor(F.rand() * SALT_FACTOR)),
    )

    # 4. Explode the Master Data Table (Right)
    # Each aircraft row is duplicated 10 times with suffixes _0 to _9
    salt_array = F.array([F.lit(i) for i in range(SALT_FACTOR)])
    exploded_aircraft_df = aircraft_df.withColumn(
        "salt", F.explode(salt_array)
    ).withColumn("salted_id", F.concat(F.col("tail_number"), F.lit("_"), F.col("salt")))

    # 5. Perform the Join on salted_id
    optimized_join_df = salted_log_df.join(
        exploded_aircraft_df, on="salted_id", how="inner"
    )

    print("⏳ Processing total count with Salted Join...")
    # Using rdd.count() again to ensure we see the true physical execution balance
    final_count = optimized_join_df.rdd.count()

    print(f"✅ Salting complete. Total records: {final_count}")
    input(
        "Check Spark UI: You should see 10-12 active tasks with similar duration. Press Enter..."
    )

    spark.stop()


if __name__ == "__main__":
    main()
