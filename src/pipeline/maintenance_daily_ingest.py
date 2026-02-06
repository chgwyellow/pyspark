import os

import pyspark.sql.functions as F

from src.utils.spark_helper import get_db_properties, get_spark_session


def run_pipeline():
    # 1. Initialize Spark & DB Config
    # Partitions will be picked up from os.environ by our helper
    spark = get_spark_session(app_name="Daily_Maintenance_ETL")
    db_config = get_db_properties()

    # Get Salt Factor from environment (injected by run_job.py)
    # Default to 20 if not found
    salt_factor = int(os.getenv("SALT_FACTOR", "20"))

    print(f"\n🏗️  Starting ETL Pipeline with Salt Factor: {salt_factor}")

    # 2. Extract
    # Note: Using relative path assuming we run from project root
    log_df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )
    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    # 3. Transform: Salting Logic (Modular Implementation)
    # We salt the logs (Left Table)
    import time

    compute_start = time.time()

    salted_log_df = log_df.withColumn(
        "salted_id",
        F.concat(F.col("device_id"), F.lit("_"), F.floor(F.rand() * salt_factor)),
    )

    # We explode the dimension table (Right Table)
    salt_array = F.array([F.lit(i) for i in range(salt_factor)])
    explode_df = aircraft_df.withColumn(
        "salt",
        F.explode(salt_array),
    ).withColumn("salted_id", F.concat(F.col("tail_number"), F.lit("_"), F.col("salt")))

    # Perform the Optimized Join
    enriched_df = salted_log_df.join(explode_df, on="salted_id", how="inner").drop(
        "salted_id", "salt"
    )

    result_count = enriched_df.count()

    compute_end = time.time()

    # 4. Load (Optional: In this stage, we might just show count or write to a new table)
    # Target table name can also be moved to settings.py late
    print(f"📊 Processed {result_count} rows in {compute_end - compute_start:.2f}s")
    enriched_df.show()

    input("Press Enter to continue...")

    spark.stop()


def main():
    # salt_factor will be read from SALT_FACTOR env var (set by run_job.py)
    run_pipeline()


if __name__ == "__main__":
    main()
