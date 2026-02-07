import os

import pyspark.sql.functions as F

from src.utils.spark_helper import get_db_properties, get_spark_session


def run_pipeline():
    """
    Execute the daily maintenance log ingestion pipeline.

    Workflow:
    1. Extract equipment logs from CSV and aircraft dimension data from PostgreSQL.
    2. Apply Salting technique to mitigate data skew in equipment logs.
    3. Perform an inner join to enrich logs with aircraft details (type, model).
    4. Perform Data Quality (DQ) checks: Row count validation and NULL check.
    5. Load the enriched data into the data warehouse (upcoming in 6.2).

    Parameters:
    - salt_factor (int): Retrieved from environment variable DATA_PIPELINE_SALT_FACTOR.
    - partitions (int): Controlled via spark_helper and CLI.
    """

    spark = get_spark_session(app_name="Daily_Maintenance_ETL")
    db_config = get_db_properties()

    # Get Salt Factor from environment (injected by run_job.py)
    salt_factor = int(os.getenv("SALT_FACTOR", "20"))

    print(f"\n🏗️  Starting ETL Pipeline with Salt Factor: {salt_factor}")

    # --- 1. Extract ---
    log_df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )
    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    # Record the original logs count
    raw_log_count = log_df.count()

    import time

    compute_start = time.time()

    # --- 2. Transform (Salting Logic) ---
    # Salt to log_df
    salted_log_df = log_df.withColumn(
        "salted_id",
        F.concat(F.col("device_id"), F.lit("_"), F.floor(F.rand() * salt_factor)),
    )

    # Salt to and explode aircraft_df
    salt_array = F.array([F.lit(i) for i in range(salt_factor)])
    explode_df = aircraft_df.withColumn(
        "salt",
        F.explode(salt_array),
    ).withColumn("salted_id", F.concat(F.col("tail_number"), F.lit("_"), F.col("salt")))

    # Perform the Optimized Join
    enriched_df = salted_log_df.join(explode_df, on="salted_id", how="inner").drop(
        "salted_id", "salt"
    )

    # --- 3. Data Quality (DQ) Checks ---
    print("🛡️  Running Data Quality Checks...")

    # Check 1: Row Count Validation
    # In inner join, the original and current counts should be the same in this case
    result_count = enriched_df.count()

    if raw_log_count != result_count:
        raise ValueError(
            f"❌ DQ Failure: Row count mismatch! Expecting {raw_log_count}, but got {result_count}."
        )
    else:
        print(f"✅ DQ Success: Row counts match ({result_count} records).")

    # Check 2: NULL Check on Critical Columns
    # The critical column here is aircraft_type
    null_count = enriched_df.filter(F.col("aircraft_type").isNull()).count()
    if null_count > 0:
        raise ValueError(
            f"❌ DQ Alert: Found {null_count} rows with NULL aircraft_type!"
        )
    else:
        print("✅ DQ Success: No NULL values detected in critical columns.")

    compute_end = time.time()

    # --- 4. Final Output (Load placeholder) ---
    print(f"📊 Processed {result_count} rows in {compute_end - compute_start:.2f}s")
    enriched_df.show()

    input("Press Enter to continue...")

    spark.stop()


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
