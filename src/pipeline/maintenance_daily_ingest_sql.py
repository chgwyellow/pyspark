import os
import sys
import time
from src.utils.spark_helper import get_db_properties, get_spark_session


def run_pipeline():
    spark = get_spark_session("Daily_Maintenance_ETL_SQL_Version")
    db_config = get_db_properties()
    salt_factor = int(os.getenv("SALT_FACTOR", "20"))

    print(f"\n🏗️  Starting SQL-Based ETL Pipeline with Salt Factor: {salt_factor}")

    # --- 1. Extract (Python) ---
    log_df = spark.read.csv(
        "data/raw/equipment_logs_skewed.csv", header=True, inferSchema=True
    )
    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    # Register to View
    log_df.createOrReplaceTempView("raw_logs")
    aircraft_df.createOrReplaceTempView("dim_aircraft")

    # Record the original count
    raw_log_count = log_df.count()
    compute_start = time.time()

    # --- 2. Transform (SQL / Salting Logic) ---
    transformation_sql = f"""
        with salted_logs as (
            select *,
                concat(device_id, '_', cast(floor(rand() * {salt_factor}) as int)) as salted_id
            from raw_logs
        ),
        exploded_aircraft as (
            select *,
                concat(tail_number, '_', salt_val) as salted_id
            from dim_aircraft
            lateral view explode(sequence(0, {salt_factor} - 1)) as salt_val
        )
        select
            l.*,
            a.aircraft_type,
            a.delivery_date,
            a.total_flight_hours
        from salted_logs l
        inner join exploded_aircraft a on l.salted_id = a.salted_id
    """

    enriched_df = spark.sql(transformation_sql)

    # Register for DQ
    enriched_df.createOrReplaceTempView("v_enriched_data")

    # --- 3. Data Quality (DQ) Checks ---
    print("🛡️  Running SQL-Based Data Quality Checks...")

    dq_metrics = spark.sql(
        """
        select
            count(*) as total_count,
            sum(
                case
                    when aircraft_type is null then 1 else 0
                end
            ) as null_types
        from v_enriched_data
    """
    ).collect()[0]

    result_count = dq_metrics["total_count"]
    null_count = dq_metrics["null_types"]

    # DQ judgement
    if raw_log_count != result_count:
        print(
            f"❌ DQ Failure: Row count mismatch! Expecting {raw_log_count}, got {result_count}."
        )
        sys.exit(1)
    if null_count > 0:
        print(f"❌ DQ Alert: Found {null_count} rows with NULL aircraft_type!")
        sys.exit(1)

    print(f"✅ DQ Success: All checks passed ({result_count} records).")
    compute_end = time.time()

    # --- 4. Load (Python) ---
    target_table = "fact_aviation_maintenance_gold"
    print(f"💾 Saving processed {result_count} rows to {target_table}...")

    try:
        enriched_df.write.jdbc(
            url=db_config.url,
            table=target_table,
            properties=db_config.properties,
            mode="overwrite",
        )
        print(f"✨ Success! Pipeline runtime: {compute_end - compute_start:.2f}s")
    except Exception as e:
        print(f"🔥 Database Write Error: {e}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()
