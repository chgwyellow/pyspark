import pyspark.sql.functions as F

from src.utils.spark_helper import get_spark_session, get_db_properties


def run_pipeline(salt_factor: int = 20):
    # 1. Initialize Spark & DB Config
    # Partitions will be picked up from os.environ by our helper
    spark = get_spark_session(app_name="Daily_Maintenance_ETL")
    db_config = get_db_properties()

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

    # 4. Load (Optional: In this stage, we might just show count or write to a new table)
    # Target table name can also be moved to settings.py late
    print(f"\n📊 Processed Record Count: {enriched_df.count()}")
    enriched_df.show()

    input("Press Enter to continue...")

    spark.stop()


def main():
    run_pipeline(salt_factor=24)


if __name__ == "__main__":
    main()
