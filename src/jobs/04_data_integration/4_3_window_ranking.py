import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.appName("Window_Functions_Ranking").getOrCreate()

    # JDBC connection setting
    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    # Get the `Golden Layer` data from Postgres
    print("--- Reading Golden Layer from Postgres ---")
    df = spark.read.jdbc(
        url=db_url, table="fact_maintenance_logs", properties=properties
    )

    # Define the Window Specification
    window_spec = Window.partitionBy("device_id").orderBy(F.col("timestamp").desc())

    # Apply row_number() to find the latest record
    ranked_df = df.withColumn("latest_rank", F.row_number().over(window=window_spec))

    # Filter for the latest record per aircraft
    latest_status_df = ranked_df.filter(F.col("latest_rank") == 1).select(
        "device_id", "aircraft_type", "timestamp", "status", "value"
    )

    print("--- Latest Status for Each Aircraft (Rank 1) ---")
    latest_status_df.show()

    # Advanced: Cumulative Error Count per Aircraft
    # Note: We don't need descending here
    trend_window = Window.partitionBy("device_id").orderBy("timestamp")

    analysis_df = df.withColumn(
        "cumulative_errors",
        F.sum(F.when(F.col("status") == "Error", 1).otherwise(0)).over(trend_window),
    )

    print("--- Maintenance Trend (Cumulative Errors) ---")
    analysis_df.select("device_id", "timestamp", "status", "cumulative_errors").orderBy(
        "device_id", "timestamp"
    ).show(10)

    spark.stop()


if __name__ == "__main__":
    main()
