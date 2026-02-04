import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("Time_Mastery_Lab").getOrCreate()

    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.jdbc(
        url=db_url, table="fact_maintenance_logs", properties=properties
    )

    # Extend the time dimension
    time_df = (
        df.withColumn("log_date", F.to_date(F.col("timestamp")))
        .withColumn("log_hour", F.hour(F.col("timestamp")))
        .withColumn(
            "log_month", F.date_trunc(format="month", timestamp=F.col("timestamp"))
        )
    )

    # Shift Classification
    # 06:00-14:00 is morning, 14:00-22:00 is afternoon, otherwise, night
    shift_df = time_df.withColumn(
        "maintenance_shift",
        F.when((F.col("log_hour") >= 6) & (F.col("log_hour") < 14), "Morning Shift")
        .when((F.col("log_hour") >= 14) & (F.col("log_hour") < 22), "Afternoon_shift")
        .otherwise("Night Shift"),
    )

    # Statistic analysis for average value and counts in every type and shift
    report_df = (
        shift_df.groupBy("aircraft_type", "maintenance_shift")
        .agg(
            F.count("log_id").alias("total_events"),
            F.round(F.avg("value"), 2).alias("avg_sensor_reading"),
        )
        .orderBy("aircraft_type", "maintenance_shift")
    )

    print("--- Maintenance Shift Performance Report ---")
    report_df.show()

    spark.stop()


if __name__ == "__main__":
    main()
