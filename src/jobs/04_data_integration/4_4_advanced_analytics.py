import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


def main():
    spark = SparkSession.builder.appName("Advanced_Window_Analytics").getOrCreate()

    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    # 1. Load Golden Layer Data
    df = spark.read.jdbc(
        url=db_url, table="public.fact_maintenance_logs", properties=properties
    )

    # 2. Moving Average: Calculate the average of (Previous + Current + Next) value
    moving_window = (
        Window.partitionBy("device_id")
        .orderBy("timestamp")
        .rowsBetween(start=-1, end=1)  # previous one to next one
    )

    df_with_moving_avg = df.withColumn(
        "smooth_value", F.avg(F.col("value")).over(moving_window)
    )

    # 3. State Change Detection (Gaps and Islands)
    change_window = Window.partitionBy("device_id").orderBy("timestamp")

    df_with_change = df_with_moving_avg.withColumn(
        "prev_status", F.lag("status").over(change_window)
    ).withColumn(
        "is_status_changed",
        F.when(F.col("status") != F.col("prev_status"), 1).otherwise(0),
    )

    # 4. Cumulative Status Change Count
    df_final = df_with_change.withColumn(
        "status_change_event_id", F.sum("is_status_changed").over(change_window)
    )

    print("--- Advanced Analytics: Smoothing and Change Detection ---")
    df_final.select(
        "device_id",
        "timestamp",
        "status",
        "value",
        "smooth_value",
        "status_change_event_id",
    ).orderBy("device_id", "timestamp").show(15)

    spark.stop()


if __name__ == "__main__":
    main()
