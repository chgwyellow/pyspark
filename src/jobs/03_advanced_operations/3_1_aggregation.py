import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


def main():
    spark = SparkSession.builder.appName("Aggregation_Lab").getOrCreate()

    schema = StructType(
        [
            StructField("log_id", IntegerType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    df = spark.read.csv("data/raw/equipment_logs.csv", header=True, schema=schema)

    # 1. Basic Aggregation: Count logs and average value per Device
    # Equivalent to SQL: SELECT device_id, count(*), avg(value) GROUP BY device_id
    stats_df = df.groupBy("device_id").agg(
        F.count("log_id").alias("total_logs"),
        F.avg("value").alias("avg_reading"),
    )

    # 2. Advanced Aggregation: Count specific status per Device using Pivot
    # This creates a summary table where statuses become columns
    pivot_df = df.groupBy("device_id").pivot("status").count().na.fill(0)

    print("--- Basic Statistics per Device ---")
    stats_df.show(5)

    print("--- Status Distribution (Pivot Table) ---")
    pivot_df.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
