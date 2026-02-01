import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
)


def main():
    spark = SparkSession.builder.appName("Aggregation_Lab").getOrCreate()

    # 1. Fact data
    schema = StructType(
        [
            StructField("log_id", IntegerType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    log_df = spark.read.csv("data/raw/equipment_logs.csv", header=True, schema=schema)

    # 2. Prepare Dimension Data (Simulated Device Master Data)
    device_master_data = [
        ("DEV-101", "Sensor-Type-A", "B-58201"),
        ("DEV-104", "Sensor-Type-A", "B-58201"),
        ("DEV-105", "Sensor-Type-B", "B-58202"),
        ("DEV-109", "Sensor-Type-C", "B-58203"),
    ]
    master_columns = ["device_id", "category", "location"]
    master_df = spark.createDataFrame(data=device_master_data, schema=master_columns)

    # 3. Perform Left Join to enrich logs with master data
    # We use a Broadcast Join here because master_df is small
    enriched_df = log_df.join(F.broadcast(master_df), on="device_id", how="left")

    # 4. Data Quality Check: Use Anti Join to find logs without master data
    # This is a classic Data Engineering pattern for finding "orphan" records
    orphans_df = log_df.join(master_df, on="device_id", how="left_anti")

    print("--- Enriched Equipment Logs ---")
    enriched_df.select("log_id", "device_id", "category", "location", "value").show(5)

    print("--- Orphan Logs (No Master Data Found) ---")
    orphans_df.select("device_id").distinct().show()

    spark.stop()


if __name__ == "__main__":
    main()
