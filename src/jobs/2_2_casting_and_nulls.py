import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType


def main():
    spark = SparkSession.builder.appName("Data_Cleaning_PoC").getOrCreate()

    # There is a "dirty" dataset
    dirty_data = [
        (1, "DEV-101", "50.5", "Running"),
        (2, "DEV-102", "80.0", None),  # Missing Status
        (3, "DEV-103", "Broken", "Error"),  # Invalid Value (String)
        (4, "DEV-104", None, "Idle"),  # Missing Value
    ]

    columns = ["log_id", "device_id", "raw_value", "status"]
    df = spark.createDataFrame(data=dirty_data, schema=columns)

    # 1. Casting: Convert 'raw_value' from String to Double
    # Notice: "Broken" will become null automatically
    df_casted = df.withColumn("value", F.col("raw_value").cast(DoubleType()))

    # 2. Null Handling:
    # Fill missing 'status' with "Unknown"
    # Fill missing 'value' with 0.0
    df_cleaned = df_casted.na.fill(value={"status": "Unknown", "value": 0.0})

    # 3. Filtering: Drop rows where device_id is null (though not in our data)
    final_df = df_cleaned.na.drop(subset=["device_id"])

    print("--- Cleaned Data Output ---")
    final_df.select("log_id", "device_id", "value", "status").show()

    spark.stop()


if __name__ == "__main__":
    main()
