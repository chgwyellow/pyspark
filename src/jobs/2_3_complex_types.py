import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType


def main():
    spark = SparkSession.builder.appName("Complex_Types_PoC").getOrCreate()

    # 1. Prepare data with Array and Struct (Simulating IoT Sensor Bursts)
    complex_data = [
        ("DEV-101", [50.1, 50.5, 50.2], {"location": "Plant-A", "type": "Temp"}),
        ("DEV-102", [80.0, 81.2], {"location": "Plant-B", "type": "Pressure"}),
    ]

    # Define Schema for complex structures
    schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("readings", ArrayType(DoubleType()), True),
            StructField(
                "metadata",
                StructType(
                    [
                        StructField("location", StringType(), True),
                        StructField("type", StringType(), True),
                    ]
                ),
                True,
            ),
        ]
    )

    df = spark.createDataFrame(data=complex_data, schema=schema)

    # 2. Accessing Struct fields using dot notation
    df_with_location = df.withColumn("plant_loc", F.col("metadata.location"))

    # 3. Exploding the Array: One row becomes multiple rows
    # This is the most common way to flatten time-series bursts
    df_exploded = df_with_location.withColumn(
        "single_reading", F.explode(F.col("readings"))
    )

    print("--- Original Complex Structure ---")
    df.show(truncate=False)

    print("--- with location ---")
    df_with_location.show(truncate=False)

    print("--- After Exploding Arrays ---")
    df_exploded.select("device_id", "plant_loc", "single_reading").show()

    spark.stop()


if __name__ == "__main__":
    main()
