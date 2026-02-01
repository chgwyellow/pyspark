import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def main():
    # 1. Initialize SparkSession
    spark = SparkSession.builder.appName("Data_Ingestion_Module").getOrCreate()

    # 2. Define Explicit Schema
    # Avoid inferSchema in production to ensure data quality
    schema = StructType(
        [
            StructField("log_id", IntegerType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    # 3. Data Ingestion
    file_path = "data/raw/equipment_logs.csv"
    df = spark.read.csv(file_path, header=True, schema=schema)

    print("--- Spark DataFrame Overview ---")
    df.printSchema()

    # 4. Partition Management
    # Observation: Small files default to 1 partition
    # Re-partitioning to 4 to simulate a distributed environment
    print(f"Original partition count: {df.rdd.getNumPartitions()}")

    df_distributed = df.repartition(4)
    print(
        f"New partition count after repartition: {df_distributed.rdd.getNumPartitions()}"
    )

    # 5. Basic Transformation & Verification
    # Using a simple filter to verify the distributed data
    error_count = df_distributed.filter(F.col("status") == "Error").count()

    print("--- Step 2: Data Verification ---")
    print(f"Total error logs found: {error_count}")
    df_distributed.show(5)

    spark.stop()


if __name__ == "__main__":
    main()
