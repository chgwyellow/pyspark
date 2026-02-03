import os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType


def main():
    spark = SparkSession.builder.appName("UDF_Performance_Lab").getOrCreate()

    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    df = spark.read.jdbc(
        url=db_url, table="fact_maintenance_logs", properties=properties
    )

    # Define Python logic, some kind of the complex error categories
    def classify_maintenance_priority(status, value):
        if status == "Error" and value > 90:
            return "CRITICAL_AOG"  # Aircraft on Ground
        elif status == "Error" or status == "Maintenance":
            return "SCHEDULED_CHECK"
        else:
            return "ROUTINE_MONITOR"

    # Register UDF and specify the return type
    classify_udf = F.udf(classify_maintenance_priority, StringType())

    # Conversion
    final_df = df.withColumn(
        "maintenance_priority", classify_udf(F.col("status"), F.col("value"))
    ).withColumn(
        "is_urgent",
        F.when(F.col("value") > 85, True).otherwise(False),  # Recommendation
    )

    print("--- Maintenance Priority Classification (via UDF) ---")
    final_df.select(
        "device_id", "status", "value", "maintenance_priority", "is_urgent"
    ).show(10)

    spark.stop()


if __name__ == "__main__":
    main()
