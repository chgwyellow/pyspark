import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():

    # 1. Initialize Spark with PostgreSQL JDBC Driver
    # No need to config .jar path because Dockerfile has set the path
    # Spark will load all of .jar while starting
    spark = SparkSession.builder.appName("Postgres_Integration_PoC").getOrCreate()

    # 2. Connection Properties (Reading from environment variables is best practice)
    # Using 'host.docker.internal' to connect to local PostgreSQL from Docker container
    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    # 3. Read Aircraft Master Data from PostgreSQL
    print("--- Reading Aircraft Master from Postgres ---")
    aircraft_df = spark.read.jdbc(
        url=db_url, table="aircraft_master", properties=properties
    )
    aircraft_df.show()

    # 4. Read Local Parquet Data
    print("--- Reading Local Equipment Logs ---")
    logs_df = spark.read.parquet("data/processed/equipment_logs")

    # 5. Transformation: Join Logs with Aircraft Master using Broadcast
    final_report = logs_df.join(
        other=F.broadcast(aircraft_df),
        on=logs_df.device_id == aircraft_df.tail_number,
        how="inner",
    ).select("log_id", "tail_number", "aircraft_type", "status", "value")

    # --- Data Quality Check Section ---

    # 1. Check for Row Explosion
    original_count = logs_df.count()
    final_count = final_report.count()

    if original_count != final_count:
        print(f"⚠️ WARNING: Row count changed from {original_count} to {final_count}!")
    else:
        print(f"✅ Row count integrity verified: {final_count} rows.")

    # 2. Check for Nulls in enriched columns (Validation for Left Join)
    null_counts = final_report.filter(F.col("aircraft_type").isNull()).count()
    if null_counts > 0:
        print(
            f"❌ ERROR: Found {null_counts} records with missing aircraft type information!"
        )
    else:
        print("✅ All logs successfully mapped to master data.")

    # 3. Simple Business Logic Check
    # Total flight hours shouldn't be negative
    invalid_hours = aircraft_df.filter(F.col("total_flight_hours") < 0).count()
    if invalid_hours > 0:
        print(
            f"❌ DATA ISSUE: Found {invalid_hours} aircraft with negative flight hours."
        )

    # 6. Write Result Back to PostgreSQL as a new table
    print("--- Writing Results back to Postgres ---")
    final_report.write.jdbc(
        url=db_url,
        table="fact_equipment_details",
        mode="overwrite",
        properties=properties,
    )

    print("✅ Job Completed: Result saved to table 'fact_equipment_details'")
    spark.stop()


if __name__ == "__main__":
    main()
