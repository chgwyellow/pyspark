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
