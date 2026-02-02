import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder.appName("spark.jars").getOrCreate()

    db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'host.docker.internal')}:{os.getenv('POSTGRES_PORT', '5432')}/maintenance_db"
    properties = {
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }

    # 1. Load datasets
    # Fact table
    logs_df = spark.read.parquet("data/processed/equipment_logs")

    # Dimension table
    aircraft_df = spark.read.jdbc(
        url=db_url, table="aircraft_master", properties=properties
    )

    # 2. Perform Left Join
    enriched_df = logs_df.join(
        F.broadcast(aircraft_df),
        on=logs_df.device_id == aircraft_df.tail_number,
        how="left",
    )

    # 3. --- DATA QUALITY GATES ---
    print("--- Starting Data Quality Checks ---")

    # A. Check for Row Integrity
    original_count = logs_df.count()
    current_count = enriched_df.count()
    if original_count != current_count:
        print(
            f"⚠️ DATA EXPLOSION: Original rows {original_count} vs Joined rows {current_count}"
        )
    else:
        print(f"✅ Row count consistent at {current_count}")

    # B. Identify Unmapped Records (Orphans)
    # This is the most important check for DEs
    orphans = enriched_df.filter(F.col("tail_number").isNull())
    orphan_count = orphans.count()

    if orphan_count > 0:
        print(
            f"❌ DATA LOSS: Found {orphan_count} log entries with no matching aircraft master data!"
        )
        orphans.select("device_id").distinct.show()
    else:
        print("✅ Mapping Success: 100% of logs have associated aircraft info.")

    # 4. Filter and Save Clean Data
    # Only save records that successfully joined for the next analysis chapter
    final_valid_df = enriched_df.filter(F.col("tail_number").isNotNull()).select(
        "log_id", "device_id", "aircraft_type", "status", "value", "timestamp"
    )

    # Write back to Postgres to create 'Golden Layer' table
    final_valid_df.write.jdbc(
        url=db_url,
        table="fact_maintenance_logs",
        mode="overwrite",
        properties=properties,
    )
    print("🚀 Golden Layer table 'fact_maintenance_logs' created in Postgres.")

    spark.stop()


if __name__ == "__main__":
    main()
