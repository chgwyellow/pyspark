import os
from collections import namedtuple

from pyspark.sql import SparkSession

# Define a structured container for Database configuration
# This provides immutable, named access to DB settings
DbConfig = namedtuple(typename="DbConfig", field_names=["url", "properties"])


def get_spark_session(app_name: str = "Aviation_Data_Pipeline"):
    """
    Standardize SparkSession creation with production-grade configurations.
    - shuffle.partitions=13: Using a prime number to reduce hash collisions.
    - autoBroadcastJoinThreshold=-1: Disabled to allow testing of shuffle joins.
    """
    # Pick up from environment (set by run_job.py) or use default '13'
    partition_count = os.getenv(key="SPARK_SHUFFLE_PARTITIONS", default="13")

    print(f"🔧 Spark Config: shuffle.partitions set to {partition_count}")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", partition_count)
        .getOrCreate()
    )


def get_db_properties() -> namedtuple:
    """
    Centrally manage database connection info using namedtuple.
    Returns a DbConfig object.
    """
    # Use host.docker.internal for local docker development
    db_host = os.getenv("POSTGRES_HOST", "host.docker.internal")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "maintenance_db")

    url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    properties = (
        {
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "password"),
            "driver": "org.postgresql.Driver",
        },
    )

    return DbConfig(url=url, properties=properties)
