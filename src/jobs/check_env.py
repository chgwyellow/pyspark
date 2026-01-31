# Check environment
# This script verifies that Java, Spark, and Python are properly configured

import polars as pl
from pyspark.sql import SparkSession

# In Spark, every manipulation must start at `SparkSession`

# Create a application named `Aircraft_Lab`
spark = SparkSession.builder.appName("Aircraft_Lab").getOrCreate()

data = [
    {"aircraft_id": "B-58201", "status": "Good", "work_hours": 5},
    {"aircraft_id": "B-58501", "status": "Maintenance", "work_hours": 20},
]

# Turn to polars DataFrame
pl_df = pl.DataFrame(data)

# Turn to Spark DataFrame, it needs to be converted to Pandas first
spark_df = spark.createDataFrame(pl_df.to_pandas())

# Show the result
spark_df.show()

# Turn off the engine
spark.stop()
