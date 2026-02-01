from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


def main():
    # Initialize SparkSession
    spark = SparkSession.builder.appName("Core_Architecture_Testing").getOrCreate()

    # Load raw data
    file_path = "data/raw/equipment_logs.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    print("--- Spark DataFrame Overview ---")
    df.printSchema()

    # Check Partitions (The core of Distributed Computing)
    # This shows how Spark splits data across multiple executors
    num_partitions = df.rdd.getNumPartitions()
    print(f"Data is split into {num_partitions} partitions.")

    # Show distributed aggregation result
    print("--- Status Count (Distributed Execution) ---")
    df.groupBy("status").count().sort(desc("count")).show()

    spark.stop()


if __name__ == "__main__":
    main()
