# 1_2 Data Ingestion and Schema Definition

## 1. The Importance of Explicit Schema

In enterprise data pipelines, using `inferSchema=True` is discouraged. Defining an **Explicit Schema** using `StructType` provides:

* **Performance**: Spark does not need to scan the data twice to determine types.
* **Reliability**: Ensures that data types (e.g., Integer vs. String) are consistent, preventing downstream job failures.

## 2. Partitioning Mechanics

Spark splits data into **Partitions** for parallel processing.

* **Small Files Problem**: For small datasets (like our 1000-row CSV), Spark often defaults to a single partition, which limits parallelism.
* **repartition(n)**: Increases or decreases partitions by performing a **Full Shuffle**. This is useful for balancing workload across executors.
* **coalesce(n)**: Decreases partitions without a full shuffle. Best used before saving data to reduce the number of output files.

## 3. Implementation Details

* **Entry Point**: `SparkSession` is used to trigger the ingestion.
* **Validation**: `df.printSchema()` is the primary tool to verify that the physical data matches our logical definition.
* **API Usage**: Employed `pyspark.sql.functions` (aliased as `F`) for robust column referencing.

## 4. Key Takeaways

* Always define schemas manually for production ETL jobs.
* Monitor partition counts to ensure your cluster's CPU cores are fully utilized.
