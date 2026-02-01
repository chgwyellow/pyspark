# 1_1 Spark Core Architecture and Distributed Computing

## 1. What is PySpark?

PySpark is the Python API for Apache Spark. It enables data engineers to write scalable code using Python syntax, while the underlying execution is performed on the **Java Virtual Machine (JVM)** via the **Py4J** bridge.

## 2. Cluster Architecture (Master-Slave Model)

Spark operates using a centralized architecture:

* **Driver Program**: The process that runs the `main()` function. It translates user code into a Logical Plan and converts it into a **Directed Acyclic Graph (DAG)** of tasks.
* **Cluster Manager**: Responsible for resource allocation (e.g., Standalone, YARN, or Kubernetes). In this Docker lab, we use `local` mode.
* **Executor Nodes**: Distributed worker processes that execute the tasks assigned by the Driver and return results to the Driver or storage.

## 3. The Concept of Partitioning

Partitioning is the key to parallelism in Spark:

* **Definition**: Spark splits a massive dataset into smaller chunks called **Partitions**.
* **Parallelism**: Each partition is typically processed by a single CPU core. More partitions allow for higher concurrency, but too many can lead to excessive overhead.
* **Verification**: We can inspect the distribution by calling `df.rdd.getNumPartitions()`.

## 4. Key Takeaways from the Lab

* **SparkSession Initialization**: Understanding `SparkSession` as the unified entry point for Spark applications.
* **Data Distribution**: Verifying how raw CSV data is automatically partitioned upon ingestion.
* **Distributed Aggregation**: Observing how Spark performs a `groupBy` count across all partitions and aggregates the final result in the Driver.
