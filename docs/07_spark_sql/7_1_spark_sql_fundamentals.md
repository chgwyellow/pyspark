# Chapter 7_1: SparkSQL Fundamentals & Views

SparkSQL is a Spark module for structured data processing. It allows you to query structured data inside Spark programs using SQL or the DataFrame API.

## Core Concepts

### 1. The SparkSession

In modern Spark (2.0+), the `SparkSession` is the entry point for SparkSQL. It integrates the functionality of `SQLContext` and `HiveContext`.

### 2. DataFrames vs. SQL

DataFrames and SQL share the same optimization engine (**Catalyst Optimizer**). Switching between them has zero performance overhead.

### 3. Views: The Bridge to SQL

To run SQL queries against a DataFrame, you must first register it as a **View**.

- **Temporary View**: Session-scoped. Disappears when the session ends.
- **Global Temporary View**: Shared across all sessions in the Spark application. Stored in the `global_temp` database.

## Key APIs

| Action | API Method | Description |
| :--- | :--- | :--- |
| **Register View** | `df.createOrReplaceTempView("name")` | Creates a local temporary view. |
| **Execute SQL** | `spark.sql("SELECT * FROM name")` | Executes a SQL query and returns a DataFrame. |
| **Drop View** | `spark.catalog.dropTempView("name")` | Removes the view from the catalog. |

## Why use SparkSQL?

1. **Accessibility**: Allows SQL users to leverage Spark's power without learning Python/Scala.
2. **Readability**: Complex transformations (like nested JOINs or CTEs) are often more readable in SQL.
3. **Optimization**: SQL queries are automatically optimized by the Catalyst engine for better performance.
