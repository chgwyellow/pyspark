# 7_4 Integrating SparkSQL into Data Pipelines

## 1. Objective

To transition from a pure PySpark DataFrame API approach to a **Hybrid Pipeline** (Python + SQL). This leverages Python for workflow orchestration and SQL for complex business logic, mirroring the **Palantir Foundry Code Repositories** standard.

## 2. The Hybrid Workflow Pattern

In a professional pipeline, the sequence usually follows:

1. **Ingestion (Python)**: Read data from various sources (JDBC, CSV, etc.).
2. **View Registration (Bridge)**: Use `createOrReplaceTempView()` to expose DataFrames to the SQL engine.
3. **Transformation (SQL)**: Execute complex joins, CASE WHEN logic, and Window Functions via `spark.sql()`.
4. **Validation & Persistence (Python)**: Apply DQ checks on the resulting DataFrame and write to the target sink (PostgreSQL).

## 3. SQL-Based Data Quality (DQ) Checks

Instead of using `.filter()` or `.count()`, we can use SQL to define "Expectations":

* **Null Check**: `SELECT count(*) FROM view WHERE col IS NULL`
* **Range Check**: `SELECT count(*) FROM view WHERE value < 0`
If the SQL result returns a value > 0, the pipeline should `sys.exit(1)`.

## 4. Performance Considerations: SQL vs. API

* **Catalyst Optimizer**: Both SQL and DataFrame API use the same optimizer; performance is identical for most operations.
* **Readability**: SQL is superior for complex joins and multi-condition CASE statements, reducing "method chaining" fatigue.
* **Debugging**: You can easily copy-paste the SQL string from your Python code into a SQL editor (like DBeaver) for instant debugging.

## 5. Implementation Goal

We will create the `maintenance_daily_ingest_sql.py` to:

* Replace PySpark transformations with a single, comprehensive SQL block.
* Implement DQ checks using SQL aggregation.
* Maintain the robust orchestration and error handling established in Chapter 6.
