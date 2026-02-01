# 3_1 Data Aggregation and Grouping

## 1. The GroupBy Mechanism

Grouping is the process of categorizing data based on specific columns and performing calculations on each group. In Spark's distributed environment, this involves a **Shuffle** operation where data with the same key is moved to the same executor.

## 2. Aggregation Functions

Common functions from `pyspark.sql.functions` (F) include:

* `count()`: Counts the number of rows.
* `sum()`, `avg()`, `max()`, `min()`: Basic arithmetic statistics.
* `countDistinct()`: Returns the number of unique items.
* `collect_list()` / `collect_set()`: Aggregates values into an array (list) or a unique set.

## 3. Pivot Tables

Spark allows you to rotate rows into columns using the `pivot()` method, which is extremely useful for generating summary reports.

## 4. Why Spark/Polars logic differs from SQL in Null Handling?

* **Explicit Methods**: Spark/Polars use specific methods like `.na.fill()` or `.fill_null()`. This treats data cleaning as a "step" in a pipeline.
* **SQL Approach**: In SQL, you often handle nulls during calculation using `COALESCE(col, 0)` or `CASE WHEN col IS NULL`.
* **The "Unknown" Logic**: In SQL, `NULL` is treated strictly by Three-Valued Logic (True, False, Unknown). In Spark, while the logic remains, the API provides "API-first" tools to eliminate nulls *before* the computation starts.
