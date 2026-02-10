# 7_2 Complex Logic in SQL: Beyond Basic Queries

## 1. Objective

To master the translation of complex PySpark DataFrame operations (like conditional logic and date manipulation) into high-performance, readable SparkSQL scripts. This is the primary development pattern used in **Palantir Foundry Code Repositories**.

## 2. Key SQL Syntax vs. PySpark Equivalents

| Feature | PySpark (`pyspark.sql.functions`) | SparkSQL |
| :--- | :--- | :--- |
| **Conditional Logic** | `F.when(cond, val).otherwise(alt)` | `CASE WHEN cond THEN val ELSE alt END` |
| **String Concatenation** | `F.concat(col1, F.lit("_"), col2)` | `concat(col1, '_', col2)` |
| **Date Arithmetic** | `F.datediff(end, start)` | `datediff(end, start)` |
| **Type Casting** | `col.cast("int")` | `CAST(col AS INT)` |
| **Null Handling** | `F.coalesce(col1, col2)` | `coalesce(col1, col2)` |

## 3. Structural Organization: CTEs (Common Table Expressions)

While subqueries can be nested, **CTEs (`WITH` clause)** are preferred in enterprise pipelines for:

* **Readability**: Breaks down massive queries into logical "steps" (similar to naming variables in Python).
* **Maintainability**: Easier to debug individual sections of the data transformation.

## 4. Implementation Scenario: Aviation Fleet Health

In this module, we simulate a fleet maintenance analysis:

1. **Stage 1 (CTE)**: Calculate aircraft age and categorize maintenance status based on flight hours.
2. **Stage 2 (Main Query)**: Aggregate metrics by status to provide a high-level summary for maintenance planning.

## 5. Summary

Transitioning to SQL for complex logic reduces code verbosity and leverages Spark's **Catalyst Optimizer** effectively, ensuring that the logic is both human-readable and machine-efficient.
