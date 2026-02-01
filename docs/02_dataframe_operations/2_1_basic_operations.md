# 2_1 Basic Data Operations: Select, Filter, and WithColumn

## 1. Transforming DataFrames

In PySpark, DataFrames are **Immutable**. This means when you perform an operation, you are not modifying the original DataFrame; instead, you are creating a new one with the transformation applied. This concept is crucial for Spark's fault tolerance.

## 2. Core Operations

* **select()**: Projects a set of expressions and returns a new DataFrame. It is equivalent to the `SELECT` clause in SQL.
* **filter() / where()**: Filters rows using a given condition. Both functions are identical in PySpark. It corresponds to the `WHERE` clause in SQL.
* **withColumn()**: A fundamental method used to:
  * Create a new column based on a calculation.
  * Update an existing column's value or data type.
  * Rename a column (though `withColumnRenamed` is more specific).

## 3. The Power of `pyspark.sql.functions` (F)

To perform complex logic (like string manipulation, mathematical operations, or conditional logic), we use the `functions` module.

* **Example**: `F.col("column_name")` is the safest way to reference columns, especially when dealing with multiple DataFrames or specific logic.

## 4. Column Expressions

Unlike standard Python lists, PySpark columns are **Expressions**. When you write `df.value * 2`, Spark creates a plan to multiply every element in that column across all distributed partitions simultaneously.

## 5. Best Practices

* **Avoid Chaining too many `withColumn`**: Each `withColumn` creates a new internal projection. For massive numbers of new columns, consider using a single `select()` with multiple expressions to optimize the execution plan.
* **Naming Conventions**: Keep column names consistent and avoid special characters or spaces.
