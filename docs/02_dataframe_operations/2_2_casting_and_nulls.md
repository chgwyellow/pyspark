# 2_2 Data Transformation: Casting and Null Handling

## 1. Type Casting (Data Conversion)

In Data Engineering, data often arrives in an incorrect format (e.g., a numeric string). **Casting** is the process of converting a column from one data type to another.

* **cast()**: The primary method to change column types.
* **Safety**: If Spark cannot cast a value (e.g., casting "Hello" to Integer), it will return `null` instead of crashing.

## 2. Handling Missing Data (Nulls)

Null values are inevitable. Spark provides the `DataFrame.na` functions to handle them effectively:

* **fill()**: Replaces null values with a specific constant (e.g., replacing null readings with `0.0`).
* **drop()**: Removes rows containing null values.
* **coalesce()**: A function (from `F`) that returns the first non-null value among given columns. (Not to be confused with the partition `coalesce`).

## 3. Best Practices for Nulls

* **Imputation**: Before dropping rows, consider if replacing nulls with a mean, median, or constant value (Imputation) makes more sense for your analysis.
* **Filtering Nulls**: Use `isNotNull()` or `isNull()` to filter rows instead of using `== null`.

## 4. Logical Comparisons with Nulls

In Spark SQL logic, `null` is not a value; it represents an "unknown" state.

* `null == null` results in `null`, not `True`.
* Always use specific null-safe operators when performing comparisons.
