# 4_5 User Defined Functions (UDF): Power and Performance

## 1. The UDF Workflow

When you use a standard Python UDF, Spark must:

* **Serialize** the data in the JVM.
* **Transfer** it to a Python worker process (via Py4J).
* **Execute** the Python code.
* **Deserialize** the result back into the JVM.
This cross-process communication is the primary cause of slowness.

## 2. Types of UDFs

* **Standard UDF**: Processes data row-by-row. Easiest to write, slowest to run.
* **Pandas UDF (Vectorized)**: Uses Apache Arrow to transfer entire batches of data. Much faster because it leverages vectorized operations.

## 3. When to use UDFs?

UDFs should be your **Last Resort**. Always check if a built-in function (`pyspark.sql.functions`) exists first.

* **Bad**: Using a UDF for simple string concatenation or basic math.
* **Good**: Using a UDF for complex proprietary logic (e.g., custom aircraft fault code parsing) that cannot be expressed in Spark SQL.

## 4. Best Practices

* Always specify the **ReturnType** to avoid Spark having to infer it.
* Keep UDFs as simple as possible to minimize the serialization overhead.
