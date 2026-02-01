# 2_3 Handling Complex Types: Arrays, Structs, and JSON

## 1. What are Complex Types?

In traditional RDBMS, data is usually flat. However, modern big data often contains nested structures:

* **StructType**: A collection of fields, similar to a "nested table" or a JSON object.
* **ArrayType**: A list of values within a single column.
* **MapType**: Key-value pairs.

## 2. Key Operations

* **explode()**: A powerful function that takes an array and turns each element into a new row. (Similar to `unnest` in some SQL dialects).
* **struct()**: Groups multiple columns into a single nested structure.
* **dot notation**: Used to access fields inside a Struct, e.g., `df.select("device.id")`.

## 3. Dealing with JSON

Spark provides `from_json()` to parse JSON strings into structured columns based on a pre-defined schema. This is critical for processing logs stored as strings in message queues (like Kafka).

## 4. Best Practices

* **Avoid over-nesting**: While Spark handles nested data well, extremely deep structures can make queries harder to read and optimize.
* **Flattening**: It is often beneficial to flatten complex structures into a tabular format at the end of an ETL pipeline for BI tool compatibility.
