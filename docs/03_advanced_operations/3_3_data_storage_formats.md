# 3_3 Data Storage Formats: CSV vs. Parquet

## 1. The Row-Based Limitation (CSV)

* **Storage**: CSV stores data row by row. Even if you only need one column, Spark must read the entire file.
* **Metadata**: No schema information is stored. Every read requires `inferSchema` or an explicit `StructType`.
* **Compression**: Poor compression ratio compared to binary formats.

## 2. The Columnar Standard (Parquet)

Apache Parquet is the industry standard for big data storage.

* **Columnar Storage**: Data is grouped by columns. If you only select `tail_number`, Spark only reads the data for that specific column.
* **Compression**: Optimized for each data type (e.g., Run-Length Encoding for strings), significantly reducing storage costs.
* **Metadata Insight**: Stores min/max values and null counts for each data block.

## 3. Predicate Pushdown (The DE Magic)

This is the most critical concept for performance.

* **Mechanism**: When you apply a `.filter()`, Spark "pushes" the filter down to the storage layer.
* **Result**: Because Parquet files contain metadata, Spark can skip reading entire files or blocks if it knows they don't contain the data you are looking for.

## 4. Key Takeaways

* **CSV** is for data exchange and human readability.
* **Parquet** is for high-performance distributed computing and long-term storage (The "Gold" layer in Medallion Architecture).
