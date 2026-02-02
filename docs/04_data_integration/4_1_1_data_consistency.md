# 4_1 Supplement: Data Consistency and Key Mapping

## 1. The Challenge of Referential Integrity

In a distributed ETL pipeline, data often comes from disparate sources (CSV files and Relational Databases). If the "Foreign Keys" (e.g., `device_id` in logs vs. `tail_number` in master data) do not match exactly in format and value, Join operations will fail to return expected results.

## 2. Source Alignment Strategy

The most robust way to ensure successful integration is to align mock data generators with the production schema:

* **Value Mapping**: Ensure ID formats (e.g., B-XXXXX) match the Master Data.
* **Data Types**: Verify that both sides are treated as `StringType` in Spark to avoid casting issues during Shuffle Joins.

## 3. Impact Analysis

When source data is updated, downstream artifacts must be refreshed:

1. **Raw Layer**: Regenerate CSV files.
2. **Silver Layer (Parquet)**: Re-run ingestion jobs to overwrite existing Parquet files with new keys.
3. **Validation**: Use `left_anti` joins to verify that no orphan records remain after the fix.
