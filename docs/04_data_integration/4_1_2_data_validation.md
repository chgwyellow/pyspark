# 4_2 Data Quality and Validation in ETL

## 1. Why Validate After Joining?

Joining a large fact table with a dimension table can introduce data quality issues:

* **Data Explosion**: If the dimension table has duplicate keys, a single row in the fact table may be duplicated multiple times.
* **Orphan Records**: If keys exist in the fact table but not in the dimension table, a `left join` will result in `null` values for enriched columns.

## 2. Key Validation Metrics

* **Row Count Integrity**: Comparing `count()` before and after the join to ensure no unintended duplication.
* **Null Percentage**: Monitoring the ratio of nulls in critical columns after enrichment.
* **Schema Consistency**: Ensuring the final data types match the target database's requirements.

## 3. Automated Quality Gates

In production, we use **Data Quality Gates** to stop the pipeline if critical errors are found:

* **Hard Fail**: Stop the job if a primary key is null.
* **Soft Warning**: Log an alert if data distribution shifts unexpectedly.

## 4. Best Practices

* Always perform a `distinct().count()` on join keys of the dimension table before joining.
* Use `left_anti` joins as a diagnostic tool to identify missing master data.
