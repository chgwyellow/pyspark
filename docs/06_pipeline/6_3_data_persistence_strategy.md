# 6_3 Data Persistence: Loading into Gold Layer

## 1. Objective

To persist high-quality, enriched aviation data into PostgreSQL, transforming it from a transient Spark dataset into a permanent organizational asset.

## 2. Table Naming Strategy: Type Prefixing

To ensure data traceability and clarity in the Data Warehouse, we adopt the **Type Prefixing** convention:

* **Prefix Logic**:
  * `stg_`: Raw/Staging data (minimal processing).
  * `fact_`: Final business facts enriched with dimensions and validated by DQ checks.
* **Target Table**: `fact_aviation_maintenance_gold`
* **Benefit**: Allows analysts to immediately identify the data's reliability and position in the Medallion Architecture (Bronze/Silver/Gold).

## 3. Error Handling: `raise` vs. `sys.exit(1)`

The pipeline manages failures based on the nature of the error:

* **`raise ValueError/Exception`**:
  * **When**: Used for **Data Quality (DQ) failures** (e.g., row count mismatch).
  * **Why**: It provides a full Traceback (stack trace), allowing engineers to pinpoint the exact line of logic failure during debugging.
* **`sys.exit(1)`**:
  * **When**: Used for **System/Orchestration failures** or after a clean error message has been printed.
  * **Why**: It signals a non-zero exit status to the OS/Orchestrator without cluttering the logs with code-level stack traces.

## 4. Persistence Strategy: Overwrite Mode

* **Decision**: Utilized `SaveMode.Overwrite`.
* **Rationale**:
  * Ensures an **Idempotent** process (the result is the same regardless of how many times it runs).
  * Automatically synchronizes the database schema with the Spark DataFrame structure.
  * Simplifies development by replacing outdated experimental tables with fresh "Gold" records.

## 5. Summary of Final Workflow

1. **Extract**: Ingest logs (CSV) and aircraft dimensions (JDBC).
2. **Transform**: Apply Salting for skew mitigation.
3. **Validate**: Execute DQ Checks (Raise exception on failure).
4. **Load**: JDBC Overwrite to a prefixed Gold Table.
5. **Clean up**: Explicitly stop the Spark session to release cluster resources.
