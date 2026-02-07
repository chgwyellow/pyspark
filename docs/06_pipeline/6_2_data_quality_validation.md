# 6_2 Data Quality Validation: Defensive Engineering

## 1. Overview

In this stage, the pipeline transitions from a simple data transformation script to a **Reliable Data Pipeline**. We implemented "Quality Gates" to ensure data integrity before any analytical or loading processes.

## 2. Implemented Validation Gates

* **Row Count Integrity Check**:
  * Logic: Compares the total records from the raw source against the final joined results.
  * Goal: To detect data loss during the Inner Join (e.g., missing aircraft data in the dimension table).
* **Critical Null Value Scanning**:
  * Logic: Specifically filters for `NULL` values in the `aircraft_type` column post-join.
  * Goal: To ensure every maintenance log is successfully enriched with metadata, preventing "orphan records" in the final report.

## 3. Error Handling Strategy: Fail-Fast Mechanism

The pipeline adopts the **Fail-Fast** principle:

* **Implementation**: Uses `sys.exit(1)` or `raise ValueError` to immediately terminate execution upon DQ failure.
* **Impact**:
  * Prevents **Data Contamination** in downstream databases.
  * Provides an explicit failure signal to the orchestrator (`run_job.py`) or external schedulers like Airflow.

## 4. Performance Benchmarking

A localized timer was integrated within the `run_pipeline` function to isolate the **actual computational duration** from human interaction time (e.g., `input()` prompts), providing accurate performance metrics for the Salting technique.

## 5. Summary

Data Quality is not an option but a requirement for aviation data. By implementing these checks, the pipeline ensures that processed information is both **Complete** and **Accurate**.
