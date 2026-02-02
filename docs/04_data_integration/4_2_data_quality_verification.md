# 4_1_5 Data Quality Verification (DQV)

## 1. Goal of DQV

The primary goal is to ensure that the data pipeline remains reliable after a **Join** operation. We need to identify any "data leakage" or "data explosion" before performing advanced analytics.

## 2. Key Validation Steps

* **Row Count Integrity**: Ensures that the number of rows after an **Inner Join** or **Left Join** matches expectations.
* **Null Check on Joined Columns**: Verifies if any record from the Fact table (logs) failed to find a match in the Dimension table (aircraft_master).
* **Distinct Key Count**: Validates that the join key remains unique in the dimension table to prevent unintended Row Explosion.

## 3. Implementation Patterns

* **Left Anti Join for Diagnostics**: A powerful pattern to extract only the rows that failed to join, allowing for rapid root cause analysis of missing master data.
* **Count Comparison**: Programmatic assertions to flag discrepancies in dataset size.

## 4. Why This Matters

Performing Window Functions on poor-quality data leads to "Garbage In, Garbage Out." Ensuring 100% mapping rate at this stage saves hours of debugging in production environments.
