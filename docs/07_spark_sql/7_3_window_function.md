# 7_3 Window Functions in SQL: Advanced Analytical Patterns

## 1. Objective

To leverage SQL Window Functions for performing complex cross-row calculations without collapsing the dataset (unlike `GROUP BY`). This is essential for time-series analysis and ranking in **Palantir Foundry**.

## 2. Core Concept: The `OVER` Clause

Window functions operate on a "window" of rows defined by:

* **PARTITION BY**: Divides rows into logical groups (e.g., group by `tail_number`).
* **ORDER BY**: Defines the sequence within each partition (e.g., sort by `event_timestamp`).
* **Frame Clause**: (Optional) Further restricts rows within the partition (e.g., `ROWS BETWEEN 3 PRECEDING AND CURRENT ROW`).

## 3. Essential Ranking Functions

| Function | Behavior | Use Case |
| :--- | :--- | :--- |
| **`ROW_NUMBER()`** | Sequential index (1, 2, 3, 4) | Finding the exact "latest" or "first" record. |
| **`RANK()`** | Rank with gaps (1, 2, 2, 4) | Identifying top performers where ties share a rank. |
| **`DENSE_RANK()`** | Rank without gaps (1, 2, 2, 3) | Grouping records into tiers based on values. |

## 4. Analytical Functions for Trends

* **`LAG(col, n)`**: Accesses data from `n` rows before (e.g., "What was the previous engine temperature?").
* **`LEAD(col, n)`**: Accesses data from `n` rows after.
* **`AVG() OVER (...)`**: Calculates moving averages for trend analysis.

## 5. Implementation Scenario: Flight History Analysis

In this module, we will:

1. Rank flight logs for each aircraft to identify the most recent maintenance event.
2. Calculate the cumulative flight hours per aircraft using a running total.
3. Compare current flight duration with the previous flight using `LAG()`.

## 6. Summary

Window functions transform SQL from a simple query tool into a powerful analytical engine, enabling sophisticated insights into sequential and historical aviation data.
