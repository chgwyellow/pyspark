# 3_2 Data Integration: Join Strategies and Multi-table Operations

## 1. Why Joins are Expensive in Distributed Systems

Unlike a local SQL database, joining two distributed DataFrames in Spark often requires a **Shuffle**. This means data from different nodes must be transferred across the network to the same executor to find matching keys.

## 2. Common Join Types

PySpark supports all standard SQL join types:

* **Inner Join**: Returns rows with matching keys in both DataFrames.
* **Left Join (Outer)**: Returns all rows from the left, plus matches from the right. (Most common for enriching fact tables).
* **Anti Join**: Returns rows from the left that have **no match** on the right. Extremely useful for data validation and identifying missing records.

## 3. Join Syntax and Best Practices

* **Join Expression**: Always use `F.col()` or explicit column references to avoid "ambiguous column" errors when both tables have columns with the same name.
* **Filtering before Joining**: Reducing the size of DataFrames *before* the join operation can significantly minimize shuffle overhead.
* **Handling Duplicates**: Ensure your join keys are unique in the dimension table to avoid unintended "Cartesian Explosion" (where 1 row unexpectedly becomes many).

## 4. Broadcast Join (The Performance Secret)

When joining a large table with a small "lookup" table, Spark can broadcast the small table to all executors, eliminating the need for a shuffle. This is triggered via `F.broadcast()`.
