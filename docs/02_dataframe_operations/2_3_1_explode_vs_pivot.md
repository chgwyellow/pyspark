# 2_3 Supplement: Explode vs. Pivot

## 1. Explode (Vertical Expansion)

* **Goal**: To flatten nested collections (Arrays or Maps) into individual rows.
* **Input**: A single row containing a list or array.
* **Output**: Multiple rows (The row count increases).
* **Use Case**: Processing IoT sensor bursts where multiple readings are stored in a single timestamp.

## 2. Pivot (Horizontal Expansion)

* **Goal**: To rotate unique values from one column into multiple separate columns.
* **Input**: Multiple rows with categorical values (e.g., Status: "Running", "Error").
* **Output**: A single summary row per grouping key (The row count decreases).
* **Use Case**: Generating summary reports, such as a "Daily Status Count" report where statuses are columns.

## 3. Key Differences

| Feature | Explode | Pivot |
| :--- | :--- | :--- |
| **Direction** | Vertical (Row-wise) | Horizontal (Column-wise) |
| **Data Change** | Normalizes nested data | Aggregates/Summarizes data |
| **Common Function** | `F.explode()` | `df.groupBy().pivot()` |
