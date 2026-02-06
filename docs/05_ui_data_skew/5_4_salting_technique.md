# 5_4 Salting Technique: Breaking the Skew

## 1. The Core Concept

When a single Key (e.g., 'B-58201') is too large, a standard Shuffle Join will map all its records to ONE partition. "Salting" adds a random suffix to the keys, forcing them to be distributed across multiple partitions.

## 2. Implementation Logic

* **Left Table (Skewed Logs)**: Append a random integer (e.g., 0-9) to each `device_id`.
  * 'B-58201' -> 'B-58201_0', 'B-58201_3', etc.
* **Right Table (Master Data)**: Explode each row by N (Salt Factor).
  * 'B-58201' row becomes 10 rows: 'B-58201_0' ... 'B-58201_9'.
* **Join**: Perform join on these salted keys. The 4.5M rows are now handled by 10-ish different tasks instead of one.

## 3. Trade-offs

* **Benefit**: Eliminates the "Long Tail" task and prevents Disk Spill.
* **Cost**: The Right Table (Dimension Table) grows N times in memory. Only suitable when the dimension table is small (e.g., aircraft master data).

## 4. Troubleshooting: Why Salt 12 vs Partition 12 Fails?

Even when aligning Salt Factor with Partition Count, data skew might persist due to **Hash Collisions**.

* **Observation**: In a 12-partition environment, 'B-58201_salted' keys clustered in specific partitions (e.g., Index 9 received 1.16M rows).
* **Root Cause**: The deterministic nature of $hash(key) \pmod{12}$ causes multiple salted keys to map to the same bucket.
* **Solution**: Use a **prime number** for `shuffle.partitions` (e.g., 13 or 17) or increase the **Salt Factor** significantly to improve the statistical distribution across available cores.

## Salting Optimization: The Final Verdict

### 1. Why Prime Partitions (13) worked better than 12?

Using a **prime number** for shuffle partitions breaks the cyclic nature of string hashes. It prevents multiple "salted" keys from hitting the same hash bucket, leading to a much smoother distribution.

### 2. Why SALT_FACTOR = 24?

By setting the salt factor to roughly **2x the number of cores**, we provide enough randomness to ensure that even if some salt keys collide, the overall impact is minimized. Each core now shares the burden of the 4.5M rows almost equally.

### 3. Results Comparison

| Strategy | Max Task Duration | Max Shuffle Read Records | System Status |
| :--- | :--- | :--- | :--- |
| **No Salting (5_3)** | 28s | 4,500,004 | Massive Spill, 11 Cores Idle |
| **Salt 12 / Part 12** | 22s | 1,165,645 | Heavy Collision on Index 9 |
| **Salt 24 / Part 13** | **20s** | **785,121** | **Balanced Load, High Utilization** |
