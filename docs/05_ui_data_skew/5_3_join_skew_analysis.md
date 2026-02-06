# 5_3 Technical Note: Join Skew Analysis & Diagnosis

## 1. Purpose

This chapter demonstrates how data skew manifests during a **Shuffle Hash Join** between a massive, imbalanced dataset (Fact Table) and a small reference table (Dimension Table). The goal is to learn how to identify physical performance bottlenecks in the Spark UI.

## 2. Experimental Setup

* **Skewed Data Source**: `equipment_logs_skewed.csv`
  * **Total Records**: 5,000,000 rows.
  * **Skewness**: 90% of data (approx. 4.5M rows) belongs to a single aircraft: `B-58201`.
* **Dimension Table**: PostgreSQL table `dim_aircraft` (30 rows of aircraft metadata).
* **Execution Strategy**:
  * Set `spark.sql.autoBroadcastJoinThreshold` to `-1` to disable automatic broadcast joins and force a Shuffle.
  * Set `spark.sql.shuffle.partitions` to `12` to align with the executor's core count.
  * Use `rdd.count()` to trigger the Action, bypassing DataFrame's **Map-side Combine** optimization to observe raw data movement.

## 3. Diagnosis: Identifying the "Monster" in Spark UI

By analyzing the **Tasks** list in the Stage detail page, we identified the following physical symptoms of data skew:

### A. Duration Skew (Long-Tail Task)

* **Healthy Tasks**: Most tasks completed within **2 - 5 seconds**.
* **Skewed Task**: The task handling `B-58201` took **28 seconds** to complete.
* **Impact**: The entire stage was bottlenecked by this single task, leaving other cores idle.

### B. Data Volume Imbalance

* **Healthy Tasks**: Shuffle Read Records were approximately **200k - 300k**.
* **Skewed Task**: Shuffle Read Records reached **4,500,004**.
* **Shuffle Read Size**: The skewed task pulled **100.9 MiB** of data over the network, compared to ~5 MiB for healthy tasks.

### C. Memory Pressure & Spill

* **Symptoms**: The skewed task triggered **Spill (Memory)** and **Spill (Disk)**.
* **Reason**: The data for `B-58201` exceeded the assigned `Execution Memory`. Spark was forced to write temporary data to local disk (Spill), causing a 10x-50x performance hit due to Disk I/O.

## 4. Key Takeaways: RDD count() vs. DF count()

* **DataFrame count()**: Highly optimized. Spark performs local counts before shuffling, hiding the skew.
* **RDD count()**: Opaque to the optimizer. It forces a full shuffle of every single object, revealing the true cost of imbalanced data distribution.

## 5. Conclusion

Data skew is not just a statistical phenomenon; it is a **resource waste**. In our case, 11 cores spent 80% of their time waiting for 1 core to finish the skewed partition. This is the primary target for the **Salting Technique** in Chapter 5_4.
