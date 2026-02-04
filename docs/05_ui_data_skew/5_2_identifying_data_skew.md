# 5_2 Performance Tuning: Identifying Data Skew

## 1. What is Data Skew?

Data Skew occurs when data is not distributed evenly across the partitions of a cluster. In Spark, since work is divided by partitions, a skewed partition forces one executor to work much longer than others.

## 2. Visual Symptoms in Spark UI

* **Duration Skew**: In the Summary Metrics table, the **Max** duration is significantly higher than the **Median** or **Min**.
* **Shuffle Read Size Skew**: One task reads megabytes or gigabytes of data while others read only a few kilobytes.

## 3. Common Causes

* **Join Keys**: Joining a large table on a key that has a high frequency of a single value (e.g., a "Default" ID or a very popular aircraft model).
* **GroupBy Operations**: Aggregating data on a low-cardinality column where one group is massive.

## 4. Why it kills performance?

Spark is only as fast as its slowest task. Even if you have 100 executors, if one task is skewed and takes 1 hour, your entire job will take at least 1 hour.
