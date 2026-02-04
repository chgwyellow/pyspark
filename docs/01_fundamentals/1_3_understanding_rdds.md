# 5_1_5 Core Concept: What are RDDs?

## 1. The Definition

* **Resilient (彈性)**: If a node fails, Spark can reconstruct the lost data using a "Lineage" (record of operations).
* **Distributed (分散式)**: Data is partitioned across multiple nodes in the cluster.
* **Dataset (資料集)**: A collection of objects.

## 2. RDD vs. DataFrame (Why we use DF now)

* **RDD**: Row-by-row objects. Spark doesn't know what's inside the data (Schema-less to the engine), so it can't optimize the execution plan. It's like a box where Spark only sees "objects."
* **DataFrame**: RDD + Schema. Because Spark knows the columns and types, it can use the **Catalyst Optimizer** to make your queries faster (like Predicate Pushdown).

## 3. Why learn about RDD?

Even though we write DataFrame code, the **Spark UI** often shows RDD operations. Understanding RDD helps you debug:

* **Immutability**: RDDs never change; every operation creates a new RDD.
* **Lazy Evaluation**: RDDs don't compute until you call an "Action" (like `.count()` or `.show()`).
