# 5_2 Supplement: Spark Memory and Disk Spill

## 1. Why Skew Leads to Spill

When a single partition (e.g., the one for B-58201) becomes too large to fit into the allocated **Execution Memory**, Spark must move the excess data to the local disk to prevent an OOM (Out of Memory) crash.

## 2. The Cost of Spilling

* **Spill (Memory)**: The size of data in its uncompressed, deserialized form in RAM.
* **Spill (Disk)**: The size of data after it has been compressed and written to disk.
* **Performance Impact**: Disk spill is the "Silent Killer." It can slow down a Stage by 10x-50x.

## 3. Identification

In the **Stages** detail page of Spark UI, look for columns:

* **Spill (Memory)**
* **Spill (Disk)**
If these have values > 0, you have a severe data skew or insufficient memory.
