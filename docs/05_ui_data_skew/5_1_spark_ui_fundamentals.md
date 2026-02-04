# 5_1 Understanding Spark UI: The Flight Recorder of Your Jobs

## 1. What is Spark UI?

Spark UI is a web-based monitoring tool that provides real-time and historical insights into your Spark application's execution. By default, it runs on port **4040**.

**Access URL**: `http://localhost:4040` (when running locally or with port mapping)

## 2. Core Hierarchy: Job > Stage > Task

Understanding this hierarchy is crucial for debugging performance issues:

* **Job**: Triggered by an "Action" (e.g., `.show()`, `.write()`, `.count()`). One action = One job.
* **Stage**: A set of tasks that can be performed in parallel without moving data across the network. A new stage is created whenever a **Shuffle** (e.g., `groupBy`, `join`) occurs.
* **Task**: The smallest unit of work, executed by a single thread on an Executor. The number of tasks equals the number of partitions.

## 3. Key Tabs to Monitor

When Spark is running, you can access the following tabs:

### 3.1 Jobs Tab

* **Purpose**: Summary of all completed and active jobs
* **What to check**:
  * Total duration of each job
  * Number of stages per job
  * Success/failure status
  * Which action triggered the job (e.g., `show at 4_6_time_mastery.py:50`)

### 3.2 Stages Tab

* **Purpose**: Detailed metrics for each stage
* **What to check**:
  * **Shuffle Read/Write**: Amount of data transferred across the network
  * **Executor Computing Time**: Actual CPU time spent on computation
  * **Task Duration**: Min, median, max task execution time (to identify skew)
  * **Input/Output Size**: Data read from source or written to destination
* **Key Insight**: If shuffle read/write is large, consider optimizing joins or using broadcast

### 3.3 Storage Tab

* **Purpose**: Shows RDDs or DataFrames that have been cached/persisted in memory
* **What to check**:
  * Memory used vs. available
  * Fraction of data cached (should be 100% for optimal performance)
  * Storage level (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
* **Note**: This tab is empty unless you explicitly use `.cache()` or `.persist()`

### 3.4 Environment Tab

* **Purpose**: Displays all Spark configuration settings
* **What to check**:
  * `spark.executor.memory`: Memory allocated to each executor
  * `spark.sql.shuffle.partitions`: Number of partitions for shuffle operations (default: 200)
  * `spark.sql.autoBroadcastJoinThreshold`: Max size for broadcast joins (default: 10MB)

### 3.5 Executors Tab

* **Purpose**: Monitors resource usage for each executor in the cluster
* **What to check**:
  * **Memory Usage**: How much heap memory is being used
  * **Disk Used**: If executors are spilling data to disk (bad sign!)
  * **Active Tasks**: Number of tasks currently running on each executor
  * **Failed Tasks**: Indicates potential executor failures or data issues

### 3.6 SQL Tab (Most Important!)

* **Purpose**: Visualizes the **Physical Execution Plan** for DataFrame/SQL queries
* **What to check**:
  * **Predicate Pushdown**: Verify filters are pushed to the data source
  * **Broadcast Join**: Confirm small tables are broadcasted (look for `BroadcastHashJoin`)
  * **Scan Type**: Check if Parquet column pruning is happening
  * **Exchange (Shuffle)**: Identify where data is being shuffled across the network
* **Key Insight**: This is where you verify if your optimizations (broadcast, predicate pushdown) actually worked!

## 4. Why it matters for Performance?

Without Spark UI, you are "flying blind." It allows you to see:

* **Data Skew**: If one task takes 10 minutes while others take 10 seconds
* **Spill to Disk**: If your executors run out of RAM and start writing temporary data to slow disks
* **Shuffle Bottlenecks**: Identify stages with excessive network I/O
* **Optimization Verification**: Confirm that broadcast joins and predicate pushdown are actually happening

## 5. How to Access Spark UI

### During Application Execution

1. Start your Spark application
2. Open browser and navigate to `http://localhost:4040`
3. Keep the application running (use `input()` to pause execution)
4. Explore the tabs while the app is active

### After Application Completes

* By default, Spark UI closes when the application stops
* To view historical data, you need to enable **Spark History Server** (port 18080)

## 6. Pro Tips

* **Use SQL Tab First**: Always check the physical plan to understand what Spark is actually doing
* **Monitor Shuffle**: Large shuffle operations are the #1 performance killer
* **Check Task Duration Distribution**: If max >> median, you have data skew
* **Watch Memory Usage**: If "Spill to Disk" is non-zero, increase executor memory
* **Verify Broadcast**: Small dimension tables should show `BroadcastHashJoin` in SQL tab
