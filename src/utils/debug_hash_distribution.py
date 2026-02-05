import pandas as pd


def get_simulated_spark_partition(key, num_partitions):
    """
    Simulate Spark's HashPartitioner logic: abs(hash(key)) % num_partitions
    """
    return abs(hash(key)) % num_partitions


def analyze_hash_distribution(num_partitions=12):
    """
    Analyze how 31 aircraft identifiers are distributed across Spark partitions.
    """
    # 1. Generate the list of 31 aircraft tails based on dim_aircraft logic
    # A321neo: 201-213, A330-900: 301-306, A350-900: 501-510, A350-1000: 551
    all_tails = (
        [f"B-582{i:02d}" for i in range(1, 14)]
        + [f"B-583{i:02d}" for i in range(1, 7)]
        + [f"B-585{i:02d}" for i in range(1, 11)]
        + ["B-58551"]
    )

    # 2. Calculate the target partition for each key
    results = []
    for tail in all_tails:
        partition_id = get_simulated_spark_partition(tail, num_partitions)
        results.append({"tail_number": tail, "partition_id": partition_id})

    df = pd.DataFrame(results)

    # 3. Group by partition to see the clustering effect
    summary = df.groupby("partition_id")["tail_number"].apply(list).reset_index()
    summary["key_count"] = summary["tail_number"].apply(len)

    print(f"--- Spark Hash Distribution Analysis (Partitions: {num_partitions}) ---")

    for _, row in summary.iterrows():
        # Identify if the notorious skewed key is in this partition
        skew_warning = (
            "!!! SKEW ALERT: B-58201 is here !!!"
            if "B-58201" in row["tail_number"]
            else ""
        )
        print(
            f"Partition {row['partition_id']:2d}: {row['key_count']:2d} keys | {row['tail_number']} {skew_warning}"
        )

    # 4. Identify idle partitions that will result in skipped tasks
    used_partitions = set(summary["partition_id"])
    all_partitions = set(range(num_partitions))
    idle_partitions = all_partitions - used_partitions

    print("-" * 60)
    print(f"Total Unique Keys: {len(all_tails)}")
    print(f"Active Tasks (Partitions with data): {len(used_partitions)}")
    print(f"Idle Tasks (Partitions with NO data): {sorted(list(idle_partitions))}")
    print("-" * 60)


if __name__ == "__main__":
    # Test with your current setting
    analyze_hash_distribution(num_partitions=12)
