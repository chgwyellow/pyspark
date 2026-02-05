import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_skewed_data(total_rows=5000000):
    """
    Generate a large-scale skewed dataset to observe distributed computing
    performance and data skew in Spark UI.

    Memory Estimation (for Docker with 7.614GB limit):
    - 5M rows: ~305 MB CSV, ~4.3 GB in Spark (Safe ✅)
    - 8M rows: ~488 MB CSV, ~6.8 GB in Spark (Risky ⚠️)
    - 10M rows: ~610 MB CSV, ~8.5 GB in Spark (OOM Risk ❌)

    Args:
        total_rows: Number of rows to generate (default: 5,000,000)
    """
    start_date = datetime(2025, 1, 1)

    # Define aircraft identifiers
    skewed_tail = "B-58201"
    other_tails = ["B-58202", "B-58301", "B-58302", "B-58501", "B-58502"]

    # Skew ratio: 90% logs belong to one aircraft (to demonstrate data skew)
    skew_count = int(total_rows * 0.9)
    other_count = total_rows - skew_count

    # Estimate memory usage
    estimated_csv_mb = (total_rows * 61) / (1024 * 1024)
    estimated_spark_gb = (estimated_csv_mb * 7) / 1024

    print(f"⏳ Generating {total_rows:,} rows of data...")
    print(f"💾 Estimated CSV size: ~{estimated_csv_mb:.1f} MB")
    print(f"🔥 Estimated Spark memory: ~{estimated_spark_gb:.1f} GB")

    # Vectorized generation for performance
    device_ids = np.concatenate(
        [np.full(skew_count, skewed_tail), np.random.choice(other_tails, other_count)]
    )
    np.random.shuffle(device_ids)

    data = {
        "log_id": np.arange(1, total_rows + 1),
        "device_id": device_ids,
        # Generate timestamps efficiently
        "timestamp": [
            start_date + timedelta(seconds=np.random.randint(0, 10000000))
            for _ in range(total_rows)
        ],
        "status": np.random.choice(
            ["Running", "Idle", "Error", "Maintenance"], total_rows
        ),
        "value": np.random.uniform(10.0, 100.0, size=total_rows).round(2),
    }

    df = pd.DataFrame(data)

    os.makedirs("data/raw", exist_ok=True)
    output_path = "data/raw/equipment_logs_skewed.csv"

    print("💿 Writing to disk...")
    df.to_csv(output_path, index=False)

    print(f"\n✅ SKEWED large-scale data generated at: {output_path}")
    print("📊 Data Skew Summary:")
    print(f"   - {skewed_tail}: {skew_count:,} rows (90%)")
    print(f"   - Others: {other_count:,} rows (10%)")
    print("\n⚠️  This skew will cause performance issues in Spark!")


if __name__ == "__main__":
    # Safe default for Docker with 7.614GB memory limit
    generate_skewed_data(5000000)
