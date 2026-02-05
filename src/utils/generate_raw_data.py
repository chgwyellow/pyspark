import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def generate_data(num_rows=5000):
    """
    Generate equipment log data for STARLUX Airlines complete fleet.

    Args:
        num_rows: Number of log entries to generate (default: 5,000)
    """
    start_date = datetime(2025, 1, 1)

    valid_tail_numbers = [
        # Airbus A321neo (13 aircraft)
        "B-58201",
        "B-58202",
        "B-58203",
        "B-58204",
        "B-58205",
        "B-58206",
        "B-58207",
        "B-58208",
        "B-58209",
        "B-58210",
        "B-58211",
        "B-58212",
        "B-58213",
        # Airbus A330-900 (6 aircraft)
        "B-58301",
        "B-58302",
        "B-58303",
        "B-58304",
        "B-58305",
        "B-58306",
        # Airbus A350-900 (10 aircraft)
        "B-58501",
        "B-58502",
        "B-58503",
        "B-58504",
        "B-58505",
        "B-58506",
        "B-58507",
        "B-58508",
        "B-58509",
        "B-58510",
        # Airbus A350-1000 (1 aircraft)
        "B-58551",
    ]

    data = {
        "log_id": range(1, num_rows + 1),
        "device_id": [np.random.choice(valid_tail_numbers) for _ in range(num_rows)],
        "timestamp": [
            start_date + timedelta(minutes=i * 5 + np.random.randint(0, 3))
            for i in range(num_rows)
        ],
        "status": np.random.choice(
            ["Running", "Idle", "Error", "Maintenance"], num_rows
        ),
        "value": np.random.uniform(10.0, 100.0, size=num_rows).round(2),
    }

    df = pd.DataFrame(data)

    os.makedirs("data/raw", exist_ok=True)

    df.to_csv("data/raw/equipment_logs.csv", index=False)
    print(
        f"✅ Raw data generated: {num_rows} rows across {len(valid_tail_numbers)} aircraft"
    )
    print("File: data/raw/equipment_logs.csv")


if __name__ == "__main__":
    generate_data()
