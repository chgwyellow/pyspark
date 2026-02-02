import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os


def generate_data(num_rows=1000):

    start_date = datetime(2026, 1, 1)
    valid_tail_numbers = [
        "B-58201",
        "B-58202",
        "B-58301",
        "B-58302",
        "B-58501",
        "B-58502",
    ]

    data = {
        "log_id": range(1, num_rows + 1),
        "device_id": [np.random.choice(valid_tail_numbers) for _ in range(num_rows)],
        "timestamp": [
            start_date + timedelta(minutes=np.random.randint(0, 10000))
            for _ in range(num_rows)
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
        "✅ Raw data generated at data/raw/equipment_logs.csv using valid tail numbers."
    )


if __name__ == "__main__":
    generate_data()
