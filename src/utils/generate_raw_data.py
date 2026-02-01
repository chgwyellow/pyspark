import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def generate_data(num_rows=1000):
    start_date = datetime(2026, 1, 1)
    data = {
        "log_id": range(1, num_rows + 1),
        "device_id": [f"DEV-{np.random.randint(100, 110)}" for _ in range(num_rows)],
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
    df.to_csv("data/raw/equipment_logs.csv", index=False)
    print("✅ Raw data generated at data/equipment_logs.csv")


if __name__ == "__main__":
    import os

    os.makedirs("data/row", exist_ok=True)
    generate_data()
