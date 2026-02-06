# 6_1 Project Orchestration: From Scripts to System

## 1. Purpose

This chapter implements a comprehensive **Project Refactoring**. By establishing a central Orchestrator and a shared Utility layer, the project addresses the issues of high hard-coding ratios and redundant code found in earlier scripts, while successfully retaining the learning history from Chapters 1-5.

## 2. Architectural Components

* **Entry Point (`run_job.py`)**:
  * Utilizes the `argparse` module to capture command-line arguments, allowing dynamic specification of Jobs and computational parameters.
  * Implements a **Reflection Mechanism**: Uses `importlib` to dynamically load any module under the `src/` directory, significantly increasing extensibility.
* **Utility Layer (`src/utils/spark_helper.py`)**:
  * **Factory Pattern**: Establishes the `get_spark_session()` factory method to standardize Spark configurations (e.g., prime number partitions `13` and disabling broadcast joins).
  * **NamedTuple Implementation**: Uses `collections.namedtuple` to define the `DbConfig` object, ensuring immutability and readability of database connection properties.
* **Configuration Layer (`src/config/settings.py`)**:
  * Establishes global settings to store static constants like `app_name` and default paths, reducing coupling between modules.

## 3. Parameter Injection Strategy

To solve the challenge of "dynamically changing parameters without modifying legacy Job code," this project adopts an **Environmental Injection** strategy:

1. `run_job.py` parses `--partitions` and `--salt` from the CLI.
2. The values are injected into the system environment via `os.environ`.
3. `spark_helper.py` reads these environment variables during initialization, achieving "configure once, apply globally."

## 4. Project Structure

```text
.
├── run_job.py               # Central Commander (argparse + importlib)
├── src/
│   ├── __init__.py          # Package marker
│   ├── config/              # Global settings & constants
│   ├── jobs/                # Practice logs (Chapters 1-5)
│   ├── pipeline/            # Production-grade ETL workflows
│   └── utils/               # Shared helpers (NamedTuple & Factory)
└── data/                    # Local data lake (CSV/Raw)
```

## 5. Execution Interface

Developers can now precisely control resource allocation via Docker without modifying source code:

```bash
docker-compose exec spark-app python3 run_job.py \
  --job pipeline.maintenance_daily_ingest \
  --partitions 17 \
  --salt 24
```
