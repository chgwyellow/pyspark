<div align="center">

# Aviation Data Analytics with PySpark

![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.37+-CD792C?style=flat&logo=polars&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-3.0+-150458?style=flat&logo=pandas&logoColor=white)
![PyArrow](https://img.shields.io/badge/PyArrow-23.0+-4B8BBE?style=flat)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=flat&logo=docker&logoColor=white)
![Poetry](https://img.shields.io/badge/Poetry-Managed-60A5FA?style=flat&logo=poetry&logoColor=white)
![Java](https://img.shields.io/badge/Java-11-007396?style=flat&logo=openjdk&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat)
![Updated](https://img.shields.io/badge/Updated-Feb%202026-brightgreen?style=flat)

**A comprehensive learning project for mastering Apache Spark and distributed data processing, designed for aviation industry data analytics applications.**

[Quick Start](#quick-start) • [Learning Path](#learning-path) • [Project Structure](#project-structure) • [Documentation](#documentation)

</div>

---

## Overview

This is a systematic PySpark learning project focused on aviation industry data analytics scenarios. Learn distributed data processing core concepts and best practices through real-world equipment log data.

### Learning Objectives

- Master Spark core architecture (Driver, Executor, Cluster Manager)
- Understand distributed computing partitioning mechanisms
- Proficiently use DataFrame API for data transformations
- Learn production environment best practices (Schema definition, Logging configuration)
- Build reproducible development environments with Docker

### Features

- **Complete Docker Environment**: One-command Spark development setup
- **Progressive Learning Path**: From basics to advanced, step by step
- **Practice-Oriented**: Real aviation industry data scenarios
- **Detailed Documentation**: Theory and code implementation for each topic
- **Best Practices**: Production-grade configurations including logging, schema definition, and code formatting

---

## Quick Start

### Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.11**
- **Poetry** (dependency management tool)

### Installation Steps

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd pyspark
   ```

2. **Install Python dependencies**

   ```bash
   poetry install
   ```

3. **Start Docker container**

   ```bash
   docker-compose up -d
   ```

4. **Verify environment**

   ```bash
   docker-compose exec spark-lab python3 src/jobs/check_env.py
   ```

### Running Examples

```bash
# 1.1 Spark Core Architecture
docker-compose exec spark-lab python3 src/jobs/1_1_spark_core.py

# 1.2 Data Ingestion and Schema Definition
docker-compose exec spark-lab python3 src/jobs/1_2_data_ingestion.py

# 2.1 Basic DataFrame Operations
docker-compose exec spark-lab python3 src/jobs/2_1_basic_operations.py

# 2.2 Data Cleaning: Casting and Null Handling
docker-compose exec spark-lab python3 src/jobs/2_2_casting_and_nulls.py

# 3.1 Aggregation and GroupBy
docker-compose exec spark-lab python3 src/jobs/3_1_aggregation.py

# 3.2 Multi-table Join Operations
docker-compose exec spark-lab python3 src/jobs/3_2_multi_table_join.py
```

---

## Learning Path

### Chapter 1: Spark Fundamentals

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **1.1 Core Architecture** | [`1_1_spark_core.py`](src/jobs/1_1_spark_core.py) | [`1_1_spark_core.md`](docs/1_1_spark_core.md) | Centralized Architecture, Master-Slave Model, Local Mode |
| **1.2 Data Ingestion** | [`1_2_data_ingestion.py`](src/jobs/1_2_data_ingestion.py) | [`1_2_data_ingestion.md`](docs/1_2_data_ingestion.md) | Explicit Schema, Partitioning, repartition vs coalesce |

### Chapter 2: DataFrame Operations

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **2.1 Basic Operations** | [`2_1_basic_operations.py`](src/jobs/2_1_basic_operations.py) | [`2_1_basic_operations.md`](docs/2_1_basic_operations.md) | select, filter, withColumn, Immutability |
| **2.2 Data Cleaning** | [`2_2_casting_and_nulls.py`](src/jobs/2_2_casting_and_nulls.py) | [`2_2_casting_and_nulls.md`](docs/2_2_casting_and_nulls.md) | Type Casting, Null Handling, fill, drop |

### Chapter 3: Advanced Operations

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **3.1 Aggregation** | [`3_1_aggregation.py`](src/jobs/3_1_aggregation.py) | [`3_1_aggregation_and_grouping.md`](docs/3_1_aggregation_and_grouping.md) | GroupBy, Aggregation Functions, Pivot Tables |
| **3.2 Join Strategies** | [`3_2_multi_table_join.py`](src/jobs/3_2_multi_table_join.py) | [`3_2_join_strategies.md`](docs/3_2_join_strategies.md) | Inner/Left/Anti Join, Broadcast Join, Shuffle |

---

## Project Structure

```text
pyspark/
├── conf/
│   └── log4j.properties
├── data/
│   ├── raw/
│   │   └── equipment_logs.csv
│   └── processed/
├── docs/
│   ├── 1_1_spark_core.md
│   ├── 1_2_data_ingestion.md
│   ├── 2_1_basic_operations.md
│   ├── 2_2_casting_and_nulls.md
│   ├── 3_1_aggregation_and_grouping.md
│   └── 3_2_join_strategies.md
├── src/
│   ├── jobs/
│   │   ├── 1_1_spark_core.py
│   │   ├── 1_2_data_ingestion.py
│   │   ├── 2_1_basic_operations.py
│   │   ├── 2_2_casting_and_nulls.py
│   │   ├── 3_1_aggregation.py
│   │   ├── 3_2_multi_table_join.py
│   │   └── check_env.py
│   └── utils/
│       └── generate_raw_data.py
├── tests/
├── notebook/
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Tech Stack

### Core Technologies

- **Apache Spark 3.5.0** - Distributed data processing engine
- **PySpark** - Python API for Spark
- **Python 3.11** - Programming language
- **Docker** - Containerized development environment

### Data Processing Tools

- **Polars 1.37+** - High-performance DataFrame library
- **Pandas 3.0+** - Traditional data analysis tool
- **PyArrow 23.0+** - Apache Arrow Python bindings

### Development Tools

- **Poetry** - Dependency management and packaging
- **Black** - Code formatter
- **isort** - Import sorter
- **pytest** - Testing framework

---

## Documentation

### Core Concepts

**Centralized Architecture**  
Spark uses a Master-Slave model where the Driver centrally manages and schedules all tasks.

**Local Mode**  
Simulates a distributed environment on a single machine, suitable for development and learning without requiring a real cluster.

**Explicit Schema**  
In production environments, data structures should be explicitly defined. Avoid using `inferSchema=True` to improve performance and reliability.

**Partitioning**  
Spark splits data into multiple partitions for parallel processing, which is key to achieving distributed computing.

### Best Practices

#### 1. Logging Configuration

This project uses `log4j.properties` for global logging configuration, avoiding repetitive setup in each program:

```properties
# conf/log4j.properties
log4j.rootCategory=ERROR, console
```

Enable via Docker environment variable:

```yaml
# docker-compose.yml
environment:
  - SPARK_CONF_DIR=/app/conf
```

#### 2. Schema Definition

**Recommended**: Use `StructType` to explicitly define schema

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
])
df = spark.read.csv(path, header=True, schema=schema)
```

**Avoid**: Using `inferSchema=True` (scans data twice)

---

## Development Guide

### Docker Commands

```bash
# Start container
docker-compose up -d

# Stop container
docker-compose down

# View container logs
docker-compose logs -f

# Enter container shell
docker-compose exec spark-lab bash

# Execute Python script
docker-compose exec spark-lab python3 src/jobs/<script_name>.py
```

### Local Development

```bash
# Start Poetry shell
poetry shell

# Install new package
poetry add <package-name>

# Install dev dependency
poetry add --group dev <package-name>

# Run tests
poetry run pytest
```

### Code Formatting

```bash
# Format code
poetry run black src/

# Sort imports
poetry run isort src/

# Or press Alt+Shift+F in VS Code
```

---

## Learning Resources

### Official Documentation

- [Apache Spark Official Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)

### Recommended Reading

- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)

---

## FAQ

<details>
<summary><b>Q: Why use Docker?</b></summary>

A: Docker provides a consistent development environment, ensuring all dependencies (including Java and Spark) are correctly installed, avoiding environment configuration issues.
</details>

<details>
<summary><b>Q: What's the difference between Local Mode and a real distributed cluster?</b></summary>

A: Local Mode uses multiple threads on a single machine to simulate a distributed environment, suitable for development and learning. A real cluster runs on multiple machines, suitable for processing large-scale data.
</details>

<details>
<summary><b>Q: How to adjust Spark's log level?</b></summary>

A: This project is configured to ERROR level in `conf/log4j.properties`. To adjust, modify that file and restart the container.
</details>

<details>
<summary><b>Q: Why avoid using inferSchema?</b></summary>

A: `inferSchema=True` makes Spark scan the data twice (once to infer schema, once to read data), impacting performance. In production environments, schema should be explicitly defined.
</details>

---

## License

This project is licensed under the MIT License.

---
