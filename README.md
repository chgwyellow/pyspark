<div align="center">

# Aviation Data Analytics with PySpark

![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=flat&logo=apachespark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)
![Polars](https://img.shields.io/badge/Polars-1.37+-CD792C?style=flat&logo=polars&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-3.0+-150458?style=flat&logo=pandas&logoColor=white)
![PyArrow](https://img.shields.io/badge/PyArrow-23.0+-4B8BBE?style=flat)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-JDBC-336791?style=flat&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=flat&logo=docker&logoColor=white)
![Poetry](https://img.shields.io/badge/Poetry-Managed-60A5FA?style=flat&logo=poetry&logoColor=white)
![Java](https://img.shields.io/badge/Java-11-007396?style=flat&logo=openjdk&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-success?style=flat)
![Updated](https://img.shields.io/badge/Updated-Feb%202026-brightgreen?style=flat)

**A comprehensive learning project for mastering Apache Spark and distributed data processing, designed for aviation industry data analytics applications.**

</div>

---

Table of Contents

- [Aviation Data Analytics with PySpark](#aviation-data-analytics-with-pyspark)
  - [Overview](#overview)
    - [Learning Objectives](#learning-objectives)
    - [Disclaimer](#disclaimer)
    - [Features](#features)
  - [Quick Start](#quick-start)
    - [Prerequisites](#prerequisites)
    - [Installation Steps](#installation-steps)
    - [Running Examples](#running-examples)
      - [Traditional Approach (Direct Execution)](#traditional-approach-direct-execution)
      - [Modern Approach (Using run\_job.py - Recommended for Chapter 6+)](#modern-approach-using-run_jobpy---recommended-for-chapter-6)
  - [Learning Path](#learning-path)
    - [Chapter 1: Spark Fundamentals](#chapter-1-spark-fundamentals)
    - [Chapter 2: DataFrame Operations](#chapter-2-dataframe-operations)
    - [Chapter 3: Advanced Operations](#chapter-3-advanced-operations)
    - [Chapter 4: Data Integration \& Analytics](#chapter-4-data-integration--analytics)
    - [Chapter 5: Spark UI \& Data Skew](#chapter-5-spark-ui--data-skew)
    - [Chapter 6: Pipeline Orchestration](#chapter-6-pipeline-orchestration)
  - [Project Structure](#project-structure)
  - [Tech Stack](#tech-stack)
    - [Core Technologies](#core-technologies)
    - [Data Processing Tools](#data-processing-tools)
    - [Database \& Integration](#database--integration)
    - [Development Tools](#development-tools)
  - [Documentation](#documentation)
    - [Core Concepts](#core-concepts)
    - [Best Practices](#best-practices)
      - [1. Logging Configuration](#1-logging-configuration)
      - [2. Schema Definition](#2-schema-definition)
      - [3. Database Integration](#3-database-integration)
      - [4. Data Quality Verification](#4-data-quality-verification)
  - [Development Guide](#development-guide)
    - [Docker Commands](#docker-commands)
    - [Local Development](#local-development)
    - [Code Formatting](#code-formatting)
    - [PostgreSQL Setup (for Chapter 4)](#postgresql-setup-for-chapter-4)
  - [Learning Resources](#learning-resources)
    - [Official Documentation](#official-documentation)
    - [Recommended Reading](#recommended-reading)
    - [Key Topics Covered](#key-topics-covered)
  - [FAQ](#faq)
  - [License](#license)

---

## Overview

This is a systematic PySpark learning project focused on aviation industry data analytics scenarios. Learn distributed data processing core concepts and best practices through real-world equipment log data, from basic DataFrame operations to production-grade pipeline orchestration.

### Learning Objectives

- Master Spark core architecture (Driver, Executor, Cluster Manager)
- Understand distributed computing partitioning mechanisms
- Proficiently use DataFrame API for data transformations
- Learn production environment best practices (Schema definition, Logging configuration)
- Master database integration with JDBC and data quality verification
- Apply Window Functions for advanced analytics and ranking
- Understand storage formats and performance optimization (Parquet, Predicate Pushdown)
- **Diagnose and resolve data skew issues using Spark UI**
- **Implement salting techniques for skewed join optimization**
- **Build production-grade ETL pipelines with centralized orchestration**
- Build reproducible development environments with Docker

### Disclaimer

**All data used in this project is fictional and created solely for educational purposes.** While this project references real-world entities (such as aircraft registration numbers and airline operations) to provide realistic learning scenarios, all maintenance records, timestamps, and operational data are completely fabricated. This project is not affiliated with any airline or aviation organization, and should not be used as a source of actual aviation data.

### Features

- **Complete Docker Environment**: One-command Spark development setup with PostgreSQL JDBC support
- **Progressive Learning Path**: From basics to advanced, 6 comprehensive chapters
- **Practice-Oriented**: Real aviation industry data scenarios with database integration
- **Detailed Documentation**: 27+ markdown files covering theory and implementation
- **Best Practices**: Production-grade configurations including logging, schema definition, and data quality checks
- **Database Integration**: PostgreSQL connectivity with JDBC, data synchronization, and quality verification
- **Performance Optimization**: Spark UI analysis, data skew diagnosis, and salting techniques
- **Production Pipeline**: Centralized job runner with dynamic configuration and modular architecture

---

## Quick Start

### Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.11**
- **Poetry** (dependency management tool)
- **PostgreSQL** (optional, for Chapter 4 database integration)

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

3. **Configure environment variables**

   ```bash
   cp .env.example .env
   # Edit .env with your PostgreSQL credentials (if using Chapter 4)
   ```

4. **Start Docker container**

   ```bash
   docker-compose up -d
   ```

5. **Verify environment**

   ```bash
   docker-compose exec spark-lab python3 src/jobs/check_env.py
   ```

### Running Examples

#### Traditional Approach (Direct Execution)

```bash
# Chapter 1: Fundamentals
docker-compose exec spark-lab python3 src/jobs/01_fundamentals/1_1_spark_core.py
docker-compose exec spark-lab python3 src/jobs/01_fundamentals/1_2_data_ingestion.py

# Chapter 2: DataFrame Operations
docker-compose exec spark-lab python3 src/jobs/02_dataframe_operations/2_1_basic_operations.py
docker-compose exec spark-lab python3 src/jobs/02_dataframe_operations/2_2_casting_and_nulls.py
docker-compose exec spark-lab python3 src/jobs/02_dataframe_operations/2_3_complex_types.py

# Chapter 3: Advanced Operations
docker-compose exec spark-lab python3 src/jobs/03_advanced_operations/3_1_aggregation.py
docker-compose exec spark-lab python3 src/jobs/03_advanced_operations/3_2_multi_table_join.py
docker-compose exec spark-lab python3 src/jobs/03_advanced_operations/3_3_storage_formats.py

# Chapter 4: Data Integration (requires PostgreSQL)
docker-compose exec spark-lab python3 src/jobs/04_data_integration/4_1_postgres_sync.py
docker-compose exec spark-lab python3 src/jobs/04_data_integration/4_2_quality_check.py
docker-compose exec spark-lab python3 src/jobs/04_data_integration/4_3_window_ranking.py
docker-compose exec spark-lab python3 src/jobs/04_data_integration/4_4_advanced_analytics.py

# Chapter 5: Spark UI & Data Skew
docker-compose exec spark-lab python3 src/jobs/05_data_skew/5_2_ingestion_balance.py
docker-compose exec spark-lab python3 src/jobs/05_data_skew/5_3_join_skew_analysis.py
docker-compose exec spark-lab python3 src/jobs/05_data_skew/5_4_salting_optimization.py
```

#### Modern Approach (Using run_job.py - Recommended for Chapter 6+)

```bash
# Chapter 6: Production Pipeline with Dynamic Configuration
docker-compose exec spark-lab python3 run_job.py \
  --job pipeline.maintenance_daily_ingest \
  --partitions 13 \
  --salt 20

# Run any job module dynamically
docker-compose exec spark-lab python3 run_job.py \
  --job jobs.05_data_skew.5_4_salting_optimization \
  --partitions 17 \
  --salt 24
```

---

## Learning Path

### Chapter 1: Spark Fundamentals

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **1.1 Core Architecture** | [`1_1_spark_core.py`](src/jobs/01_fundamentals/1_1_spark_core.py) | [`1_1_spark_core.md`](docs/01_fundamentals/1_1_spark_core.md) | Centralized Architecture, Master-Slave Model, Local Mode |
| **1.2 Data Ingestion** | [`1_2_data_ingestion.py`](src/jobs/01_fundamentals/1_2_data_ingestion.py) | [`1_2_data_ingestion.md`](docs/01_fundamentals/1_2_data_ingestion.md) | Explicit Schema, Partitioning, repartition vs coalesce |

### Chapter 2: DataFrame Operations

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **2.1 Basic Operations** | [`2_1_basic_operations.py`](src/jobs/02_dataframe_operations/2_1_basic_operations.py) | [`2_1_basic_operations.md`](docs/02_dataframe_operations/2_1_basic_operations.md) | select, filter, withColumn, Immutability |
| **2.2 Data Cleaning** | [`2_2_casting_and_nulls.py`](src/jobs/02_dataframe_operations/2_2_casting_and_nulls.py) | [`2_2_casting_and_nulls.md`](docs/02_dataframe_operations/2_2_casting_and_nulls.md) | Type Casting, Null Handling, fill, drop |
| **2.3 Complex Types** | [`2_3_complex_types.py`](src/jobs/02_dataframe_operations/2_3_complex_types.py) | [`2_3_complex_types.md`](docs/02_dataframe_operations/2_3_complex_types.md) | Arrays, Structs, JSON, explode, pivot |

**Supplementary Materials:**

- [Explode vs Pivot](docs/02_dataframe_operations/2_3_1_explode_vs_pivot.md) - Understanding vertical vs horizontal data transformation

### Chapter 3: Advanced Operations

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **3.1 Aggregation** | [`3_1_aggregation.py`](src/jobs/03_advanced_operations/3_1_aggregation.py) | [`3_1_aggregation_and_grouping.md`](docs/03_advanced_operations/3_1_aggregation_and_grouping.md) | GroupBy, Aggregation Functions, Pivot Tables |
| **3.2 Join Strategies** | [`3_2_multi_table_join.py`](src/jobs/03_advanced_operations/3_2_multi_table_join.py) | [`3_2_join_strategies.md`](docs/03_advanced_operations/3_2_join_strategies.md) | Inner/Left/Anti Join, Broadcast Join, Shuffle |
| **3.3 Storage Formats** | [`3_3_storage_formats.py`](src/jobs/03_advanced_operations/3_3_storage_formats.py) | [`3_3_data_storage_formats.md`](docs/03_advanced_operations/3_3_data_storage_formats.md) | CSV vs Parquet, Columnar Storage, Compression |

**Supplementary Materials:**

- [Broadcast Join Deep Dive](docs/03_advanced_operations/3_2_1_broadcast_join.md) - Eliminating shuffle for small table joins
- [Predicate Pushdown](docs/03_advanced_operations/3_3_1_predicate_pushdown.md) - Query optimization at storage layer

### Chapter 4: Data Integration & Analytics

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **4.1 PostgreSQL Sync** | [`4_1_postgres_sync.py`](src/jobs/04_data_integration/4_1_postgres_sync.py) | [`4_1_spark_jdbc_fundamentals.md`](docs/04_data_integration/4_1_spark_jdbc_fundamentals.md) | JDBC Connection, Read/Write Modes, Partitioning |
| **4.2 Quality Check** | [`4_2_quality_check.py`](src/jobs/04_data_integration/4_2_quality_check.py) | [`4_2_data_quality_verification.md`](docs/04_data_integration/4_2_data_quality_verification.md) | Row Count Integrity, Null Checks, Anti Join |
| **4.3 Window Ranking** | [`4_3_window_ranking.py`](src/jobs/04_data_integration/4_3_window_ranking.py) | [`4_3_window_functions_ranking.md`](docs/04_data_integration/4_3_window_functions_ranking.md) | Window Functions, row_number, rank, dense_rank |
| **4.4 Advanced Analytics** | [`4_4_advanced_analytics.py`](src/jobs/04_data_integration/4_4_advanced_analytics.py) | [`4_4_advanced_window_frames.md`](docs/04_data_integration/4_4_advanced_window_frames.md) | Moving Averages, Gaps and Islands, rowsBetween |

**Supplementary Materials:**

- [Data Consistency](docs/04_data_integration/4_1_supplement_data_consistency.md) - Referential integrity and key mapping
- [Data Validation](docs/04_data_integration/4_1_supplement_data_validation.md) - Quality gates and validation metrics
- [JDBC Limitations](docs/04_data_integration/4_1_supplement_jdbc_limitations.md) - Understanding connection scope and DDL operations
- [UDF Performance](docs/04_data_integration/4_5_udf_performance_and_alternatives.md) - User-Defined Functions optimization
- [Time Series Mastery](docs/04_data_integration/4_6_time_series_mastery.md) - Advanced time-based analytics

### Chapter 5: Spark UI & Data Skew

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **5.1 Spark UI Fundamentals** | N/A | [`5_1_spark_ui_fundamentals.md`](docs/05_ui_data_skew/5_1_spark_ui_fundamentals.md) | Jobs, Stages, Tasks, DAG Visualization |
| **5.2 Identifying Data Skew** | [`5_2_ingestion_balance.py`](src/jobs/05_data_skew/5_2_ingestion_balance.py) | [`5_2_identifying_data_skew.md`](docs/05_ui_data_skew/5_2_identifying_data_skew.md) | Task Duration Variance, Partition Size Analysis |
| **5.3 Join Skew Analysis** | [`5_3_join_skew_analysis.py`](src/jobs/05_data_skew/5_3_join_skew_analysis.py) | [`5_3_join_skew_analysis.md`](docs/05_ui_data_skew/5_3_join_skew_analysis.md) | Shuffle Read/Write Metrics, Skewed Joins |
| **5.4 Salting Optimization** | [`5_4_salting_optimization.py`](src/jobs/05_data_skew/5_4_salting_optimization.py) | [`5_4_salting_technique.md`](docs/05_ui_data_skew/5_4_salting_technique.md) | Salt Keys, Hash Distribution, Performance Tuning |

**Supplementary Materials:**

- [Memory and Spill](docs/05_ui_data_skew/5_2_supplement_memory_and_spill.md) - Understanding memory pressure and disk spill

### Chapter 6: Pipeline Orchestration

| Topic | Code | Documentation | Key Concepts |
|-------|------|---------------|--------------|
| **6.1 Project Orchestration** | [`run_job.py`](run_job.py) + [`maintenance_daily_ingest.py`](src/pipeline/maintenance_daily_ingest.py) | [`6_1_project_orchestration.md`](docs/06_pipeline/6_1_project_orchestration.md) | Dynamic Job Loading, argparse, Environment Injection, Factory Pattern |

**Key Features:**

- **Centralized Job Runner**: Use `run_job.py` to dynamically load and execute any job module
- **Configuration Management**: Centralized settings in `src/config/settings.py`
- **Utility Layer**: Reusable helpers in `src/utils/spark_helper.py` (SparkSession factory, DB config)
- **Production Pipeline**: `maintenance_daily_ingest.py` demonstrates real-world ETL with salting optimization

---

## Project Structure

```text
pyspark/
├── .devcontainer/              # VS Code development container config
├── conf/
│   └── log4j.properties        # Spark logging configuration
├── data/
│   ├── raw/                    # Raw CSV data files (generated by utils)
│   └── processed/              # Processed Parquet files
├── docs/                       # Learning documentation (27 files)
│   ├── 01_fundamentals/
│   │   ├── 1_1_spark_core.md
│   │   ├── 1_2_data_ingestion.md
│   │   └── 1_3_understanding_rdds.md
│   ├── 02_dataframe_operations/
│   │   ├── 2_1_basic_operations.md
│   │   ├── 2_2_casting_and_nulls.md
│   │   ├── 2_3_supplement_complex_types.md
│   │   └── 2_3_1_explode_vs_pivot.md
│   ├── 03_advanced_operations/
│   │   ├── 3_1_aggregation_and_grouping.md
│   │   ├── 3_2_join_strategies.md
│   │   ├── 3_2_supplement_broadcast_join.md
│   │   ├── 3_3_data_storage_formats.md
│   │   └── 3_3_supplement_predicate_pushdown.md
│   ├── 04_data_integration/
│   │   ├── 4_1_spark_jdbc_fundamentals.md
│   │   ├── 4_1_supplement_data_consistency.md
│   │   ├── 4_1_supplement_data_validation.md
│   │   ├── 4_1_supplement_jdbc_limitations.md
│   │   ├── 4_2_data_quality_verification.md
│   │   ├── 4_3_window_functions_ranking.md
│   │   ├── 4_4_advanced_window_frames.md
│   │   ├── 4_5_udf_performance_and_alternatives.md
│   │   └── 4_6_time_series_mastery.md
│   ├── 05_ui_data_skew/
│   │   ├── 5_1_spark_ui_fundamentals.md
│   │   ├── 5_2_identifying_data_skew.md
│   │   ├── 5_2_supplement_memory_and_spill.md
│   │   ├── 5_3_join_skew_analysis.md
│   │   └── 5_4_salting_technique.md
│   └── 06_pipeline/
│       └── 6_1_project_orchestration.md
├── sql/
│   └── init/
│       └── 01_init_maintenance.sql  # PostgreSQL schema initialization
├── src/
│   ├── config/                 # Global configuration
│   │   └── settings.py         # Project-wide settings (namedtuple)
│   ├── jobs/                   # Learning scripts (Chapters 1-5)
│   │   ├── 01_fundamentals/
│   │   │   ├── 1_1_spark_core.py
│   │   │   └── 1_2_data_ingestion.py
│   │   ├── 02_dataframe_operations/
│   │   │   ├── 2_1_basic_operations.py
│   │   │   ├── 2_2_casting_and_nulls.py
│   │   │   └── 2_3_complex_types.py
│   │   ├── 03_advanced_operations/
│   │   │   ├── 3_1_aggregation.py
│   │   │   ├── 3_2_multi_table_join.py
│   │   │   └── 3_3_storage_formats.py
│   │   ├── 04_data_integration/
│   │   │   ├── 4_1_postgres_sync.py
│   │   │   ├── 4_2_quality_check.py
│   │   │   ├── 4_3_window_ranking.py
│   │   │   ├── 4_4_advanced_analytics.py
│   │   │   ├── 4_5_udf_implementation.py
│   │   │   └── 4_6_time_mastery.py
│   │   ├── 05_data_skew/
│   │   │   ├── 5_2_ingestion_balance.py
│   │   │   ├── 5_3_join_skew_analysis.py
│   │   │   └── 5_4_salting_optimization.py
│   │   └── check_env.py
│   ├── pipeline/               # Production ETL pipelines (Chapter 6)
│   │   └── maintenance_daily_ingest.py
│   └── utils/                  # Shared utilities
│       ├── spark_helper.py     # SparkSession factory, DB config
│       ├── generate_raw_data.py
│       ├── skew_data_generator.py
│       └── debug_hash_distribution.py
├── tests/                      # Unit tests
├── notebook/                   # Jupyter notebooks
├── run_job.py                  # 🎯 Central job runner (Chapter 6)
├── .env.example                # Environment variables template
├── Dockerfile                  # Spark + PostgreSQL JDBC driver
├── docker-compose.yml          # Container orchestration
├── pyproject.toml              # Poetry dependencies
└── README.md
```

---

## Tech Stack

### Core Technologies

- **Apache Spark 3.5.0** - Distributed data processing engine
- **PySpark** - Python API for Spark
- **Python 3.11** - Programming language
- **Docker** - Containerized development environment
- **PostgreSQL JDBC Driver 42.7.1** - Database connectivity

### Data Processing Tools

- **Polars 1.37+** - High-performance DataFrame library
- **Pandas 3.0+** - Traditional data analysis tool
- **PyArrow 23.0+** - Apache Arrow Python bindings for Parquet support

### Database & Integration

- **PostgreSQL** - Relational database for data integration
- **psycopg2-binary** - Python PostgreSQL adapter
- **python-dotenv** - Environment variable management

### Development Tools

- **Poetry** - Dependency management and packaging
- **Black** - Code formatter
- **isort** - Import sorter
- **pytest** - Testing framework
- **findspark** - Spark discovery for notebooks

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

**Columnar Storage (Parquet)**  
Industry-standard format for big data that enables column pruning, predicate pushdown, and efficient compression.

**Window Functions**  
Perform calculations across related rows without collapsing them, essential for ranking, deduplication, and time-series analysis.

**JDBC Integration**  
Spark uses JDBC drivers to connect to relational databases, enabling distributed reads/writes with parallel execution.

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

#### 3. Database Integration

**Environment Variables**: Store credentials in `.env` file

```bash
POSTGRES_HOST=host.docker.internal
POSTGRES_PORT=5432
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

**Connection Pattern**:

```python
import os

db_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/dbname"
properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}
df = spark.read.jdbc(url=db_url, table="table_name", properties=properties)
```

#### 4. Data Quality Verification

Always validate data after joins:

```python
# Check row count integrity
original_count = df_fact.count()
joined_count = df_joined.count()
assert original_count == joined_count, "Row explosion detected!"

# Check for orphan records
orphans = df_fact.join(df_dim, on="key", how="left_anti")
assert orphans.count() == 0, "Missing master data!"
```

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
docker-compose exec spark-lab python3 src/jobs/<chapter>/<script_name>.py
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

### PostgreSQL Setup (for Chapter 4)

1. **Install PostgreSQL locally** or use Docker

2. **Create database**:

   ```sql
   CREATE DATABASE maintenance_db;
   ```

3. **Run initialization script**:

   ```bash
   psql -U postgres -d maintenance_db -f sql/init/01_init_maintenance.sql
   ```

4. **Update `.env`** with your credentials

---

## Learning Resources

### Official Documentation

- [Apache Spark Official Documentation](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [PostgreSQL JDBC Driver](https://jdbc.postgresql.org/documentation/)

### Recommended Reading

- [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)

### Key Topics Covered

- **Chapter 1**: Spark architecture, local mode, schema definition, partitioning
- **Chapter 2**: DataFrame transformations, type casting, null handling, complex types
- **Chapter 3**: Aggregations, join strategies, broadcast joins, storage formats, predicate pushdown
- **Chapter 4**: JDBC connectivity, data quality verification, window functions, moving averages, gaps and islands
- **Chapter 5**: Spark UI analysis, data skew diagnosis, task duration variance, salting techniques, hash distribution
- **Chapter 6**: Dynamic job loading, centralized orchestration, factory pattern, environment injection, production ETL pipelines

---

## FAQ

<details>
<summary><b>Q: Why use Docker?</b></summary>

A: Docker provides a consistent development environment, ensuring all dependencies (including Java 11, Spark 3.5.0, and PostgreSQL JDBC driver) are correctly installed, avoiding environment configuration issues.
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

<details>
<summary><b>Q: Do I need PostgreSQL to complete this project?</b></summary>

A: PostgreSQL is only required for Chapter 4 (Data Integration). Chapters 1-3 work with CSV and Parquet files only. You can skip Chapter 4 if you want to focus on core Spark concepts first.
</details>

<details>
<summary><b>Q: What's the difference between Parquet and CSV?</b></summary>

A: Parquet is a columnar storage format optimized for big data. It provides better compression, supports predicate pushdown (skipping irrelevant data), and stores schema metadata. CSV is row-based, human-readable, but inefficient for large-scale analytics.
</details>

<details>
<summary><b>Q: When should I use Window Functions vs GroupBy?</b></summary>

A: Use `groupBy` when you want to collapse rows into aggregated summaries. Use Window Functions when you need to keep row-level detail while adding ranking, running totals, or moving averages.
</details>

---

## License

This project is licensed under the MIT License.

---
