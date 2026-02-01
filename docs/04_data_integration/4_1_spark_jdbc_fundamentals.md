# 4_1 Spark JDBC Fundamentals: Connecting to PostgreSQL

## 1. What is JDBC?

**JDBC (Java Database Connectivity)** is an API that allows Java-based applications (like Apache Spark) to interact with relational databases. Since Spark runs on the JVM, it uses JDBC drivers to translate Spark SQL commands into database-specific SQL.

## 2. The Architecture of Spark-DB Connection

Unlike a standard Python script (using `psycopg2`), Spark manages DB connections in a distributed manner:

* **The Driver**: Establishes the initial connection and retrieves the table schema.
* **The Executors**: Each executor can open its own connection to the database to read or write data in parallel (if partitioning is configured).

## 3. Core Connection Properties

To connect to PostgreSQL, Spark requires four primary pieces of information:

* **URL**: `jdbc:postgresql://<host>:<port>/<dbname>`
* **Table**: The target table name or a subquery.
* **Driver**: `org.postgresql.Driver`
* **Credentials**: User and Password.

## 4. Reading Modes & Performance

* **Single Connection (Default)**: Spark reads the entire table through one executor. This is fine for small tables but a bottleneck for large ones.
* **Partitioned Read**: By providing a `partitionColumn`, `lowerBound`, and `upperBound`, Spark can split the SQL query into multiple parallel tasks (e.g., `SELECT * FROM table WHERE id BETWEEN 1 AND 1000`).

## 5. Write SaveModes

* **Append**: Adds new data to the existing table.
* **Overwrite**: Drops/Truncates the table and creates a new one.
* **ErrorIfExists**: Throws an error if the table already exists (Default).
* **Ignore**: Does nothing if the table exists.
