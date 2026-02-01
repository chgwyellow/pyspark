# Use Python 3.11 official image as base
FROM python:3.11-slim-bullseye

# Switch to root user to install system dependencies
USER root

# Install Java 11 (required for Spark) and other dependencies
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Download and install Apache Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64" > ${SPARK_HOME}/conf/spark-env.sh && \
    chmod +x ${SPARK_HOME}/conf/spark-env.sh

# Download PostgreSQL JDBC Driver for Spark-PostgreSQL connectivity
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.1.jar \
    -o ${SPARK_HOME}/jars/postgresql-42.7.1.jar

# Set the working directory inside the container
WORKDIR /app

# Copy dependency files first to leverage Docker cache
COPY pyproject.toml poetry.lock* /app/

# Install Poetry and project dependencies globally within the container
# We disable virtualenv creation because the container itself provides isolation
RUN pip3 install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --only main --no-root --no-interaction --no-ansi

# Copy the rest of the application source code
COPY . /app

# Define environment variables for PySpark to use the correct Python interpreter
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Default command to run the environment check script
CMD ["python3", "src/jobs/check_env.py"]
