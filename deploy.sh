#!/bin/bash

# 1. Define variables
CONTAINER_NAME="spark-lab"
JOB_NAME="pipeline.maintenance_daily_ingest"
DEFAULT_PARTITIONS="13"
DEFAULT_SALT="24"

echo "------------------------------------------------"
echo "Starting Local Deployment & Pipeline Trigger"
echo "------------------------------------------------"

# 2. Check if Docker container is running
if [ "$(docker inspect -f '{{.State.Running}}' $CONTAINER_NAME 2>/dev/null)" != "true" ]; then
    echo "Container $CONTAINER_NAME is not running. Starting it now..."
    docker-compose up -d
    sleep 5 # Wait for DB and Spark initialization
fi

# 3. Execute Pipeline
# We can pass different variables to reach flexible deployment
echo "Executing $JOB_NAME..."
docker-compose exec $CONTAINER_NAME python3 run_job.py \
    --job $JOB_NAME \
    --partitions $DEFAULT_PARTITIONS \
    --salt $DEFAULT_SALT

# 4. Check execution state (This is the return of sys.exit(1))
if [ $? -eq 0 ]; then
    echo "------------------------------------------------"
    echo "Deployment and Data Write SUCCESS!"
    echo "------------------------------------------------"
else
    echo "------------------------------------------------"
    echo "Pipeline FAILED! Please check the logs above."
    echo "------------------------------------------------"
    exit 1
fi