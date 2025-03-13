#!/bin/bash

if [ -d "$MINIO_DATA_DIR" ]; then
    echo "Starting MinIO server..."
    minio server $MINIO_DATA_DIR --console-address ":9001" &
fi

# Wait for MinIO to be ready
echo "Waiting for MinIO to start..."
until curl -s http://localhost:9000/minio/health/live; do
  sleep 3
done
echo "MinIO is ready."

mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD

# Create the bucket if it does not exist
if ! mc ls myminio/gaming > /dev/null 2>&1; then
  echo "Creating bucket 'gaming'..."
  mc mb myminio/gaming/spark-events/

fi

if [ "$SPARK_MODE" = "master" ];
then
    echo "Starting Spark Master..."
    start-master.sh
elif [ "$SPARK_MODE" = "worker" ];
then
    echo "Starting Spark Worker..."
    start-worker.sh "$SPARK_MASTER_URL"
elif [ "$SPARK_MODE" == "history" ]
then
  start-history-server.sh
else
    echo "Unknown SPARK_MODE: $SPARK_MODE"
    exit 1
fi

tail -f /dev/null
