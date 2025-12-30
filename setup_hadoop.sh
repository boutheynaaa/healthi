#!/bin/bash

echo "=============================================="
echo "  HealthInsight - Hadoop Cluster Setup"
echo "=============================================="

# Function to check if container is running
check_container() {
    docker ps --format '{{.Names}}' | grep -q "$1"
}


# Wait for containers to start
echo "⏳ Waiting for containers to start..."
sleep 10
cho "✓ Hadoop master container is running"

# Create HDFS directories
echo "📁 Creating HDFS directories..."
docker exec healthinsight-hadoop-master bash -c "
    hdfs dfs -mkdir -p /data/raw 2>/dev/null || true
    hdfs dfs -mkdir -p /data/processed 2>/dev/null || true
    hdfs dfs -mkdir -p /analytics/results 2>/dev/null || true
    hdfs dfs -mkdir -p /analytics/batch_results 2>/dev/null || true
    hdfs dfs -chmod -R 777 /data 2>/dev/null || true
    hdfs dfs -chmod -R 777 /analytics 2>/dev/null || true
    echo '✓ HDFS directories created'
"

