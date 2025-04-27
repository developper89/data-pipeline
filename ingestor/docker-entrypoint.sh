#!/bin/bash
set -e

# Print environment for debugging (exclude sensitive info)
echo "Starting InfluxDB Ingestor Service"
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Kafka Validated Data Topic: $KAFKA_VALIDATED_DATA_TOPIC"
echo "Kafka Consumer Group ID: $KAFKA_CONSUMER_GROUP_ID"
echo "InfluxDB URL: $INFLUXDB_URL"
echo "InfluxDB Bucket: $INFLUXDB_BUCKET"
echo "InfluxDB Org: $INFLUXDB_ORG"

# Execute command passed to docker run
exec "$@" 