#!/bin/bash
set -e

# Print environment for debugging (exclude sensitive info)
echo "Starting InfluxDB3 Ingestor Service"
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Kafka Validated Data Topic: $KAFKA_VALIDATED_DATA_TOPIC"
echo "Kafka Consumer Group ID: $KAFKA_CONSUMER_GROUP_ID"
echo "InfluxDB3 URL: $INFLUXDB3_URL"
echo "InfluxDB3 Database: $INFLUXDB3_DATABASE"
echo "Service Name: $SERVICE_NAME"

# Execute command passed to docker run
exec "$@" 