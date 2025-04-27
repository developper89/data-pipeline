#!/bin/bash
set -e

uv pip install --system -e /app/packages/preservarium-sdk[dev,core]
# Print environment for debugging (exclude sensitive info)
echo "Starting Cache Service"
echo "Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
echo "Kafka Validated Data Topic: $KAFKA_VALIDATED_DATA_TOPIC"
echo "Kafka Consumer Group ID: $KAFKA_CONSUMER_GROUP_ID"
echo "Redis Host: $REDIS_HOST"
echo "Redis Port: $REDIS_PORT"
echo "Redis Password: ${REDIS_PASSWORD:+*****}" # Show asterisks if password is set
echo "Redis Metadata TTL: $REDIS_METADATA_TTL"

# Execute command passed to docker run
exec "$@" 