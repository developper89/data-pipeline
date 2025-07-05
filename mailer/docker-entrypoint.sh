#!/bin/bash

# Exit on any error
set -e

echo "Starting Mailer Service..."

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
while ! nc -z ${KAFKA_BOOTSTRAP_SERVERS%:*} ${KAFKA_BOOTSTRAP_SERVERS#*:}; do
    echo "Kafka is not ready yet. Waiting..."
    sleep 2
done
echo "Kafka is ready!"

# Test SMTP configuration if provided
if [ -n "${SMTP_HOST}" ] && [ -n "${SMTP_PORT}" ]; then
    echo "Testing SMTP connection to ${SMTP_HOST}:${SMTP_PORT}..."
    timeout 10 nc -z ${SMTP_HOST} ${SMTP_PORT} && echo "SMTP server is reachable" || echo "Warning: SMTP server may not be reachable"
fi

# Start the mailer service
echo "Starting mailer service..."
exec python main.py 