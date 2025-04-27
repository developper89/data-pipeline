import os
import logging

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "ingestor_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "preservarium")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")
INFLUXDB_BATCH_SIZE = int(os.getenv("INFLUXDB_BATCH_SIZE", "100"))
INFLUXDB_FLUSH_INTERVAL_MS = int(os.getenv("INFLUXDB_FLUSH_INTERVAL_MS", "5000"))
INFLUXDB_TIMESTAMP_PRECISION = os.getenv("INFLUXDB_TIMESTAMP_PRECISION", "s")  # Options: 'ns', 'us', 'ms', 's'

# Service Configuration
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

LOG_LEVEL = LOG_LEVEL_MAP.get(os.getenv("LOG_LEVEL", "INFO"), logging.INFO) 