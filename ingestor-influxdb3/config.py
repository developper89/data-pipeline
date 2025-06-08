import os
import logging

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "ingestor_influxdb3_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# InfluxDB3 Configuration
INFLUXDB3_URL = os.getenv("INFLUXDB3_URL", "http://influxdb3-core:8181")
INFLUXDB3_TOKEN = os.getenv("INFLUXDB3_TOKEN", "")
INFLUXDB3_DATABASE = os.getenv("INFLUXDB3_DATABASE", "iot_data")
INFLUXDB3_BATCH_SIZE = int(os.getenv("INFLUXDB3_BATCH_SIZE", "100"))
INFLUXDB3_FLUSH_INTERVAL_MS = int(os.getenv("INFLUXDB3_FLUSH_INTERVAL_MS", "5000"))
INFLUXDB3_TIMESTAMP_PRECISION = os.getenv("INFLUXDB3_TIMESTAMP_PRECISION", "s")  # Options: 'ns', 'us', 'ms', 's'

# InfluxDB3 API Configuration
INFLUXDB3_API_VERSION = os.getenv("INFLUXDB3_API_VERSION", "v3")
INFLUXDB3_WRITE_ENDPOINT = os.getenv("INFLUXDB3_WRITE_ENDPOINT", "/api/v3/write_lp")
INFLUXDB3_QUERY_ENDPOINT = os.getenv("INFLUXDB3_QUERY_ENDPOINT", "/api/v3/query_sql")

# HTTP Client Configuration
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "30"))
HTTP_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
HTTP_RETRY_BACKOFF_FACTOR = float(os.getenv("HTTP_RETRY_BACKOFF_FACTOR", "1.0"))

# Service Configuration
SERVICE_NAME = os.getenv("SERVICE_NAME", "pipeline-ingestor-influxdb3")
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

LOG_LEVEL = LOG_LEVEL_MAP.get(os.getenv("LOG_LEVEL", "INFO"), logging.INFO) 