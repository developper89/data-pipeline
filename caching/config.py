import os
import logging

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "cache_service_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_METADATA_TTL = int(os.getenv("REDIS_METADATA_TTL", "86400"))  # 24 hours

# Service Configuration
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

LOG_LEVEL = LOG_LEVEL_MAP.get(os.getenv("LOG_LEVEL", "INFO"), logging.INFO)
SERVICE_NAME = "caching" 