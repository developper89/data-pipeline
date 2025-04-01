# mqtt_connector/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_RAW_DATA_TOPIC = os.getenv("KAFKA_TOPIC", "iot_raw_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors") # Optional

# MQTT Config
MQTT_BROKER_HOST = os.getenv("BROKER_HOST", "localhost")
MQTT_BROKER_PORT = int(os.getenv("BROKER_PORT", "1883"))
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "mqtt_connector_service")
MQTT_USERNAME = os.getenv("MQTT_USERNAME") # Optional
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") # Optional
MQTT_TOPIC_SUBSCRIBE_PATTERN = os.getenv("MQTT_TOPICS", "sensors/#,devices/#")
USE_TLS = os.getenv("USE_TLS", "false").lower() == "true"
CA_CERT_CONTENT = os.getenv("CA_CERT_CONTENT")

# Service Config
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
SERVICE_NAME = "mqtt_connector"