# coap_connector/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# CoAP Server Config
COAP_HOST = os.getenv("COAP_HOST", "::") # Listen on all IPv6 and IPv4 interfaces
COAP_PORT = int(os.getenv("COAP_PORT", "5683")) # Default CoAP port

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_DATA_TOPIC = os.getenv("KAFKA_RAW_DATA_TOPIC", "iot_raw_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors") # Optional: For gateway-specific errors

# Service Config
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SERVICE_NAME = "coap_connector"
# Base path for data ingestion (e.g., devices POST to /data/device123)
COAP_BASE_DATA_PATH = tuple(filter(None, os.getenv("COAP_BASE_DATA_PATH", "data").split('/'))) # -> ('data',)