# normalizer_service/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_RAW_DATA_TOPIC = os.getenv("KAFKA_RAW_DATA_TOPIC", "iot_raw_data")
KAFKA_VALIDATED_DATA_TOPIC = os.getenv("KAFKA_VALIDATED_DATA_TOPIC", "iot_validated_data")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "normalizer_group")
# Time in seconds consumer waits for messages if buffer is empty
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# Database Config (URL usually in shared, but acknowledge here)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/iot_config")

# Script Storage Config
SCRIPT_STORAGE_TYPE = os.getenv("SCRIPT_STORAGE_TYPE", "local") # 'local' or 's3'
LOCAL_SCRIPT_DIR = os.getenv("LOCAL_SCRIPT_DIR", "../storage/parser_scripts")

# Validation Config
USE_ENHANCED_VALIDATION = os.getenv("USE_ENHANCED_VALIDATION", "true").lower() in ('true', 'yes', '1')

# Service Config
# LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SERVICE_NAME = "normalizer"