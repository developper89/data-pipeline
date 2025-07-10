# mail_service/config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "iot_alerts")
KAFKA_ERROR_TOPIC = os.getenv("KAFKA_ERROR_TOPIC", "iot_errors")
KAFKA_CONSUMER_GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP_ID", "mail_service_group")
KAFKA_CONSUMER_POLL_TIMEOUT_S = float(os.getenv("KAFKA_CONSUMER_POLL_TIMEOUT_S", "1.0"))

# SMTP Configuration
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "alerts@preservarium.com")
SMTP_FROM_NAME = os.getenv("SMTP_FROM_NAME", "Preservarium Alert System")

# Email Template Configuration
EMAIL_TEMPLATE_DIR = os.getenv("EMAIL_TEMPLATE_DIR", "/app/mailer/templates")
DEFAULT_EMAIL_TEMPLATE = os.getenv("DEFAULT_EMAIL_TEMPLATE", "alert_notification.html")

# Service Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
SERVICE_NAME = "mailer"
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))

# Email Rate Limiting
EMAIL_RATE_LIMIT_PER_MINUTE = int(os.getenv("EMAIL_RATE_LIMIT_PER_MINUTE", "60"))
EMAIL_BATCH_SIZE = int(os.getenv("EMAIL_BATCH_SIZE", "10"))

# Database Configuration (for logging email send status)
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/preservarium") 