# Mailer Service

The Mailer Service is responsible for consuming alert messages from Kafka and sending email notifications to specified recipients.

## Features

- Kafka integration for consuming alerts
- SMTP email delivery with TLS/SSL support
- HTML email templates using Jinja2
- Rate limiting and retry logic
- Comprehensive error handling
- Health monitoring and statistics

## Configuration

Set these environment variables:

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka servers (default: localhost:9092)
- `KAFKA_ALERTS_TOPIC`: Alerts topic (default: iot_alerts)
- `SMTP_HOST`: SMTP server (default: smtp.gmail.com)
- `SMTP_PORT`: SMTP port (default: 587)
- `SMTP_USERNAME`: SMTP username
- `SMTP_PASSWORD`: SMTP password
- `SMTP_FROM_EMAIL`: From email address

## Usage

Run with Docker Compose:

```bash
docker-compose up mailer
```

Or standalone:

```bash
cd mailer
python main.py
```
