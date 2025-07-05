# mailer/main.py
"""
Mailer Service - Email Alert Notification Service

This service consumes alert messages from Kafka and sends email notifications
to specified recipients using SMTP.

Features:
- Kafka integration for consuming alerts
- SMTP email sending with HTML templates
- Rate limiting and retry logic
- Error handling and logging
- Health monitoring and statistics
"""

import sys
import os

# Add the parent directory to the Python path to access shared modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from service import main

if __name__ == "__main__":
    asyncio.run(main()) 