# mailer/alert_consumer.py
import logging
import asyncio
import json
from typing import Dict, Any, List
import time

from confluent_kafka import KafkaError, KafkaException

from shared.mq.kafka_helpers import AsyncResilientKafkaConsumer, create_kafka_producer
from email_service import EmailService
import config

logger = logging.getLogger(__name__)

class AlertConsumer:
    """
    Consumes alert messages from Kafka and sends email notifications to recipients.
    """
    
    def __init__(self):
        """Initialize the alert consumer."""
        self.resilient_consumer = None  # Use AsyncResilientKafkaConsumer
        self.error_producer = None
        self.running = False
        self._stop_event = asyncio.Event()
        
        # Initialize email service
        self.email_service = EmailService()
        
        # Statistics
        self.stats = {
            'messages_processed': 0,
            'emails_sent': 0,
            'emails_failed': 0,
            'errors': 0,
            'started_at': None
        }
        
    async def start(self):
        """Start the alert consumer."""
        if self.running:
            logger.warning("Alert consumer already running")
            return False
            
        # Test SMTP connection first
        if not self.email_service.test_smtp_connection():
            logger.error("SMTP connection test failed. Please check email configuration.")
            return False
            
        # Initialize error producer
        self.error_producer = create_kafka_producer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
        )
            
        # Initialize resilient consumer
        self.resilient_consumer = AsyncResilientKafkaConsumer(
            topic=config.KAFKA_ALERTS_TOPIC,
            group_id=config.KAFKA_CONSUMER_GROUP_ID,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',  # Only process new alerts
            on_error_callback=self._on_consumer_error
        )
            
        self.running = True
        self.stats['started_at'] = time.time()
        
        logger.info("Alert consumer started")
        return True
        
    async def consume_alerts(self):
        """Start consuming alert messages using AsyncResilientKafkaConsumer."""
        if not self.resilient_consumer:
            logger.error("Cannot start consuming: AsyncResilientKafkaConsumer not initialized")
            return
            
        try:
            # This handles all the complexity: polling, error recovery, reconnection, etc.
            await self.resilient_consumer.consume_messages(
                message_handler=self._process_alert,
                commit_offset=True,
                batch_processing=False  # Process alerts individually
            )
        except Exception as e:
            logger.exception(f"Fatal error in alert consumer: {e}")
            self.running = False
        
    async def _process_alert(self, message) -> bool:
        """
        Process a single alert message.
        
        Args:
            message: Kafka message object
            
        Returns:
            True if processing was successful, False otherwise
        """
        try:
            self.stats['messages_processed'] += 1
            
            # Message value is already deserialized by KafkaConsumer
            alert_data = message.parsed_value
            
            alert_id = alert_data.get('id', alert_data.get('alert_id', 'unknown'))
            logger.info(f"Processing alert {alert_id}")
            
            # Validate required fields
            required_fields = ['id', 'message', 'recipient_emails']
            missing_fields = [field for field in required_fields if field not in alert_data or not alert_data[field]]
            
            if missing_fields:
                error_msg = f"Alert {alert_id} missing required fields: {missing_fields}"
                logger.error(error_msg)
                await self._send_error(alert_id, error_msg, alert_data)
                self.stats['errors'] += 1
                return True  # Consider invalid message as processed (won't retry)
            
            # Extract alert information
            message_content = alert_data['message']
            recipient_emails = alert_data['recipient_emails']
            subject = alert_data.get('subject', f"Alert Notification - {alert_id}")
            
            # Validate recipient emails
            if not isinstance(recipient_emails, list) or not recipient_emails:
                error_msg = f"Alert {alert_id} has invalid recipient_emails format"
                logger.error(error_msg)
                await self._send_error(alert_id, error_msg, alert_data)
                self.stats['errors'] += 1
                return True  # Consider invalid message as processed (won't retry)
            
            # Send email notification
            try:
                success = await self.email_service.send_alert_notification(
                    alert_id=alert_id,
                    message=message_content,
                    recipient_emails=recipient_emails,
                    subject=subject,
                    alert_data=alert_data
                )
                
                if success:
                    logger.info(f"Successfully sent alert email for {alert_id} to {len(recipient_emails)} recipients")
                    self.stats['emails_sent'] += 1
                    return True
                else:
                    error_msg = f"Failed to send alert email for {alert_id}"
                    logger.error(error_msg)
                    await self._send_error(alert_id, error_msg, alert_data)
                    self.stats['emails_failed'] += 1
                    return False  # Indicate failure - message will be retried
                    
            except Exception as e:
                error_msg = f"Exception while sending alert email for {alert_id}: {str(e)}"
                logger.exception(error_msg)
                await self._send_error(alert_id, error_msg, alert_data)
                self.stats['emails_failed'] += 1
                return False  # Indicate failure - message will be retried
                
        except Exception as e:
            logger.exception(f"Unexpected error processing alert message: {e}")
            self.stats['errors'] += 1
            return False  # Indicate failure - message will be retried
    
    async def _on_consumer_error(self, error, context):
        """Handle consumer-level errors."""
        logger.error(f"Consumer error: {error}")
        self.stats['errors'] += 1
        # Could implement additional error handling logic here if needed
    
    async def _send_error(self, alert_id: str, error_message: str, alert_data: Dict[str, Any]):
        """Send error message to error topic."""
        if not self.error_producer:
            return
            
        try:
            error_data = {
                'service': config.SERVICE_NAME,
                'error_type': 'email_processing_error',
                'alert_id': alert_id,  # Keep alert_id for error tracking
                'error_message': error_message,
                'timestamp': time.time(),
                'original_alert': alert_data
            }
            
            self.error_producer.send(
                config.KAFKA_ERROR_TOPIC,
                value=error_data,
                key=alert_id
            )
            
        except Exception as e:
            logger.error(f"Failed to send error message to Kafka: {e}")
    
    async def stop(self):
        """Stop the alert consumer."""
        if not self.running:
            logger.warning("Alert consumer not running")
            return
            
        logger.info("Stopping alert consumer...")
        self.running = False
        self._stop_event.set()
        
        # Stop the resilient consumer
        if self.resilient_consumer:
            await self.resilient_consumer.stop()
        
        # Close error producer
        if self.error_producer:
            try:
                self.error_producer.close()
                logger.info("Kafka error producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka error producer: {e}")
            finally:
                self.error_producer = None
                
        logger.info("Alert consumer stopped")
                
    def get_statistics(self) -> Dict[str, Any]:
        """Get consumer statistics."""
        stats = self.stats.copy()
        if stats['started_at']:
            stats['uptime_seconds'] = time.time() - stats['started_at']
        return stats
    
    def reset_statistics(self):
        """Reset consumer statistics."""
        self.stats = {
            'messages_processed': 0,
            'emails_sent': 0,
            'emails_failed': 0,
            'errors': 0,
            'started_at': time.time() if self.running else None
        } 