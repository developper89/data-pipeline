# mailer/alert_consumer.py
import logging
import asyncio
import json
from typing import Dict, Any, List
import time

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.mq.kafka_helpers import create_kafka_consumer, create_kafka_producer
from email_service import EmailService
import config

logger = logging.getLogger(__name__)

class AlertConsumer:
    """
    Consumes alert messages from Kafka and sends email notifications to recipients.
    """
    
    def __init__(self):
        """Initialize the alert consumer."""
        self.consumer = None
        self.error_producer = None
        self.running = False
        self._stop_event = asyncio.Event()
        self._task = None
        
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
            return
            
        # Test SMTP connection first
        if not self.email_service.test_smtp_connection():
            logger.error("SMTP connection test failed. Please check email configuration.")
            return False
            
        self.running = True
        self._stop_event.clear()
        self.stats['started_at'] = time.time()
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("Alert consumer started")
        return True
        
    async def stop(self):
        """Stop the alert consumer."""
        if not self.running:
            logger.warning("Alert consumer not running")
            return
            
        logger.info("Stopping alert consumer...")
        self.running = False
        self._stop_event.set()
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Alert consumer task did not finish promptly, cancelling")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
            
        await self._close_consumer()
        await self._close_error_producer()
        logger.info("Alert consumer stopped")
            
    async def _consume_loop(self):
        """Main loop for consuming alert messages."""
        retry_delay = 5  # Initial retry delay in seconds
        max_retry_delay = 60  # Maximum retry delay in seconds
        
        while self.running:
            try:
                # Create the consumer if it doesn't exist
                if not self.consumer:
                    self.consumer = create_kafka_consumer(
                        topic=config.KAFKA_ALERTS_TOPIC,
                        group_id=config.KAFKA_CONSUMER_GROUP_ID,
                        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                        auto_offset_reset='latest'  # Only process new alerts
                    )
                    if not self.consumer:
                        logger.error("Failed to create Kafka consumer for alerts, will retry...")
                        await asyncio.sleep(retry_delay)
                        retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff
                        continue
                        
                    retry_delay = 5  # Reset retry delay on successful connection
                
                # Create error producer if needed
                if not self.error_producer:
                    self.error_producer = create_kafka_producer(
                        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS
                    )
                
                # Poll for messages (non-blocking in asyncio context)
                message_batch = self.consumer.poll(timeout_ms=1000)
                
                if not message_batch:
                    # No messages, check if we should stop
                    if self._stop_event.is_set():
                        break
                    await asyncio.sleep(0.1)
                    continue
                
                # Process received messages
                for tp, messages in message_batch.items():
                    for message in messages:
                        if self._stop_event.is_set():
                            break
                            
                        await self._process_alert(message.value)
                        self.stats['messages_processed'] += 1
                
                # Commit offsets
                self.consumer.commit()
                    
            except KafkaError as e:
                logger.error(f"Kafka error in alert consumer: {e}")
                self.stats['errors'] += 1
                await self._close_consumer()
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
            except Exception as e:
                logger.exception(f"Unexpected error in alert consumer: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, max_retry_delay)
                
            # Check if we should stop
            if self._stop_event.is_set():
                break
    
    async def _process_alert(self, alert_data: Dict[str, Any]):
        """
        Process an alert message from Kafka and send email notifications.
        
        Args:
            alert_data: The alert data from Kafka
        """
        try:
            alert_id = alert_data.get('alert_id', 'unknown')
            recipients = alert_data.get('recipients', [])
            notify_creator = alert_data.get('notify_creator', False)
            
            # Validate alert data
            if not alert_id:
                logger.error("Invalid alert message: missing alert_id")
                return
            
            if not recipients and not notify_creator:
                logger.warning(f"[{alert_id}] No recipients specified for alert")
                return
            
            # Parse recipients if they're in string format (comma-separated)
            if isinstance(recipients, str):
                recipients = [email.strip() for email in recipients.split(',') if email.strip()]
            
            # Ensure recipients is a list
            if not isinstance(recipients, list):
                recipients = []
            
            # Filter out invalid email addresses
            valid_recipients = []
            for email in recipients:
                if isinstance(email, str) and '@' in email and '.' in email:
                    valid_recipients.append(email.strip())
                else:
                    logger.warning(f"[{alert_id}] Invalid email address: {email}")
            
            if not valid_recipients:
                logger.warning(f"[{alert_id}] No valid email recipients found")
                return
            
            logger.info(f"[{alert_id}] Processing alert for {len(valid_recipients)} recipients")
            
            # Send email with retry logic
            success = self.email_service.send_alert_email_with_retry(alert_data, valid_recipients)
            
            if success:
                self.stats['emails_sent'] += 1
                logger.info(f"[{alert_id}] Successfully sent alert email to {len(valid_recipients)} recipients")
            else:
                self.stats['emails_failed'] += 1
                logger.error(f"[{alert_id}] Failed to send alert email after all retry attempts")
                
                # Send error to error topic
                await self._send_error(alert_id, "Failed to send alert email", alert_data)
                
        except Exception as e:
            logger.exception(f"Error processing alert: {e}")
            self.stats['errors'] += 1
            await self._send_error(
                alert_data.get('alert_id', 'unknown'), 
                f"Error processing alert: {str(e)}", 
                alert_data
            )
    
    async def _send_error(self, alert_id: str, error_message: str, alert_data: Dict[str, Any]):
        """Send error message to error topic."""
        if not self.error_producer:
            return
            
        try:
            error_data = {
                'service': config.SERVICE_NAME,
                'error_type': 'email_processing_error',
                'alert_id': alert_id,
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
    
    async def _close_consumer(self):
        """Close the Kafka consumer if it exists."""
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka alert consumer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka consumer: {e}")
            finally:
                self.consumer = None
    
    async def _close_error_producer(self):
        """Close the Kafka error producer if it exists."""
        if self.error_producer:
            try:
                self.error_producer.close()
                logger.info("Kafka error producer closed")
            except Exception as e:
                logger.error(f"Error closing Kafka error producer: {e}")
            finally:
                self.error_producer = None
                
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