# mailer/email_service.py
import logging
import smtplib
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from typing import List, Dict, Any, Optional
from collections import defaultdict, deque
from jinja2 import Environment, FileSystemLoader, Template
import asyncio
from preservarium_sdk.core.utils import translate

import config

logger = logging.getLogger(__name__)

class EmailService:
    """
    Email service for sending alert notifications via SMTP.
    Supports HTML templates, rate limiting, and retry logic.
    """
    
    def __init__(self):
        """Initialize the email service."""
        self.smtp_host = config.SMTP_HOST
        self.smtp_port = config.SMTP_PORT
        self.smtp_username = config.SMTP_USERNAME
        self.smtp_password = config.SMTP_PASSWORD
        self.smtp_use_tls = config.SMTP_USE_TLS
        self.smtp_use_ssl = config.SMTP_USE_SSL
        self.from_email = config.SMTP_FROM_EMAIL
        self.from_name = config.SMTP_FROM_NAME
        
        # Rate limiting
        self.rate_limit_per_minute = config.EMAIL_RATE_LIMIT_PER_MINUTE
        self.email_timestamps = deque()
        
        # Template engine
        try:
            self.template_env = Environment(
                loader=FileSystemLoader(config.EMAIL_TEMPLATE_DIR),
                autoescape=True
            )
            # Add custom filters
            self.template_env.filters['alert_level_class'] = self._alert_level_to_css_class
            logger.info(f"Template directory loaded: {config.EMAIL_TEMPLATE_DIR}")
        except Exception as e:
            logger.warning(f"Could not load template directory {config.EMAIL_TEMPLATE_DIR}: {e}")
            self.template_env = None
        
        # Default template fallback
        self.default_template = self._get_default_template()
        
    def _get_language(self) -> str:
        """
        Get the language for translations. Defaults to French as requested.
        
        Returns:
            Language code (default: 'fr')
        """
        return "fr"  # French as default language
        
    def _format_french_date(self, date_str: str) -> str:
        """
        Convert ISO date string to French human readable format.
        
        Args:
            date_str: ISO format date string
            
        Returns:
            French formatted date like "Jeudi 18 décembre à 9h50"
        """
        try:
            from datetime import datetime
            import locale
            
            # Parse the date string (handle both with and without microseconds)
            if 'T' in date_str:
                if '.' in date_str:
                    # Has microseconds
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                else:
                    # No microseconds
                    dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                # Just date
                dt = datetime.fromisoformat(date_str)
            
            # French day and month names
            days_fr = ["Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi", "Dimanche"]
            months_fr = ["janvier", "février", "mars", "avril", "mai", "juin",
                        "juillet", "août", "septembre", "octobre", "novembre", "décembre"]
            
            day_name = days_fr[dt.weekday()]
            month_name = months_fr[dt.month - 1]
            
            return f"{day_name} {dt.day} {month_name} à {dt.hour}h{dt.minute:02d}"
            
        except Exception as e:
            logger.warning(f"Error formatting date {date_str}: {e}")
            return date_str[:19]  # Fallback to truncated ISO format
    
    def _alert_level_to_css_class(self, level):
        """
        Convert alert level string to CSS class name.
        
        Args:
            level: Alert level string name (e.g., 'CRITICAL', 'WARNING', 'INFO')
            
        Returns:
            CSS class name string
        """
        level_mapping = {
            'CRITICAL': 'critical',
            'WARNING': 'medium',
            'INFO': 'low'
        }
        
        return level_mapping.get(level.upper(), 'info')
        
    def _get_default_template(self) -> Template:
        """Get the default email template."""
        default_html = """
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <title>Alert Notification</title>
            <style>
                body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
                .container { max-width: 600px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                .header { background-color: #dc3545; color: white; padding: 20px; margin: -30px -30px 20px -30px; border-radius: 8px 8px 0 0; }
                .header.resolved { background-color: #28a745; }
                .alert-high { border-left: 4px solid #dc3545; }
                .alert-critical { border-left: 4px solid #721c24; background-color: #f8d7da; }
                .alert-medium { border-left: 4px solid #fd7e14; }
                .alert-low { border-left: 4px solid #ffc107; }
                .alert-info { border-left: 4px solid #17a2b8; }
                .alert-box { padding: 15px; margin: 15px 0; border-radius: 4px; background-color: #f8f9fa; }
                .details { margin: 20px 0; }
                .details table { width: 100%; border-collapse: collapse; }
                .details th, .details td { padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }
                .footer { margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header{% if is_resolved %} resolved{% endif %}">
                    {% if is_resolved %}
                        <h1>{{ alert_resolved_notification }}</h1>
                    {% else %}
                        <h1>{{ alert_notification }}</h1>
                    {% endif %}
                </div>
                
                <div class="alert-box alert-{{ alert.level|alert_level_class }}">
                    <h2>{{ alert.name }}</h2>
                    <p><strong>{{ alert.message }}</strong></p>
                </div>
                
                <div class="details">
                    <h3>{{ alert_details }}</h3>
                    <table>
                        <tr><th>{{ device_id }}</th><td>{{ alert.device_id }}</td></tr>
                        <tr><th>{{ sensor_name }}</th><td>{{ alert.sensor_name }}</td></tr>
                        <tr><th>{{ severity }}</th><td>{{ alert.level }}</td></tr>
                        <tr><th>{{ alert_type }}</th><td>{{ translated_alarm_type }}</td></tr>
                        <tr><th>{{ field }}</th><td>{{ alert.field_name }}</td></tr>
                        <tr><th>{{ trigger_value }}</th><td>{{ alert.error_value }}</td></tr>
                        <tr><th>{{ condition }}</th><td>{{ formatted_threshold }}</td></tr>
                        <tr><th>{{ triggered_at }}</th><td>{{ formatted_triggered_at }}</td></tr>
                    </table>
                </div>
                
                <div class="footer">
                    <p>{{ email_footer_automated }}</p>
                    <p>{{ email_footer_no_reply }}</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        if self.template_env:
            return self.template_env.from_string(default_html)
        else:
            from jinja2 import Template, Environment
            # Create a temporary environment with the custom filter
            temp_env = Environment()
            temp_env.filters['alert_level_class'] = self._alert_level_to_css_class
            return temp_env.from_string(default_html)
    
    def _check_rate_limit(self) -> bool:
        """
        Check if we're within the rate limit for sending emails.
        
        Returns:
            True if we can send an email, False if rate limited
        """
        current_time = time.time()
        
        # Remove timestamps older than 1 minute
        while self.email_timestamps and current_time - self.email_timestamps[0] > 60:
            self.email_timestamps.popleft()
        
        # Check if we're at the limit
        if len(self.email_timestamps) >= self.rate_limit_per_minute:
            return False
        
        # Add current timestamp
        self.email_timestamps.append(current_time)
        return True
    
    def _create_smtp_connection(self) -> smtplib.SMTP:
        """Create and configure SMTP connection."""
        if self.smtp_use_ssl:
            smtp = smtplib.SMTP_SSL(self.smtp_host, self.smtp_port)
        else:
            smtp = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.smtp_use_tls:
                smtp.starttls()
        
        if self.smtp_username and self.smtp_password:
            smtp.login(self.smtp_username, self.smtp_password)
        
        return smtp
    
    def _render_email_template(self, alert_data: Dict[str, Any]) -> tuple[str, str]:
        """
        Render email template with alert data.
        
        Args:
            alert_data: Alert information dictionary
            
        Returns:
            Tuple of (subject, html_body)
        """
        try:
            # Try to load custom template first
            if self.template_env:
                try:
                    template = self.template_env.get_template(config.DEFAULT_EMAIL_TEMPLATE)
                    logger.debug(f"Successfully loaded external template: {config.DEFAULT_EMAIL_TEMPLATE}")
                except Exception as e:
                    logger.warning(f"Failed to load external template {config.DEFAULT_EMAIL_TEMPLATE}: {e}")
                    logger.info("Falling back to default template")
                    template = self.default_template
            else:
                logger.info("Template environment not available, using default template")
                template = self.default_template
            
            # Render template with alert data and translations
            lang = self._get_language()
            
            # Translate alarm type directly from Kafka AlertMessage (using enum values)
            translated_alarm_type = ""
            alarm_type = alert_data.get('alarm_type', '')
            if alarm_type == 'Measure':
                translated_alarm_type = translate("alarm_type_MEASURE", lang, "alarm_type_MEASURE")
            elif alarm_type == 'Status':
                translated_alarm_type = translate("alarm_type_STATUS", lang, "alarm_type_STATUS")
            else:
                translated_alarm_type = str(alarm_type)
            
            # Format threshold with operator symbol (math_operator is now the actual symbol)
            math_operator = alert_data.get('math_operator', '')
            threshold = alert_data.get('threshold', '')
            formatted_threshold = f"{math_operator} {threshold}"
            
            # Format date in French
            formatted_triggered_at = ""
            start_date = alert_data.get('start_date') or alert_data.get('triggered_at')
            if start_date:
                formatted_triggered_at = self._format_french_date(str(start_date))
            
            # Use the is_resolved flag from the alert data (set by alarm handler)
            is_resolved = alert_data.get('is_resolved', False)
            
            template_context = {
                'alert': alert_data,
                'is_resolved': is_resolved,
                'alert_notification': translate("alert_notification", lang, "alert_notification"),
                'alert_resolved_notification': translate("alert_resolved_notification", lang, "alert_resolved_notification"),
                'email_footer_automated': translate("email_footer_automated", lang, "email_footer_automated"),
                'email_footer_no_reply': translate("email_footer_no_reply", lang, "email_footer_no_reply"),
                'alert_details': translate("alert_details", lang, "alert_details"),
                'alert_id': translate("alert_id", lang, "alert_id"),
                'device_id': translate("device_id", lang, "device_id"),
                'sensor_name': translate("sensor_name", lang, "sensor_name"),
                'alert_type': translate("alert_type", lang, "alert_type"),
                'field': translate("field", lang, "field"),
                'trigger_value': translate("trigger_value", lang, "trigger_value"),
                'condition': translate("condition", lang, "condition"),
                'triggered_at': translate("triggered_at", lang, "triggered_at"),
                'severity': translate("severity", lang, "severity"),
                'device': translate("device", lang, "device"),
                'translated_alarm_type': translated_alarm_type,
                'formatted_threshold': formatted_threshold,
                'formatted_triggered_at': formatted_triggered_at
            }
            html_body = template.render(**template_context)
            
            # Generate subject - use the is_resolved flag
            alert_name = alert_data.get('name', 'Unknown')
            
            if is_resolved:
                subject = translate(
                    "alert_subject_resolved",
                    lang,
                    "alert_subject_resolved",
                    alert_name=alert_name,
                    device_id=alert_data.get('device_id', 'Unknown Device')
                )
            else:
                subject = translate(
                    "alert_subject",
                    lang,
                    "alert_subject",
                    alert_name=alert_name,
                    device_id=alert_data.get('device_id', 'Unknown Device')
                )
            
            return subject, html_body
            
        except Exception as e:
            logger.error(f"Error rendering email template: {e}")
            # Fallback to simple text
            subject = f"Alert: {alert_data.get('name', 'System Alert')}"
            html_body = f"""
            <html>
            <body>
            <h2>Alert Notification</h2>
            <p><strong>Message:</strong> {alert_data.get('message', 'Alert triggered')}</p>
            <p><strong>Device:</strong> {alert_data.get('device_id', 'Unknown')}</p>
            <p><strong>Level:</strong> {alert_data.get('level', 'Unknown')}</p>
            <p><strong>Time:</strong> {alert_data.get('start_date', 'Unknown')}</p>
            </body>
            </html>
            """
            return subject, html_body
    
    def send_alert_email(self, alert_data: Dict[str, Any], recipients: List[str]) -> bool:
        """
        Send alert email to recipients.
        
        Args:
            alert_data: Alert information dictionary
            recipients: List of email addresses to send to
            
        Returns:
            True if email was sent successfully, False otherwise
        """
        if not recipients:
            logger.warning("No recipients specified for alert email")
            return False
        
        # Check rate limit
        if not self._check_rate_limit():
            logger.warning("Email rate limit exceeded, skipping email")
            return False
        
        try:
            # Render email content
            subject, html_body = self._render_email_template(alert_data)
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = Header(subject, 'utf-8')
            msg['From'] = f"{self.from_name} <{self.from_email}>"
            msg['To'] = ', '.join(recipients)
            
            # Add HTML content
            html_part = MIMEText(html_body, 'html', 'utf-8')
            msg.attach(html_part)
            
            # Send email
            with self._create_smtp_connection() as smtp:
                smtp.send_message(msg, to_addrs=recipients)
            
            logger.info(f"Alert email sent to {', '.join(recipients)} for alert {alert_data.get('id')}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send alert email: {e}")
            return False
    
    def send_alert_email_with_retry(self, alert_data: Dict[str, Any], recipients: List[str]) -> bool:
        """
        Send alert email with retry logic.
        
        Args:
            alert_data: Alert information dictionary
            recipients: List of email addresses to send to
            
        Returns:
            True if email was sent successfully, False after all retries failed
        """
        for attempt in range(config.MAX_RETRY_ATTEMPTS):
            try:
                if self.send_alert_email(alert_data, recipients):
                    return True
                    
                if attempt < config.MAX_RETRY_ATTEMPTS - 1:
                    logger.warning(f"Email send attempt {attempt + 1} failed, retrying in {config.RETRY_DELAY_SECONDS} seconds")
                    time.sleep(config.RETRY_DELAY_SECONDS)
                    
            except Exception as e:
                logger.error(f"Email send attempt {attempt + 1} failed with error: {e}")
                if attempt < config.MAX_RETRY_ATTEMPTS - 1:
                    time.sleep(config.RETRY_DELAY_SECONDS)
        
        logger.error(f"Failed to send alert email after {config.MAX_RETRY_ATTEMPTS} attempts")
        return False
    
    def test_smtp_connection(self) -> bool:
        """
        Test SMTP connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            with self._create_smtp_connection() as smtp:
                logger.info("SMTP connection test successful")
                return True
        except Exception as e:
            logger.error(f"SMTP connection test failed: {e}")
            return False 