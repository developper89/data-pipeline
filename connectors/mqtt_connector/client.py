# mqtt_connector/client.py
import logging
import asyncio
import paho.mqtt.client as paho
from paho import mqtt
import base64
import ssl
import tempfile
import os
import time
import json
from shared.models.common import RawMessage
from shared.models.translation import RawData, TranslationResult
from shared.translation.manager import TranslationManager
from shared.translation.factory import TranslatorFactory
from shared.config_loader import get_translator_configs

from kafka_producer import KafkaMsgProducer
import config

logger = logging.getLogger(__name__)

class CertificateManager:
    def __init__(self):
        self.temp_files = []

    def create_temp_cert_file(self, cert_content):
        """Create a temporary file with the certificate content."""
        if not cert_content:
            return None
        try:
            # Create temp file
            temp = tempfile.NamedTemporaryFile(delete=False, suffix='.pem')
            temp.write(base64.b64decode(cert_content))
            temp.close()
            self.temp_files.append(temp.name)
            return temp.name
        except Exception as e:
            logger.error(f"Failed to create temporary certificate file: {e}")
            return None

    def cleanup(self):
        """Clean up temporary certificate files."""
        for temp_file in self.temp_files:
            try:
                os.unlink(temp_file)
                logger.debug(f"Removed temporary certificate file: {temp_file}")
            except Exception as e:
                logger.error(f"Failed to remove temporary file {temp_file}: {e}")
        self.temp_files = []

class MQTTClientWrapper:
    def __init__(self, kafka_producer: KafkaMsgProducer, event_loop=None):
        self.kafka_producer = kafka_producer
        self.cert_manager = CertificateManager()
        self.event_loop = event_loop  # Reference to main async event loop
        # Use MQTTv311 instead of MQTTv5 as the broker may not support v5
        self.client = paho.Client(client_id=config.MQTT_CLIENT_ID, protocol=paho.MQTTv311)
        self._setup_callbacks()
        self._setup_connection()
        self._connected = False
        
        # Initialize translation manager
        self.translation_manager = self._initialize_translation_manager()

    def _initialize_translation_manager(self) -> TranslationManager:
        """Initialize the translation manager with configured translators."""
        try:
            # Get translator configurations for MQTT connector
            translator_configs = get_translator_configs('mqtt-connector')
            
            if not translator_configs:
                logger.warning("No translator configurations found for MQTT connector")
                return TranslationManager([])
            
            translators = []
            for translator_config in translator_configs:
                translator = TranslatorFactory.create_translator(translator_config)
                if translator:
                    translators.append(translator)
                    logger.info(f"Loaded translator: {translator.__class__.__name__}")
                else:
                    logger.warning(f"Failed to create translator from config: {translator_config}")
            
            logger.info(f"Initialized TranslationManager with {len(translators)} translator(s)")
            return TranslationManager(translators)
            
        except Exception as e:
            logger.error(f"Failed to initialize translation manager: {e}")
            return TranslationManager([])

    def _extract_device_id_using_translation(self, topic: str, payload_bytes: bytes) -> TranslationResult:
        """Extract device ID using the translation layer."""
        try:
            # Create RawData for translation
            # Split topic path into components for pattern matching
            raw_data = RawData(
                payload_bytes=payload_bytes,
                protocol='mqtt',
                path=topic,  # Generic path components for protocol-agnostic translators
                metadata={
                    'mqtt_topic': topic,  # Keep for backward compatibility
                    'path_string': topic  # Also provide full path as string
                }
            )
            
            # Use translation manager to extract device ID
            result = self.translation_manager.extract_device_id(raw_data)
            
            if result.success and result.device_id:
                logger.debug(f"Successfully extracted device ID '{result.device_id}' from topic '{topic}'")
                return result
            else:
                logger.warning(f"Failed to extract device ID from topic '{topic}': {result.error}")
                # Fallback to using the entire topic as device ID
                raise Exception(f"Failed to extract device ID from topic '{topic}': {result.error}")
                
        except Exception as e:
            logger.error(f"Error during device ID extraction: {e}")
            # Fallback to using the entire topic as device ID
            raise Exception(f"Failed to extract device ID from topic '{topic}': {e}")

    def _setup_callbacks(self):
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log

    def _setup_connection(self):
        # Set up authentication if provided
        if config.MQTT_USERNAME and config.MQTT_PASSWORD:
            self.client.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)

        # Set up TLS if enabled
        if config.USE_TLS:
            try:
                ca_cert_path = None
                if config.CA_CERT_CONTENT:
                    ca_cert_path = self.cert_manager.create_temp_cert_file(config.CA_CERT_CONTENT)
                
                if ca_cert_path:
                    self.client.tls_set(
                        ca_certs=ca_cert_path,
                        cert_reqs=ssl.CERT_REQUIRED,
                        tls_version=ssl.PROTOCOL_TLS,  # Using more compatible TLS version
                        ciphers=None
                    )
                    # Allow insecure TLS for testing if needed
                    # Uncomment this line only for development/testing
                    # self.client.tls_insecure_set(True)
                    logger.info("TLS configuration set up successfully")
                else:
                    logger.warning("TLS enabled but no CA certificate provided")
            except Exception as e:
                logger.error(f"Failed to set up TLS configuration: {e}")
                raise

    def connect(self):
        try:
            logger.info(f"Connecting to MQTT Broker {config.MQTT_BROKER_HOST}:{config.MQTT_BROKER_PORT}")
            self.client.connect(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT, keepalive=60)
        except Exception as e:
            logger.exception(f"Failed to initiate MQTT connection: {e}")
            raise

    def start_loop(self):
        logger.info("Starting MQTT client loop...")
        self.client.loop_forever()

    def stop_loop(self):
        logger.info("Stopping MQTT client loop...")
        self.client.loop_stop()
        logger.info("Disconnecting MQTT client...")
        self.client.disconnect()
        logger.info("Cleaning up certificates...")
        self.cert_manager.cleanup()

    def publish_message(self, topic: str, payload: dict, qos=None, retain=None):
        """
        Publish a message to an MQTT topic.
        
        Args:
            topic: The MQTT topic to publish to
            payload: The message payload (dict)
            qos: Quality of Service (0, 1, or 2)
            retain: Whether to retain the message
            
        Returns:
            True if the message was queued for sending, False otherwise
        """
        if not self._connected:
            logger.error("Cannot publish message: MQTT client not connected")
            return False
            
        # Set default QoS and retain if not specified
        if qos is None:
            qos = config.MQTT_DEFAULT_QOS
        if retain is None:
            retain = config.MQTT_DEFAULT_RETAIN
            
        try:
            # Convert dict payload to JSON string
            payload = json.dumps(payload)
            
            logger.debug(f"Publishing to MQTT topic '{topic}' (QoS: {qos}, Retain: {retain}), payload: {payload}")
            return True
            result = self.client.publish(topic, payload, qos=qos, retain=retain)
            
            # Check if the message was queued successfully
            if result.rc != paho.MQTT_ERR_SUCCESS:
                logger.error(f"Failed to queue MQTT message: {paho.error_string(result.rc)}")
                return False
                
            return True
        except Exception as e:
            logger.exception(f"Error publishing MQTT message: {e}")
            # Schedule the async error publish
            if self.event_loop and not self.event_loop.is_closed():
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.kafka_producer.publish_error("MQTT Publish Error", str(e), {"topic": topic}),
                        self.event_loop
                    )
                except Exception as publish_error:
                    logger.error(f"Failed to schedule error publish: {publish_error}")
            else:
                logger.warning("Cannot publish error to Kafka: no event loop reference available")
            return False

    # --- Callback Implementations ---

    def on_connect(self, client, userdata, flags, rc, properties=None):
        # Extended reason codes dictionary to include MQTTv5 codes
        rc_codes = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized",
            # MQTTv5 specific reason codes
            16: "No matching subscribers",
            17: "No subscription existed",
            24: "Connection failed",
            129: "Malformed packet",
            130: "Protocol error",
            131: "Implementation specific error",
            132: "Unsupported protocol version",
            133: "Client identifier not valid",
            134: "Bad username or password",
            135: "Not authorized",
            136: "Server unavailable",
            137: "Server busy",
            138: "Banned",
            139: "Server shutting down",
            140: "Bad authentication method",
            141: "Keep alive timeout",
            142: "Session taken over",
            143: "Topic filter invalid",
            144: "Topic name invalid",
            145: "Packet identifier in use",
            146: "Packet identifier not found",
            147: "Receive maximum exceeded",
            148: "Topic alias invalid",
            149: "Packet too large",
            150: "Message rate too high",
            151: "Quota exceeded",
            152: "Administrative action",
            153: "Payload format invalid",
            154: "Retain not supported",
            155: "QoS not supported",
            156: "Use another server",
            157: "Server moved",
            158: "Shared subscriptions not supported",
            159: "Connection rate exceeded",
            160: "Maximum connect time",
            161: "Subscription identifiers not supported",
            162: "Wildcard subscriptions not supported"
        }
        
        # MQTTv5 returns a ReasonCode object for rc, so we need to convert it to an int
        if isinstance(rc, int):
            rc_int = rc
        else:
            # For MQTTv5, rc is a ReasonCode object with a 'value' attribute
            rc_int = rc.value
            
        if rc_int == 0:
            self._connected = True
            logger.info("Successfully connected to MQTT Broker")
            # Subscribe to topics
            topics = [topic.strip() for topic in config.MQTT_TOPIC_SUBSCRIBE_PATTERN.split(',')]
            for topic in topics:
                logger.info(f"Subscribing to MQTT topic: {topic}")
                client.subscribe(topic, qos=1)
        else:
            self._connected = False
            error_message = rc_codes.get(rc_int, f"Unknown error code: {rc_int}")
            logger.error(f"Failed to connect to MQTT broker: {error_message}")
            if rc_int == 4 or rc_int == 134:
                logger.error("Please check your MQTT_USERNAME and MQTT_PASSWORD")
            elif rc_int == 5 or rc_int == 135:
                logger.error("Please check your certificates and/or credentials")
            elif rc_int == 132:
                logger.error("Unsupported protocol version. The broker may not support MQTTv5.")
            elif rc_int == 136:
                logger.error("Server unavailable. Please check if the broker is running.")

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        # For MQTTv5, granted_qos might be a list of ReasonCodes instead of integers
        if granted_qos and not isinstance(granted_qos[0], int):
            granted_qos_values = [qos.value for qos in granted_qos]
            logger.info(f"Subscribed (mid={mid}): QoS={granted_qos_values}")
        else:
            logger.info(f"Subscribed (mid={mid}): QoS={granted_qos}")

    def on_disconnect(self, client, userdata, rc, properties=None):
        self._connected = False
        
        # MQTTv5 returns a ReasonCode object for rc, so we need to convert it to an int
        if isinstance(rc, int):
            rc_int = rc
        else:
            # For MQTTv5, rc is a ReasonCode object with a 'value' attribute
            rc_int = rc.value
            
        logger.warning(f"Disconnected from MQTT Broker with result code {rc_int}")

    def on_message(self, client, userdata, msg):
        try:
            # Use translation layer to extract device ID
            result = self._extract_device_id_using_translation(msg.topic, msg.payload)
            
            payload_bytes = msg.payload
            
            # Convert binary payload to hex string to match RawMessage model expectation
            payload_str = payload_bytes.hex() if isinstance(payload_bytes, bytes) else str(payload_bytes)
            
            # Create RawMessage - with payload as string
            raw_message = RawMessage(
                device_id=result.device_id,
                payload_hex=payload_str,  # Updated field name from payload to payload_hex
                protocol="mqtt",
                device_type=result.device_type,  # Pass device_type from translation result
                metadata={
                    "protocol": "mqtt",
                    "mqtt_topic": msg.topic,
                    "mqtt_qos": msg.qos,
                    "mqtt_retain": msg.retain,
                    "translator_used": result.translator_used,
                    "manufacturer": result.translator.manufacturer,
                }
            )
            
            # Log specific device for debugging
            if result.device_id == "2207001":
                logger.info(f"Received message on topic '{msg.topic}' (QoS {msg.qos})")
                logger.info(f"Extracted device_id: {result.device_id}")
                logger.info(f"payload_hex is: {raw_message.payload_hex}")
            
            # Publish to Kafka - schedule the async call in the main event loop
            if self.event_loop and not self.event_loop.is_closed():
                try:
                    # Schedule the coroutine to run in the main event loop from this thread
                    asyncio.run_coroutine_threadsafe(
                        self.kafka_producer.publish_raw_message(raw_message),
                        self.event_loop
                    )
                except Exception as e:
                    logger.error(f"Failed to schedule Kafka publish: {e}")
            else:
                logger.error("Cannot publish to Kafka: no event loop reference available")

        except Exception as e:
            logger.exception(f"Error processing MQTT message from topic {msg.topic}: {e}")
            # Schedule the async error publish
            if self.event_loop and not self.event_loop.is_closed():
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.kafka_producer.publish_error("MQTT Message Processing Error", str(e), {"topic": msg.topic}),
                        self.event_loop
                    )
                except Exception as publish_error:
                    logger.error(f"Failed to schedule error publish: {publish_error}")
            else:
                logger.warning("Cannot publish error to Kafka: no event loop reference available")

    def on_log(self, client, userdata, level, buf):
        if level == mqtt.MQTT_LOG_INFO:
            logger.info(f"PAHO-INFO: {buf}")
        elif level == mqtt.MQTT_LOG_NOTICE:
            logger.info(f"PAHO-NOTICE: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            logger.warning(f"PAHO-WARN: {buf}")
        elif level == mqtt.MQTT_LOG_ERR:
            logger.error(f"PAHO-ERR: {buf}")
        elif level == mqtt.MQTT_LOG_DEBUG:
            logger.debug(f"PAHO-DEBUG: {buf}")