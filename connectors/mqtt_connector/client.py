# mqtt_connector/client.py
import logging
import paho.mqtt.client as paho
from paho import mqtt
import base64
import ssl
import tempfile
import os
import time
from shared.models.common import RawMessage

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
    def __init__(self, kafka_producer: KafkaMsgProducer):
        self.kafka_producer = kafka_producer
        self.cert_manager = CertificateManager()
        self.client = paho.Client(client_id=config.MQTT_CLIENT_ID, protocol=paho.MQTTv5)
        self._setup_callbacks()
        self._setup_connection()
        self._connected = False

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
                        tls_version=ssl.PROTOCOL_TLS_CLIENT,
                        ciphers=None
                    )
                    self.client.tls_insecure_set(False)
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

    # --- Callback Implementations ---

    def on_connect(self, client, userdata, flags, rc, properties=None):
        rc_codes = {
            0: "Connection successful",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized"
        }
        if rc == 0:
            self._connected = True
            logger.info("Successfully connected to MQTT Broker")
            # Subscribe to topics
            topics = [topic.strip() for topic in config.MQTT_TOPIC_SUBSCRIBE_PATTERN.split(',')]
            for topic in topics:
                logger.info(f"Subscribing to MQTT topic: {topic}")
                client.subscribe(topic, qos=1)
        else:
            self._connected = False
            error_message = rc_codes.get(rc, f"Unknown error code: {rc}")
            logger.error(f"Failed to connect to MQTT broker: {error_message}")
            if rc == 4:
                logger.error("Please check your MQTT_USERNAME and MQTT_PASSWORD")
            elif rc == 5:
                logger.error("Please check your certificates and/or credentials")

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logger.info(f"Subscribed (mid={mid}): QoS={granted_qos}")

    def on_disconnect(self, client, userdata, rc, properties=None):
        self._connected = False
        logger.warning(f"Disconnected from MQTT Broker with result code {rc}")

    def on_message(self, client, userdata, msg):
        try:
            logger.debug(f"Received message on topic '{msg.topic}' (QoS {msg.qos})")
            
            topic_parts = msg.topic.split('/')
            if len(topic_parts) >= 2 and topic_parts[0] == 'broker_data' and topic_parts[-1] == 'data':
                 device_id = topic_parts[2]
            else:
                 logger.warning(f"Could not extract device ID from topic: {msg.topic}. Using topic as ID.")
                 device_id = msg.topic # Fallback or error

            payload_bytes = msg.payload

            # Create RawMessage
            raw_message = RawMessage(
                device_id=device_id,
                payload=payload_bytes,
                protocol="mqtt",
                metadata={
                    "mqtt_topic": msg.topic,
                    "mqtt_qos": msg.qos,
                    "mqtt_retain": msg.retain,
                }
            )
            
            # Publish to Kafka
            self.kafka_producer.publish_raw_message(raw_message)

        except Exception as e:
            logger.exception(f"Error processing MQTT message from topic {msg.topic}: {e}")
            self.kafka_producer.publish_error("MQTT Message Processing Error", str(e), {"topic": msg.topic})

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