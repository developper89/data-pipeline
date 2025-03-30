# mqtt_connector/client.py
import logging
import paho.mqtt.client as paho
from paho import mqtt
import base64
import ssl # For TLS support

from shared.models.common import RawMessage
from .kafka_producer import KafkaMsgProducer
from . import config

logger = logging.getLogger(__name__)

class MQTTClientWrapper:
    def __init__(self, kafka_producer: KafkaMsgProducer):
        self.kafka_producer = kafka_producer
        self.client = paho.Client(client_id=config.MQTT_CLIENT_ID, protocol=paho.MQTTv5)
        self._setup_callbacks()
        self._setup_connection()
        self._connected = False

    def _setup_callbacks(self):
        self.client.on_connect = self.on_connect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log # Optional: for detailed logging

    def _setup_connection(self):
        if config.MQTT_USERNAME and config.MQTT_PASSWORD:
            self.client.username_pw_set(config.MQTT_USERNAME, config.MQTT_PASSWORD)
        # Add TLS settings if needed based on env vars (e.g., CA_CERTS, CERTFILE, KEYFILE)
        # if config.MQTT_USE_TLS:
        #     self.client.tls_set(ca_certs=config.MQTT_CA_CERTS,
        #                         certfile=config.MQTT_CERTFILE,
        #                         keyfile=config.MQTT_KEYFILE,
        #                         cert_reqs=ssl.CERT_REQUIRED,
        #                         tls_version=ssl.PROTOCOL_TLS_CLIENT)
        #     self.client.tls_insecure_set(config.MQTT_TLS_INSECURE) # If needed

    def connect(self):
        try:
            logger.info(f"Connecting to MQTT Broker {config.MQTT_BROKER_HOST}:{config.MQTT_BROKER_PORT}")
            self.client.connect(config.MQTT_BROKER_HOST, config.MQTT_BROKER_PORT, keepalive=60)
        except Exception as e:
            logger.exception(f"Failed to initiate MQTT connection: {e}")
            # Implement retry logic if needed, or let the main loop handle it

    def start_loop(self):
        logger.info("Starting MQTT client loop...")
        # loop_forever() blocks, handles reconnects automatically.
        self.client.loop_forever()

    def stop_loop(self):
        logger.info("Stopping MQTT client loop...")
        self.client.loop_stop()
        logger.info("Disconnecting MQTT client...")
        self.client.disconnect()

    # --- Callback Implementations ---

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self._connected = True
            logger.info(f"Successfully connected to MQTT Broker with result code {rc}")
            # Subscribe upon successful connection
            logger.info(f"Subscribing to MQTT topic pattern: {config.MQTT_TOPIC_SUBSCRIBE_PATTERN}")
            client.subscribe(config.MQTT_TOPIC_SUBSCRIBE_PATTERN, qos=1) # Use QoS 1 for at-least-once
        else:
            self._connected = False
            logger.error(f"Failed to connect to MQTT Broker, return code: {rc}")
            # paho-mqtt handles retries internally in loop_forever

    def on_subscribe(self, client, userdata, mid, granted_qos, properties=None):
        logger.info(f"Subscribed (mid={mid}): QoS={granted_qos}")

    def on_disconnect(self, client, userdata, rc, properties=None):
         self._connected = False
         logger.warning(f"Disconnected from MQTT Broker with result code {rc}. Will attempt reconnection.")

    def on_message(self, client, userdata, msg):
        try:
            logger.debug(f"Received message on topic '{msg.topic}' (QoS {msg.qos}): {len(msg.payload)} bytes")

            # Extract device ID from topic (adjust logic based on your pattern)
            # Example: devices/DEVICE_ID/data -> extract DEVICE_ID
            topic_parts = msg.topic.split('/')
            if len(topic_parts) >= 2 and topic_parts[0] == 'devices' and topic_parts[-1] == 'data':
                 device_id = topic_parts[1]
            else:
                 logger.warning(f"Could not extract device ID from topic: {msg.topic}. Using topic as ID.")
                 device_id = msg.topic # Fallback or error

            payload_bytes = msg.payload

            # Create RawMessage
            raw_message = RawMessage(
                device_id=device_id,
                payload_b64=base64.b64encode(payload_bytes).decode('ascii'),
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
         # Map paho levels to Python logging levels if desired
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