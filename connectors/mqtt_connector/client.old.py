import os
import json
import paho.mqtt.client as mqtt
from kafka import KafkaProducer
import logging
import datetime
import tempfile
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get configuration from environment variables
BROKER_HOST = os.getenv('BROKER_HOST')
BROKER_PORT = int(os.getenv('BROKER_PORT', '1883'))
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
MQTT_TOPICS = os.getenv('MQTT_TOPICS', '#').split(',')  # Default to all topics

# Authentication configuration
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

# TLS/SSL Configuration
USE_TLS = os.getenv('USE_TLS', 'false').lower() == 'true'
CA_CERT_CONTENT = os.getenv('CA_CERT_CONTENT')  # Base64 encoded certificate content
CLIENT_CERT_CONTENT = os.getenv('CLIENT_CERT_CONTENT')  # Base64 encoded client certificate content
CLIENT_KEY_CONTENT = os.getenv('CLIENT_KEY_CONTENT')  # Base64 encoded client key content

def create_temp_cert_files():
    """Create temporary certificate files from environment variables."""
    temp_files = {}
    
    try:
        if USE_TLS:
            if CA_CERT_CONTENT:
                ca_cert = tempfile.NamedTemporaryFile(delete=False, suffix='.crt')
                ca_cert.write(base64.b64decode(CA_CERT_CONTENT))
                ca_cert.close()
                temp_files['ca_certs'] = ca_cert.name
                logger.info("Created temporary CA certificate file")

            if CLIENT_CERT_CONTENT and CLIENT_KEY_CONTENT:
                # Create client certificate file
                client_cert = tempfile.NamedTemporaryFile(delete=False, suffix='.crt')
                client_cert.write(base64.b64decode(CLIENT_CERT_CONTENT))
                client_cert.close()
                temp_files['certfile'] = client_cert.name
                logger.info("Created temporary client certificate file")

                # Create client key file
                client_key = tempfile.NamedTemporaryFile(delete=False, suffix='.key')
                client_key.write(base64.b64decode(CLIENT_KEY_CONTENT))
                client_key.close()
                temp_files['keyfile'] = client_key.name
                logger.info("Created temporary client key file")

    except Exception as e:
        logger.error(f"Error creating temporary certificate files: {e}")
        cleanup_temp_files(temp_files)
        raise

    return temp_files

def cleanup_temp_files(temp_files):
    """Clean up temporary certificate files."""
    for file_path in temp_files.values():
        try:
            os.unlink(file_path)
            logger.debug(f"Removed temporary file: {file_path}")
        except Exception as e:
            logger.error(f"Error removing temporary file {file_path}: {e}")

def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the MQTT broker."""
    rc_codes = {
        0: "Connection successful",
        1: "Connection refused - incorrect protocol version",
        2: "Connection refused - invalid client identifier",
        3: "Connection refused - server unavailable",
        4: "Connection refused - bad username or password",
        5: "Connection refused - not authorized"
    }
    
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
        for topic in MQTT_TOPICS:
            topic = topic.strip()
            client.subscribe(topic)
            logger.info(f"Subscribed to topic: {topic}")
    else:
        error_message = rc_codes.get(rc, f"Unknown error code: {rc}")
        logger.error(f"Failed to connect to MQTT broker: {error_message}")
        if rc == 4:
            logger.error("Please check your MQTT_USERNAME and MQTT_PASSWORD")
        elif rc == 5:
            logger.error("Please check your certificates and/or credentials")

def on_disconnect(client, userdata, rc):
    """Callback for when the client disconnects from the MQTT broker."""
    if rc != 0:
        logger.warning("Unexpected disconnection from MQTT broker")
    else:
        logger.info("Disconnected from MQTT broker")

def on_message(client, userdata, msg):
    """Callback for when a message is received from the MQTT broker."""
    try:
        # Parse the message payload
        payload = msg.payload.decode()
        
        # Create a message object with topic and payload
        message = {
            "topic": msg.topic,
            "payload": payload,
            "timestamp": str(datetime.datetime.now()),
            "broker": BROKER_HOST,
            "port": BROKER_PORT
        }
        
        # Send to Kafka
        producer.send(
            KAFKA_TOPIC,
            value=json.dumps(message).encode()
        )
        logger.debug(f"Forwarded message from MQTT topic {msg.topic} to Kafka topic {KAFKA_TOPIC}")
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")

def main():
    """Main function to set up MQTT client and Kafka producer."""
    temp_files = {}
    
    try:
        # Set up Kafka producer
        global producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"Connected to Kafka bootstrap servers: {KAFKA_BOOTSTRAP}")
        
        # Set up MQTT client
        client = mqtt.Client()

        # Configure authentication if credentials are provided
        if MQTT_USERNAME and MQTT_PASSWORD:
            logger.info(f"Configuring MQTT authentication for user: {MQTT_USERNAME}")
            client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

        # Configure callbacks
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        # Configure TLS if enabled
        if USE_TLS:
            logger.info("TLS is enabled, configuring certificates")
            temp_files = create_temp_cert_files()
            
            client.tls_set(**temp_files)
            logger.info("TLS configuration completed")
        
        # Connect to MQTT broker
        logger.info(f"Connecting to MQTT broker {BROKER_HOST}:{BROKER_PORT}")
        client.connect(BROKER_HOST, BROKER_PORT, 60)
        
        # Start the MQTT loop
        logger.info(f"Starting MQTT connection loop for topics: {MQTT_TOPICS}")
        client.loop_forever()
        
    except Exception as e:
        logger.error(f"Error in main loop: {e}")
        raise
    finally:
        # Clean up temporary certificate files
        if temp_files:
            cleanup_temp_files(temp_files)

if __name__ == "__main__":
    main() 