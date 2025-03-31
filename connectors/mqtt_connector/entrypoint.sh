#!/bin/bash

# Exit on any error
set -e

# Function to validate required environment variables
validate_env() {
    local missing=()
    
    # Required variables
    [[ -z "$BROKER_HOST" ]] && missing+=("BROKER_HOST")
    [[ -z "$BROKER_PORT" ]] && missing+=("BROKER_PORT")
    [[ -z "$KAFKA_BOOTSTRAP" ]] && missing+=("KAFKA_BOOTSTRAP")
    [[ -z "$KAFKA_TOPIC" ]] && missing+=("KAFKA_TOPIC")
    [[ -z "$MQTT_TOPICS" ]] && missing+=("MQTT_TOPICS")
    
    # If using TLS, validate certificate
    if [[ "$USE_TLS" == "true" && -z "$CA_CERT_CONTENT" ]]; then
        missing+=("CA_CERT_CONTENT")
    fi
    
    # If any required variables are missing, exit with error
    if [ ${#missing[@]} -ne 0 ]; then
        echo "Error: Missing required environment variables:"
        printf '%s\n' "${missing[@]}"
        exit 1
    fi
}

# Validate environment variables
validate_env

# Check if CONNECTOR_SCRIPT_PATH is set
if [ -z "$CONNECTOR_SCRIPT_PATH" ]; then
    echo "Error: CONNECTOR_SCRIPT_PATH environment variable is not set"
    exit 1
fi

# Check if the script exists
if [ ! -f "$CONNECTOR_SCRIPT_PATH" ]; then
    echo "Error: Script not found at $CONNECTOR_SCRIPT_PATH"
    exit 1
fi

# Execute the specified connector script
exec python "$CONNECTOR_SCRIPT_PATH" 