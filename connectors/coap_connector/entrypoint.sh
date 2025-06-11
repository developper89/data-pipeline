#!/bin/bash

# Exit on any error
set -e

# Function to validate required environment variables
validate_env() {
    local missing=()
    
    # Required variables
    [[ -z "$COAP_HOST" ]] && missing+=("COAP_HOST")
    [[ -z "$COAP_PORT" ]] && missing+=("COAP_PORT")
    [[ -z "$KAFKA_BOOTSTRAP_SERVERS" ]] && missing+=("KAFKA_BOOTSTRAP_SERVERS")
    [[ -z "$KAFKA_RAW_DATA_TOPIC" ]] && missing+=("KAFKA_RAW_DATA_TOPIC")
    [[ -z "$KAFKA_ERROR_TOPIC" ]] && missing+=("KAFKA_ERROR_TOPIC")
    [[ -z "$KAFKA_DEVICE_COMMANDS_TOPIC" ]] && missing+=("KAFKA_DEVICE_COMMANDS_TOPIC")
    [[ -z "$COAP_BASE_DATA_PATH" ]] && missing+=("COAP_BASE_DATA_PATH")
    
    # If any required variables are missing, exit with error
    if [ ${#missing[@]} -ne 0 ]; then
        echo "Error: Missing required environment variables:"
        printf '%s\n' "${missing[@]}"
        exit 1
    fi
}

# Validate environment variables
validate_env

# Execute the CoAP connector script
exec python /app/coap_connector/main.py 