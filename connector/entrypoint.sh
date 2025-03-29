#!/bin/bash

# Validate required environment variables
if [ -z "$CONNECTOR_SCRIPT_PATH" ]; then
    echo "Error: CONNECTOR_SCRIPT_PATH environment variable is required"
    exit 1
fi

if [ ! -f "$CONNECTOR_SCRIPT_PATH" ]; then
    echo "Error: Connector script not found at $CONNECTOR_SCRIPT_PATH"
    exit 1
fi

# Execute the connector script
exec python "$CONNECTOR_SCRIPT_PATH" 