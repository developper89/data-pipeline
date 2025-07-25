#!/bin/bash
set -e

# Install the SDK in development mode
uv pip install --system -e /app/packages/preservarium-sdk[dev,core,database,cache]
# Execute the passed command (default: starting the application)
exec "$@" 