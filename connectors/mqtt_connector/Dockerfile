FROM andy67/custom:python-base3.12-slim


# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY connectors/mqtt_connector/requirements.txt .
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy the connector scripts
COPY connectors/mqtt_connector/ /app/mqtt_connector/

# The entrypoint script that will run the specified connector
COPY connectors/mqtt_connector/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"] 