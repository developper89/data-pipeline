FROM python:3.12-slim

WORKDIR /app

# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY connectors/mqtt_connector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the connector scripts
COPY connectors/mqtt_connector/ /app/mqtt_connector/

# The entrypoint script that will run the specified connector
COPY connectors/mqtt_connector/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"] 