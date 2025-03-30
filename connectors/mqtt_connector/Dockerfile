FROM python:3.12-slim

WORKDIR /app

# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY connector/mqtt/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the connector scripts
COPY connector/mqtt/scripts/ /app/connector_scripts/

# The entrypoint script that will run the specified connector
COPY connector/mqtt/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"] 