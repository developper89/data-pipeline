FROM andy67/custom:python-base3.12-slim


# Install system dependencies including protoc for dynamic protobuf compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    nano \
    protobuf-compiler \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY connectors/coap_connector/requirements.txt .
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy the connector scripts
COPY connectors/coap_connector/ /app/coap_connector/

# Set working directory
WORKDIR /app

# The entrypoint script that will run the specified connector
COPY connectors/coap_connector/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"] 