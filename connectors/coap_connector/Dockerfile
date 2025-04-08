FROM andy67/custom:python-base3.12-slim


# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    nano \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create data directory
RUN mkdir -p /app/data

# Copy requirements file
COPY connectors/coap_connector/requirements.txt .

# Install Python dependencies
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy application code
COPY connectors/coap_connector/ /app/coap_connector/

# Expose the CoAP port
EXPOSE 5683/udp

# Run the application
CMD ["python", "main.py"] 