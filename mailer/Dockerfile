FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules
COPY ../shared ./shared

# Copy application code
COPY . ./mailer

# Set Python path to include shared modules
ENV PYTHONPATH=/app

# Create templates directory
RUN mkdir -p /app/templates

# Set working directory to mailer
WORKDIR /app/mailer

# Expose port for health checks (if needed in future)
EXPOSE 8080

# Copy and set up entrypoint
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

# Run the service
ENTRYPOINT ["/app/docker-entrypoint.sh"] 