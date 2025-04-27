FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY ingestor/requirements.txt /app/ingestor/
RUN pip install --no-cache-dir -r /app/ingestor/requirements.txt

# Copy application code
COPY ingestor/ /app/ingestor/
COPY shared/ /app/shared/

# Make entrypoint script executable
COPY ingestor/docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

ENV PYTHONPATH="${PYTHONPATH}:/app"

# Set the entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command
CMD ["python", "-m", "ingestor.main"] 