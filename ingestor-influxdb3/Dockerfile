FROM andy67/custom:python-base3.12-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Make entrypoint script executable
RUN chmod +x /app/docker-entrypoint.sh

ENV PYTHONPATH="${PYTHONPATH}:/app"

# Set the entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command
CMD ["python", "/app/main.py"] 