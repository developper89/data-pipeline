FROM andy67/custom:python-base3.12-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Make entrypoint script executable
COPY docker-entrypoint.sh /app/
RUN chmod +x /app/docker-entrypoint.sh

ENV PYTHONPATH="${PYTHONPATH}:/app"

# Set the entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]

# Default command
# CMD ["python", "-m", "cache_service.main"]
# CMD ["python", "/app/caching/main.py"]
CMD ["tail", "-f", "/dev/null"] 