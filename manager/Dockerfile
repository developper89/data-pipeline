FROM python:3.12-slim

WORKDIR /app

# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the manager code
# COPY manager/ /app/manager/

CMD ["python", "/app/manager/manager.py"] 