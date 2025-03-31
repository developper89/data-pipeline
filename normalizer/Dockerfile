FROM python:3.12-slim

WORKDIR /app

# Install required system packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY normalizer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the normalizer code
COPY normalizer/ /app/normalizer/

CMD ["python", "/app/normalizer/normalizer_service.py"] 