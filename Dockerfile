FROM --platform=linux/amd64 python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create dagster home directory
RUN mkdir -p /app/dagster_home
ENV DAGSTER_HOME=/app/dagster_home

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project code
COPY . .

# Create dagster.yaml
RUN echo "telemetry:\n  enabled: false" > $DAGSTER_HOME/dagster.yaml

# Start the Dagster UI
CMD ["dagster", "dev", "-m", "ml_pipeline", "-h", "0.0.0.0"]