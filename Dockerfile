# Minimal Dockerfile for reproducible builds and smoke runs
# Uses a slim Python base and installs only test/runtime deps needed

FROM python:3.11-slim

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
# Install minimal runtime requirements; avoid heavy optional packages in CI
RUN pip install --no-cache-dir -r requirements.txt

# Copy repository
COPY . /app

# Default command runs a quick smoke test (can be overridden)
CMD ["pytest", "-q"]
