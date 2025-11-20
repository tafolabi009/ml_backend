# Production Dockerfile for ML Backend Services
# Serves ValidationEngine, CollapseEngine, and DataService via gRPC

FROM nvidia/cuda:12.2.0-runtime-ubuntu22.04

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-dev \
    python3-pip \
    git \
    curl \
    wget \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.11 as default
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Upgrade pip
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy and install ML packages (resonance_nn)
COPY packages/resonance_nn-0.1.0-py3-none-any.whl /app/packages/
RUN pip install --no-cache-dir /app/packages/resonance_nn-0.1.0-py3-none-any.whl

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY proto/ ./proto/
COPY server.py ./

# Create necessary directories
RUN mkdir -p /models /cache /data/signatures

# Expose gRPC ports
EXPOSE 50051 50052 50054

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import grpc; channel = grpc.insecure_channel('localhost:50051'); grpc.channel_ready_future(channel).result(timeout=5)" || exit 1

# Run server
CMD ["python", "server.py"]
