FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy ALL service code
COPY . /app/

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install ML packages if they exist
RUN pip install --no-cache-dir ../ml_backend/packages/*.whl 2>/dev/null || true

# Create directories
RUN mkdir -p /data/signatures /cache

ENV PYTHONUNBUFFERED=1
ENV PORT=50053

EXPOSE 50053

CMD ["python", "server.py"]
