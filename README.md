# Synthos ML Backend

Microservices architecture for ML validation platform.

## Structure

```
validation_service/    # Port 50051 - Cascade training & diversity analysis
collapse_service/      # Port 50053 - Collapse detection & recommendations
go_backend/            # Port 8080  - API Gateway
job_orchestrator/      # Port 50052 - Job coordination
data_service/          # Port 50054 - Dataset management
ml_backend/            # Original monolithic code (reference)
```

## Quick Start

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f validation-service
docker-compose logs -f collapse-service

# Stop all
docker-compose down
```

## Services

### Python Services (Ready)

**validation_service/** - Complete self-contained service
- Cascade training (18 models across 3 tiers)
- Diversity analysis with stratification
- All code in one folder

**collapse_service/** - Complete self-contained service
- 8-dimensional collapse detection
- Problem localization
- Fix recommendations
- All code in one folder

### Go Services (In Development)

**go_backend/** - REST API Gateway  
**job_orchestrator/** - Job queue management  
**data_service/** - Dataset storage and retrieval

## Development

```bash
# Run service locally
cd validation_service
pip install -r requirements.txt
python server.py

# Rebuild service
docker-compose build validation-service
docker-compose up -d validation-service
```

## Infrastructure

- PostgreSQL (database)
- Redis (cache/queue)
- MinIO (S3-compatible storage)
- Prometheus (monitoring)

Each service is independent with its own Dockerfile and dependencies.
