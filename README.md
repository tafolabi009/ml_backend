# Synthos Backend - ML Validation Platform

**Status:** Alpha / Experimental  
**Last Updated:** November 11, 2025

This repository contains the complete backend infrastructure for the Synthos ML validation platform, consisting of:
- **Go Backend** - API Gateway for customer-facing REST APIs
- **ML Backend** - Python-based ML validation engine with collapse detection

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     API GATEWAY (Go)                         │
│                  - REST API (Customer-facing)                │
│                  - JWT Authentication                        │
│                  - Rate limiting                             │
└─────────────────────────────┬───────────────────────────────┘
                              │ gRPC
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
        ▼                     ▼                     ▼
┌──────────────┐    ┌──────────────────┐    ┌──────────────┐
│ Dataset      │    │ Validation       │    │ Collapse     │
│ Service      │    │ Engine           │    │ Engine       │
│ (Python)     │    │ (Python)         │    │ (Python)     │
└──────────────┘    └──────────────────┘    └──────────────┘
        │                     │                     │
        └─────────────────────┴─────────────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │    PostgreSQL + Redis + S3    │
              └───────────────────────────────┘
```

## Repository Structure

```
backend/
├── go_backend/              # Go API Gateway
│   ├── cmd/api/             # Main application entry point
│   ├── internal/            # Internal packages
│   │   ├── handlers/        # HTTP request handlers
│   │   ├── middleware/      # HTTP middleware
│   │   ├── models/          # Data models
│   │   ├── auth/            # Authentication utilities
│   │   ├── database/        # Database layer
│   │   ├── grpc/            # gRPC clients
│   │   └── storage/         # S3 integration
│   ├── pkg/                 # Public packages
│   ├── proto/               # Protocol buffers
│   └── README.md
│
├── ml_backend/              # Python ML validation engine
│   ├── src/                 # Source code
│   │   ├── collapse_engine/      # Collapse detection
│   │   ├── data_processors/      # Data processing
│   │   ├── grpc_services/        # gRPC servers
│   │   ├── storage/              # Storage providers
│   │   ├── utils/                # Utilities
│   │   └── validation_engine/    # Validation logic
│   ├── tests/               # Test suite
│   ├── config/              # Configuration files
│   ├── docs/                # Documentation
│   └── README.md
│
├── docker-compose.yml       # Full stack deployment
└── README.md               # This file
```

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Go 1.21+ (for local development)
- Python 3.11+ (for local development)
- PostgreSQL 15+
- Redis 7+

### Running with Docker Compose

```bash
# Clone the repository
git clone https://github.com/tafolabi009/backend.git
cd backend

# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f api-gateway
docker-compose logs -f ml-backend

# Stop all services
docker-compose down
```

The API Gateway will be available at `http://localhost:8080`.

### Running Locally (Development)

#### Go API Gateway

```bash
cd go_backend

# Install dependencies
go mod download

# Copy environment file
cp .env.example .env

# Run the server
go run cmd/api/main.go
```

#### Python ML Backend

```bash
cd ml_backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run tests
./run_tests.sh

# Run the gRPC server
python src/grpc_services/validation_server.py
```

## API Documentation

### Base URL
```
https://api.synthos.ai/v1
```

### Authentication
All protected endpoints require a JWT token in the Authorization header:
```
Authorization: Bearer <access_token>
```

### Key Endpoints

#### Authentication
- `POST /auth/register` - Register new user
- `POST /auth/login` - User login
- `POST /auth/refresh` - Refresh access token

#### Datasets
- `POST /datasets/upload` - Initiate dataset upload
- `GET /datasets` - List all datasets
- `GET /datasets/:id` - Get dataset details
- `DELETE /datasets/:id` - Delete dataset

#### Validations
- `POST /validations/create` - Create validation job
- `GET /validations/:id` - Get validation results
- `GET /validations/:id/collapse-details` - Get collapse analysis
- `GET /validations/:id/recommendations` - Get fix recommendations

For complete API documentation, see [docs/synthos-api-architecture.md](ml_backend/docs/synthos-api-architecture.md).

## Development Workflow

### Making Changes to Go Backend

```bash
cd go_backend

# Create a new handler
touch internal/handlers/new_feature.go

# Format code
go fmt ./...

# Run tests
go test ./...

# Build
go build -o api cmd/api/main.go
```

### Making Changes to ML Backend

```bash
cd ml_backend

# Add new functionality
touch src/new_module/feature.py

# Format code
ruff format .

# Run tests
pytest tests/ -v --cov=src

# Run linting
ruff check .
mypy src/
```

## Testing

### Go Backend Tests
```bash
cd go_backend
go test ./... -v -cover
```

### Python ML Backend Tests
```bash
cd ml_backend
./run_tests.sh  # Runs all tests with coverage
```

### Integration Tests
```bash
# Start all services
docker-compose up -d

# Run integration tests
cd ml_backend
pytest tests/integration/ -v
```

## Production Deployment

### Environment Variables

#### Go API Gateway
- `ENVIRONMENT` - production
- `PORT` - 8080
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string
- `JWT_SECRET` - **REQUIRED** - Strong random string
- `AWS_REGION` - AWS region
- `S3_BUCKET` - S3 bucket name
- `VALIDATION_ENGINE_ADDR` - gRPC address

#### ML Backend
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string
- `AWS_ACCESS_KEY_ID` - AWS credentials
- `AWS_SECRET_ACCESS_KEY` - AWS credentials
- `S3_BUCKET` - S3 bucket name

### Building for Production

```bash
# Build Go API Gateway
cd go_backend
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o api cmd/api/main.go

# Build Docker images
docker build -t synthos-api-gateway:latest go_backend/
docker build -t synthos-ml-backend:latest ml_backend/

# Push to registry
docker tag synthos-api-gateway:latest your-registry/synthos-api-gateway:latest
docker push your-registry/synthos-api-gateway:latest
```

### Kubernetes Deployment

```bash
# Apply Kubernetes manifests
kubectl apply -f ml_backend/deployment/kubernetes/

# Check deployment status
kubectl get pods -n synthos
kubectl get services -n synthos
```

## Monitoring & Observability

### Metrics
- Prometheus metrics exposed at `/metrics`
- Grafana dashboards available in `ml_backend/deployment/monitoring/`

### Logging
- Structured JSON logs
- Log levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
- Centralized logging with ELK stack (optional)

### Health Checks
- API Gateway: `GET /health`
- ML Backend: `GET /health` (gRPC health check)

## Security

### Best Practices
- ✅ JWT tokens with short expiration (15 minutes)
- ✅ HTTPS/TLS for all communications
- ✅ Password hashing with bcrypt
- ✅ Input validation on all endpoints
- ✅ CORS configuration
- 🚧 Rate limiting (in progress)
- 🚧 API key management (in progress)
- 🚧 Secrets management with Vault (planned)

### Known Security Issues
- JWT secret must be changed in production
- Database passwords should use secrets management
- S3 bucket policies need review

## Contributing

### Branching Strategy
- `main` - Production-ready code
- `develop` - Development branch
- `feature/*` - Feature branches
- `bugfix/*` - Bug fix branches

### Commit Messages
Follow conventional commits:
```
feat: Add warranty request endpoint
fix: Resolve JWT token expiration issue
docs: Update API documentation
test: Add unit tests for collapse detector
```

### Pull Request Process
1. Create feature branch from `develop`
2. Write tests for new functionality
3. Ensure all tests pass
4. Update documentation
5. Submit PR with detailed description
6. Wait for code review

## Known Issues & Limitations

### Current State: Alpha/Experimental

⚠️ **This system is NOT production-ready**

#### Critical Issues
- ❌ Database layer not implemented (using mock data)
- ❌ gRPC clients not connected to ML backend
- ❌ S3 integration incomplete
- ❌ Warranty system not implemented
- ❌ Report generation (PDF) not implemented
- ❌ WebSocket support for real-time updates missing
- ❌ Rate limiting not implemented
- ❌ Comprehensive test coverage needed (currently ~30%)

#### What Works
- ✅ REST API structure and routing
- ✅ JWT authentication and middleware
- ✅ Basic CRUD operations (mock data)
- ✅ Docker containerization
- ✅ ML validation engine (see ml_backend/PRODUCTION_IMPROVEMENTS.md)

## Roadmap

### Phase 1: Core Infrastructure (Current)
- [x] Go API Gateway skeleton
- [x] REST API endpoints (handlers)
- [x] JWT authentication
- [x] Docker Compose setup
- [ ] Database integration
- [ ] gRPC client implementation
- [ ] S3 integration

### Phase 2: ML Integration (1-2 months)
- [ ] Connect Go backend to Python validation engine
- [ ] Real-time progress updates via WebSocket
- [ ] Async job processing with RabbitMQ
- [ ] Result caching with Redis

### Phase 3: Production Features (2-3 months)
- [ ] Rate limiting and API quotas
- [ ] Warranty management system
- [ ] Report generation (PDF)
- [ ] Comprehensive monitoring
- [ ] 70%+ test coverage
- [ ] Security audit and hardening

### Phase 4: Scale & Polish (3-6 months)
- [ ] Kubernetes production deployment
- [ ] Load testing and optimization
- [ ] Horizontal scaling
- [ ] Advanced analytics
- [ ] Customer dashboard

## Performance

### Current Performance (Alpha)
- API latency: ~50-100ms (no database)
- Throughput: TBD (not benchmarked)
- ML validation: See ml_backend/TESTING_GUIDE.md

### Target Performance (Production)
- API latency: <500ms (p95)
- Throughput: 1000+ requests/second
- ML validation: 1B rows in <24 hours

## License

MIT License - See LICENSE file for details

## Support & Contact

- **Issues:** https://github.com/tafolabi009/backend/issues
- **Documentation:** [ml_backend/docs/](ml_backend/docs/)
- **Email:** support@synthos.ai (not yet active)

---

**Remember:** This is an **Alpha/Experimental** system. Do not use in production without:
1. Implementing database layer
2. Completing security hardening
3. Achieving 70%+ test coverage
4. Running comprehensive load tests
5. Security audit

See [ml_backend/PRODUCTION_IMPROVEMENTS.md](ml_backend/PRODUCTION_IMPROVEMENTS.md) for recent improvements and [ml_backend/TESTING_GUIDE.md](ml_backend/TESTING_GUIDE.md) for testing instructions.
