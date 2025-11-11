# Synthos Go Backend - API Gateway

This is the Go-based API Gateway for the Synthos ML validation platform. It provides RESTful APIs for customer-facing operations and communicates with the Python ML backend via gRPC.

## Architecture

```
┌─────────────────────────────────────────┐
│         API Gateway (Go)                │
│  - REST API endpoints                   │
│  - JWT authentication                   │
│  - Request validation                   │
│  - Rate limiting                        │
└─────────────┬───────────────────────────┘
              │ gRPC
              ▼
┌─────────────────────────────────────────┐
│    ML Backend (Python)                  │
│  - Validation Engine                    │
│  - Collapse Detection                   │
│  - Model Training                       │
└─────────────────────────────────────────┘
```

## Features

### Implemented
- ✅ User authentication (register, login, token refresh)
- ✅ Dataset management (upload, list, get, delete)
- ✅ Validation jobs (create, list, get, results)
- ✅ Collapse analysis endpoints
- ✅ Recommendations endpoints
- ✅ Analytics endpoints
- ✅ JWT middleware
- ✅ CORS middleware
- ✅ Logging middleware
- ✅ Error handling

### In Progress
- 🚧 Database integration (PostgreSQL)
- 🚧 gRPC client for Python backend
- 🚧 S3 integration for file uploads
- 🚧 Warranty management
- 🚧 Report generation (PDF)
- 🚧 WebSocket support for real-time updates

### Planned
- 📋 Rate limiting
- 📋 Redis caching
- 📋 Metrics/monitoring (Prometheus)
- 📋 Swagger/OpenAPI documentation
- 📋 Unit tests
- 📋 Integration tests

## API Endpoints

### Authentication
- `POST /api/v1/auth/register` - Register new user
- `POST /api/v1/auth/login` - User login
- `POST /api/v1/auth/refresh` - Refresh access token

### Datasets (Protected)
- `POST /api/v1/datasets/upload` - Initiate dataset upload
- `POST /api/v1/datasets/:id/complete` - Complete upload
- `GET /api/v1/datasets` - List all datasets
- `GET /api/v1/datasets/:id` - Get dataset details
- `DELETE /api/v1/datasets/:id` - Delete dataset

### Validations (Protected)
- `POST /api/v1/validations/create` - Create validation job
- `GET /api/v1/validations` - List validations
- `GET /api/v1/validations/:id` - Get validation details
- `GET /api/v1/validations/:id/report` - Download report
- `GET /api/v1/validations/:id/certificate` - Download certificate
- `GET /api/v1/validations/:id/collapse-details` - Get collapse analysis
- `GET /api/v1/validations/:id/recommendations` - Get recommendations

### Warranties (Protected)
- `POST /api/v1/warranties/:validation_id/request` - Request warranty
- `GET /api/v1/warranties` - List warranties
- `GET /api/v1/warranties/:id` - Get warranty details
- `POST /api/v1/warranties/:id/claim` - File warranty claim

### Analytics (Protected)
- `GET /api/v1/analytics/usage` - Get usage statistics
- `GET /api/v1/analytics/validation-history` - Get validation history

## Configuration

Environment variables:
- `ENVIRONMENT` - development, staging, production (default: development)
- `PORT` - Server port (default: 8080)
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_URL` - Redis connection string
- `JWT_SECRET` - Secret key for JWT signing (required in production)
- `AWS_REGION` - AWS region for S3 (default: us-east-1)
- `S3_BUCKET` - S3 bucket name for datasets
- `VALIDATION_ENGINE_ADDR` - gRPC address for Python validation engine
- `DATA_SERVICE_ADDR` - gRPC address for data service

## Running Locally

```bash
# Install dependencies
go mod download

# Run the server
go run cmd/api/main.go

# Or build and run
go build -o api cmd/api/main.go
./api
```

## Development

```bash
# Format code
go fmt ./...

# Run tests
go test ./...

# Run with hot reload (install air first)
air
```

## Building for Production

```bash
# Build optimized binary
CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o api cmd/api/main.go

# Build Docker image
docker build -t synthos-api-gateway .

# Run with Docker
docker run -p 8080:8080 synthos-api-gateway
```

## Project Structure

```
go_backend/
├── cmd/
│   └── api/
│       └── main.go              # Application entry point
├── internal/
│   ├── handlers/                # HTTP request handlers
│   │   ├── auth.go
│   │   ├── datasets.go
│   │   ├── validations.go
│   │   ├── warranties.go
│   │   └── analytics.go
│   ├── middleware/              # HTTP middleware
│   │   ├── middleware.go
│   │   └── auth.go
│   ├── models/                  # Data models
│   │   ├── user.go
│   │   ├── dataset.go
│   │   └── validation.go
│   ├── auth/                    # Authentication utilities
│   │   └── jwt.go
│   ├── database/                # Database layer (TODO)
│   ├── grpc/                    # gRPC clients (TODO)
│   └── storage/                 # S3 integration (TODO)
├── pkg/
│   ├── config/                  # Configuration management
│   │   └── config.go
│   └── utils/                   # Utility functions
├── proto/                       # Protocol buffer definitions (TODO)
├── migrations/                  # Database migrations (TODO)
├── go.mod
├── go.sum
├── Dockerfile
└── README.md
```

## License

See main repository LICENSE file.
