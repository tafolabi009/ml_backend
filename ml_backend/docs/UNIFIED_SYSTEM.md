# Unified System Overview

**Synthos ML Validation Platform - Complete System Architecture**

> ⚠️ **Status: Alpha** - Core implementation complete, testing in progress.

---

## 🎯 System Purpose

Synthos is a **unified validation and certification platform** for AI training data. The system detects model collapse **before** expensive training runs, potentially saving millions in wasted compute.

---

## 🏗️ System Components

### 1. API Gateway (Go/Fiber)
**Port:** 8000

The customer-facing REST API that handles:
- User authentication (JWT)
- Dataset uploads
- Validation job creation
- Results retrieval
- Analytics

### 2. Job Orchestrator (Go)
**Port:** 8080 (REST), 50053 (gRPC)

Central pipeline controller that:
- Manages job queue
- Coordinates ML services
- Tracks job progress
- Handles retries and failures

### 3. ML Backend (Python)
**Ports:** 50051 (Validation), 50052 (Collapse), 50054 (Data)

The core ML validation engine with:
- Cascade training (18 models)
- Collapse detection (8 dimensions)
- Problem localization
- Recommendations

### 4. Data Layer
- **PostgreSQL:** Primary database (users, datasets, validations)
- **Redis:** Caching and job queue
- **MinIO/S3:** Object storage for datasets

### 5. Monitoring
- **Prometheus:** Metrics collection
- **Grafana:** Visualization dashboards
- **Jaeger:** Distributed tracing (optional)

---

## 📊 Data Flow

```
Customer → API Gateway → Job Orchestrator → ML Backend → Results → Customer
              ↑                ↑                ↑
              ├── PostgreSQL ──┘                │
              ├── Redis ───────────────────────┘
              └── MinIO/S3 ────────────────────┘
```

### Validation Flow

1. **Upload**: Customer uploads dataset via API Gateway
2. **Queue**: Job Orchestrator queues validation job
3. **Process**: ML Backend runs 6-stage validation pipeline
4. **Store**: Results stored in PostgreSQL
5. **Return**: Customer retrieves results via API

---

## 🔌 Service Communication

### External (REST)
```
Customer → HTTPS → API Gateway (port 8000)
```

### Internal (gRPC)
```
API Gateway → gRPC → Job Orchestrator (port 8080/50053)
Job Orchestrator → gRPC → ML Backend (ports 50051/52/54)
```

### Async (Redis)
```
Job Orchestrator ← Redis Queue ← Background Workers
```

---

## 🔒 Security Layers

1. **API Gateway**: JWT authentication, rate limiting
2. **Internal**: mTLS for service-to-service communication
3. **Database**: Encrypted connections, role-based access
4. **Storage**: Signed URLs, encryption at rest

---

## 🚀 Deployment Options

### Development (Docker Compose)
```bash
docker-compose up -d
```

### Production (Kubernetes)
- Helm charts in `deployment/`
- Horizontal pod autoscaling
- GPU node pools for ML backend

### GPU Cloud (RunPod)
- See `RUNPOD_DEPLOYMENT.md`
- Supports A10G, A100, H100, H200 GPUs

---

## 📊 Key Metrics

| Metric | Target | Description |
|--------|--------|-------------|
| API Latency (p95) | <500ms | REST API response time |
| Validation Time | <48h | For 500M row dataset |
| Accuracy | >90% | Prediction vs actual |
| Availability | 99.9% | System uptime |
| GPU Utilization | >80% | During ML processing |

---

## 🛠️ Configuration

All services configured via environment variables:

| Service | Config Source |
|---------|---------------|
| API Gateway | `.env`, Secrets Manager |
| Job Orchestrator | `.env`, ConfigMaps |
| ML Backend | `config/*.yaml`, `.env` |
| Database | Connection string |
| Redis | Connection string |

---

## 📚 Related Documentation

- [Main README](../../README.md) - Project overview
- [Architecture](ARCHITECTURE.md) - Technical architecture
- [Data Flow](DATA_FLOW.md) - Detailed data flow
- [Unified Pipeline](UNIFIED_PIPELINE.md) - ML pipeline details
- [API Architecture](synthos-api-architecture.md) - Full API spec
- [Go Backend](../../go_backend/GO_BACKEND_IMPLEMENTATION.md) - API Gateway details
- [Testing Guide](../TESTING_GUIDE.md) - How to test

---

*Last Updated: June 2026*
