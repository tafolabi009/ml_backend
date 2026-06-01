# Synthos

AI training data validation platform that detects model collapse before it happens.

**Live at [synthos.dev](https://synthos.dev)** | A [Genovo Technologies](https://genovotech.com) product

> ⚠️ **Status: Alpha** — Core architecture implemented. Testing and validation in progress.

---

## What It Does

Synthos validates AI training data across **eight quality dimensions** — distribution fidelity, correlation preservation, entropy stability, gradient health, loss landscape, spectral coherence, generalization gap, and statistical consistency — using a proprietary multi-scale cascade of Temporal Eigenstate Networks (TEN). It helps AI teams avoid costly training failures by catching data quality issues and model collapse risks before training begins.

## Architecture

```
                        ┌──────────────────┐
                        │  Frontend (Next.js)│
                        │     Vercel :3000   │
                        └────────┬──────────┘
                                 │ HTTPS
                                 ▼
                        ┌──────────────────┐
                        │  API Gateway     │
                        │  Go/Fiber :8000  │
                        └──┬────┬────┬─────┘
                           │    │    │
              ┌────────────┘    │    └────────────┐
              ▼                 ▼                 ▼
     ┌────────────────┐ ┌────────────┐   ┌───────────────┐
     │ Job Orchestrator│ │ PostgreSQL │   │     Redis     │
     │ Go :8080 :50053 │ │   15       │   │      7        │
     └──┬─────────┬────┘ └────────────┘   └───────────────┘
        │ gRPC    │ gRPC
        ▼         ▼
┌───────────────┐ ┌───────────────┐   ┌───────────────┐
│ ML Backend    │ │ Collapse      │   │ Data Service  │
│ Validation    │ │ Engine        │   │ Go :50055     │
│ Engine :50051 │ │ :50052        │   └───────┬───────┘
│ Python/PyTorch│ │ Python/PyTorch│           │
└───────────────┘ └───────────────┘           ▼
                                       ┌───────────┐
                                       │ GCS / S3  │
                                       │ MinIO     │
                                       └───────────┘
```

### Services

| Service | Language | Port(s) | Purpose |
|---------|----------|---------|---------|
| **API Gateway** | Go (Fiber) | 8000 | REST API, JWT auth, RBAC, rate limiting, WebSocket progress |
| **Job Orchestrator** | Go (Gorilla Mux + gRPC) | 8080, 50053 | Pipeline coordination, priority job queue, worker pool |
| **ML Backend** | Python (PyTorch + gRPC) | 50051, 50052 | Validation engine + collapse detection engine |
| **Data Service** | Go (gRPC) | 50055 | Dataset upload/download streaming, profiling, metadata |
| **Admin Dashboard** | Go (Fiber) | 3001 | Internal ops console with basic auth |
| **Frontend** | TypeScript (Next.js) | 3000 | Customer dashboard (auto-deploys to Vercel) |

### Inter-Service Communication

All backend services communicate via **gRPC** with optional **mTLS** (enabled by default in production). Protobuf contracts are defined in the [`proto/`](proto/) directory:

| Proto | Package | Key RPCs |
|-------|---------|----------|
| `validation.proto` | `validation` | TrainCascade (streaming), AnalyzeDiversity, GetPredictions |
| `collapse.proto` | `collapse` | DetectCollapse, LocalizeCollapse, GenerateRecommendations |
| `orchestrator.proto` | `orchestrator` | CreateJob, GetJobStatus, CancelJob, ListJobs |
| `data.proto` | `data` | UploadDataset (streaming), ProfileDataset, StreamDataset |

---

## ML Validation Pipeline

When a dataset is submitted for validation, the ML Backend runs a **6-stage pipeline**:

```
Data Loading → Diversity Analysis → Cascade Training → Collapse Detection → Localization → Recommendations
     ↓               ↓                    ↓                   ↓                 ↓                ↓
   Load           Analyze           Train 18 models      8-dimensional     Pinpoint rows     Actionable
   any format     statistical       across 3 tiers       scoring           with issues       fix plans
                  spread                                                                     + cost-benefit
```

### 8-Dimensional Collapse Detection

| Dimension | Threshold | Description |
|-----------|-----------|-------------|
| Distribution Fidelity | 70 | KS test, Wasserstein distance, KL divergence |
| Correlation Preservation | 70 | Frobenius norm, correlation-of-correlations |
| Entropy Stability | 65 | Shannon entropy ratio, mutual information |
| Gradient Health | 60 | Gradient norms, vanishing/exploding detection |
| Loss Landscape | 65 | Convergence analysis, plateau detection |
| Spectral Coherence | 70 | FFT-based spectral analysis (custom CUDA kernel) |
| Generalization Gap | 75 | Train/test performance divergence |
| Statistical Consistency | 70 | Higher-order moment matching |

### Model Architecture: Temporal Eigenstate Networks (TEN)

The platform uses custom **TEN** — FFT-based and Triton-accelerated spectral eigenstate decomposition with **O(T log T) complexity** (no attention mechanism). Up to 9.8x speedups over transformers:

| Size | Parameters | Layers | Context Length | Cascade Role |
|------|-----------|--------|----------------|--------------|
| tiny | 76M | 4 | 2K | Tier 1 — 10 variants, fast screening |
| small | 454M | 8 | 4K | Tier 2 — 5 variants, correlation analysis |
| base | 983M | 12 | 8K | Tier 3 — 3 variants, final validation |
| medium | 1.8B | 16 | 16K | Available for extended validation |
| large | 3.9B | 24 | 32K | Available for extended validation |

**Total cascade**: 18 models (10 + 5 + 3) trained per validation run.

### SynthOS CUDA Kernel

A custom fused CUDA kernel (`synthos_kernel/`) replaces PyTorch's multi-step spectral analysis pipeline (FFT → PSD → Normalize → Entropy) with a single optimized kernel. Architecture-aware dispatch from Pascal (2x speedup) to Hopper (5x speedup).

---

## Role-Based Access Control

| Role | Route Prefix | Capabilities |
|------|-------------|-------------|
| **Admin** | `/api/v1/admin` | User management, platform settings, promo codes, audit log |
| **Developer** | `/api/v1/developer` | Service status, API docs, logs, metrics |
| **Support** | `/api/v1/support` | Ticket queue, assignment, warranty claim processing |
| **User** | `/api/v1/` | Upload datasets, run validations, manage credits, webhooks |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Go 1.21+ (for backend development)
- Python 3.11+ with CUDA 11.8+ (for ML development)

### Run All Services

```bash
# Clone
git clone https://github.com/tafolabi009/ml_backend.git
cd ml_backend

# Configure
cp .env.example .env

# Start core services
docker-compose up -d

# Start with monitoring (Prometheus + Grafana + Jaeger)
docker-compose --profile monitoring up -d

# Start with admin dashboard
docker-compose --profile admin up -d
```

**Endpoints after startup:**
- API Gateway: `http://localhost:8000`
- Admin Dashboard: `http://localhost:3001` (admin/admin)
- MinIO Console: `http://localhost:9001` (minioadmin/minioadmin)
- Grafana: `http://localhost:3100` (when monitoring profile active)

### ML Backend Only (Development)

```bash
cd ml_backend
pip install -r requirements.txt

# Development server
python server.py

# Production server (with DB connections)
python server_production.py
```

### Run Validation Programmatically

```python
import asyncio
from src.orchestrator import SynthosOrchestrator

async def main():
    orchestrator = SynthosOrchestrator(
        collapse_threshold=65.0,
        diversity_threshold=50.0,
        gpu_memory_fraction=0.9,
        enable_mixed_precision=True,
    )

    result = await orchestrator.validate(
        dataset_path="data.parquet",
        dataset_format="parquet",
        stream_progress=True,
    )

    if result.approved_for_training:
        print(f"✅ APPROVED — Score: {result.collapse_score:.1f}/100")
    else:
        print(f"❌ REJECTED — {result.reason}")
        for rec in result.recommendations:
            print(f"  💡 {rec['description']}")

asyncio.run(main())
```

---

## Environment Configuration

Copy `.env.example` to `.env` and configure:

| Variable | Purpose | Default |
|----------|---------|---------|
| `DATABASE_URL` | PostgreSQL connection string | `postgres://postgres:postgres@postgres:5432/synthos` |
| `REDIS_URL` / `REDIS_PASSWORD` | Redis cache and pub/sub | `redis:6379` |
| `JWT_SECRET` | JWT token signing key | dev secret (change in production) |
| `CLOUD_PROVIDER` | `gcp` or `aws` | `gcp` |
| `GCP_PROJECT_ID` | Google Cloud project ID | — |
| `GCS_BUCKET` | Dataset storage bucket | — |
| `S3_BUCKET` / `S3_ENDPOINT` | S3/MinIO storage (when using AWS) | `synthos-datasets` |
| `GPU_MEMORY_FRACTION` | Fraction of GPU memory to use | `0.9` |
| `ENABLE_MIXED_PRECISION` | BF16/FP16 mixed precision training | `true` |
| `ENABLE_MTLS` | Mutual TLS for gRPC | `true` |
| `RATE_LIMIT_RPM` | gRPC rate limit (requests/minute) | `60` |

---

## API Reference

Interactive API documentation is available in the developer console after login.

### Authentication

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/auth/register` | POST | Create account |
| `/api/v1/auth/login` | POST | Get JWT token |
| `/api/v1/auth/refresh` | POST | Refresh access token |
| `/api/v1/auth/forgot-password` | POST | Request password reset |
| `/api/v1/auth/verify-email` | POST | Verify email address |
| `/api/v1/auth/2fa/setup` | POST | Enable TOTP 2FA |
| `/api/v1/auth/api-keys` | POST | Create scoped API key |

### Datasets

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/datasets/upload` | POST | Get presigned upload URL |
| `/api/v1/datasets/:id/complete` | POST | Mark upload complete |
| `/api/v1/datasets` | GET | List user datasets |
| `/api/v1/datasets/:id` | GET | Get dataset details |
| `/api/v1/datasets/:id` | DELETE | Delete dataset |

### Validations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/validations/create` | POST | Start validation job |
| `/api/v1/validations` | GET | List validations |
| `/api/v1/validations/:id` | GET | Get validation results |
| `/api/v1/validations/:id/report` | GET | Download full report |
| `/api/v1/validations/:id/certificate` | GET | Download PDF certificate |
| `/api/v1/validations/:id/collapse-details` | GET | Detailed collapse analysis |
| `/api/v1/validations/:id/cancel` | POST | Cancel running validation |

### Credits & Billing

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/credits/balance` | GET | Check credit balance |
| `/api/v1/credits/packages` | GET | Available credit packages |
| `/api/v1/credits/purchase` | POST | Purchase credits |
| `/api/v1/credits/redeem` | POST | Redeem promo code |

### Warranties

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/warranties/request` | POST | Request data quality warranty |
| `/api/v1/warranties` | GET | List warranties |
| `/api/v1/warranties/:id/claim` | POST | File warranty claim |

### Webhooks

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/webhooks` | POST | Register webhook endpoint |
| `/api/v1/webhooks` | GET | List webhooks |
| `/api/v1/webhooks/:id` | PUT | Update webhook |
| `/api/v1/webhooks/:id` | DELETE | Remove webhook |

---

## Deployment

### Production (GCP Cloud Run)

```bash
# Build and deploy all services via Cloud Build
gcloud builds submit --config cloudbuild.yaml --region us-central1
```

Cloud Build creates 4 Docker images and pushes to GCP Artifact Registry (`us-central1-docker.pkg.dev`).

### GPU Deployment (RunPod)

For GPU-accelerated ML inference, see [`ml_backend/RUNPOD_DEPLOYMENT.md`](ml_backend/RUNPOD_DEPLOYMENT.md).

| Configuration | GPUs | Cost/Hour |
|--------------|------|-----------|
| 1× A10G (testing) | 1 | $0.80 |
| 4× A10G (production) | 4 | $3.20 |
| 1× A100 40GB | 1 | $1.89 |
| 4× A100 40GB | 4 | $7.56 |

### Frontend

Auto-deploys to Vercel on push to `main` branch of the frontend repository.

---

## Database Schema

PostgreSQL 15 with 8 core tables:

| Table | Purpose |
|-------|---------|
| `users` | Accounts, roles, subscription tiers |
| `datasets` | Uploaded dataset metadata and S3 paths |
| `validations` | Validation jobs, status, risk scores |
| `validation_results` | Predicted accuracy, confidence intervals, dimension scores |
| `collapse_details` | Collapse detection output, severity, affected dimensions |
| `recommendations` | Prioritized fix suggestions per validation |
| `warranties` | Data quality warranties and coverage |
| `warranty_claims` | Filed claims against warranties |
| `jobs` | Job orchestrator queue entries with priorities and payloads |

Schema defined in [`scripts/schema.sql`](scripts/schema.sql). Migrations in [`migrations/`](migrations/).

---

## Security

- **Authentication**: JWT with refresh tokens, TOTP-based 2FA, scoped API keys
- **Authorization**: Role hierarchy (admin > developer > support > user) with scope-based access
- **Inter-service**: mTLS for gRPC (certificates in `/etc/synthos/certs/`)
- **Rate limiting**: Sliding window per user+IP (REST) + token bucket (gRPC, 60 req/min)
- **Data**: bcrypt password hashing, parameterized SQL queries (asyncpg/pgx), input sanitization
- **Secrets**: GCP Secret Manager in production
- **Audit**: Full audit logging for admin actions

For the full security audit report, see [`SECURITY_AUDIT_REPORT.md`](SECURITY_AUDIT_REPORT.md).

---

## Monitoring

When running with the `monitoring` Docker Compose profile:

| Tool | Port | Purpose |
|------|------|---------|
| Prometheus | 9090 | Metrics collection |
| Grafana | 3100 | Dashboards and alerting |
| Jaeger | 16686 | Distributed tracing |

---

## Testing

```bash
cd ml_backend

# Run all tests with coverage
./run_tests.sh

# Or manually
pytest tests/ -v --cov=src --cov-report=html

# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v --timeout=300

# Load/benchmark tests
python tests/load/test_load.py
```

For the full testing guide, see [`ml_backend/TESTING_GUIDE.md`](ml_backend/TESTING_GUIDE.md).

---

## Project Structure

```
synthos/
├── go_backend/                     # API Gateway (Go/Fiber)
│   ├── cmd/api/main.go             # Entry point, routing, DI
│   ├── internal/
│   │   ├── handlers/               # 17 handler files (~280KB)
│   │   ├── middleware/              # Auth, rate limiting, tracing
│   │   ├── models/                  # Domain models
│   │   ├── repository/              # Database access (pgx)
│   │   └── auth/                    # JWT management
│   └── pkg/                         # Shared packages
│       ├── config/                  # Env config + GCP Secrets
│       ├── database/                # PostgreSQL pooling
│       ├── storage/                 # S3 + GCS providers
│       ├── grpcclient/              # gRPC clients with circuit breaker
│       ├── monitoring/              # Prometheus metrics
│       ├── tracing/                 # Jaeger distributed tracing
│       ├── websocket/               # Real-time progress
│       ├── email/                   # Notification emails
│       ├── pdfgen/                  # Certificate PDF generation
│       └── webhook/                 # Outbound webhook delivery
│
├── job_orchestrator/                # Pipeline Coordinator (Go)
│   ├── main.go                      # REST + gRPC dual protocol
│   └── internal/
│       ├── service/                 # Orchestration, queue, resources
│       └── api/                     # REST handlers
│
├── ml_backend/                      # ML Engine (Python/PyTorch)
│   ├── src/
│   │   ├── orchestrator.py          # 6-stage pipeline coordinator
│   │   ├── model_architectures.py   # TEN wrappers (legacy Resonance NN aliases)
│   │   ├── validation_engine/       # Diversity analyzer, cascade trainer
│   │   ├── collapse_engine/         # Detector, localizer, recommender, signatures
│   │   ├── synthos_kernel/          # Custom CUDA kernels (spectral entropy)
│   │   ├── grpc_services/           # gRPC service implementations
│   │   ├── data_processors/         # Multi-format dataset loader
│   │   ├── storage/                 # Storage provider abstraction
│   │   ├── connections/             # PostgreSQL + Redis connection manager
│   │   └── utils/                   # GPU optimizer, error handling, metrics
│   ├── config/                      # ml_config.yaml, hardware_config.yaml
│   ├── tests/                       # Unit, integration, load tests
│   ├── docs/                        # ML-specific documentation
│   ├── server.py                    # Development gRPC server
│   └── server_production.py         # Production gRPC server
│
├── data_service/                    # Dataset Service (Go/gRPC)
│   └── main.go                      # Upload/download/profile datasets
│
├── admin_dashboard/                 # Internal Ops Console (Go/Fiber)
│   └── main.go                      # Dashboard with basic auth
│
├── proto/                           # Shared Protobuf definitions
│   ├── validation.proto
│   ├── collapse.proto
│   ├── orchestrator.proto
│   └── data.proto
│
├── scripts/                         # Database schema, deploy scripts
├── migrations/                      # SQL migration files
├── monitoring/                      # Prometheus + Grafana configs
├── docker-compose.yml               # Full-stack local development
├── cloudbuild.yaml                  # GCP Cloud Build CI/CD
└── .env.example                     # Environment configuration template
```

---

## Documentation Index

| Document | Location | Description |
|----------|----------|-------------|
| **Architecture** | [`ml_backend/docs/ARCHITECTURE.md`](ml_backend/docs/ARCHITECTURE.md) | ML backend architecture and module inventory |
| **Data Flow** | [`ml_backend/docs/DATA_FLOW.md`](ml_backend/docs/DATA_FLOW.md) | Visual data flow through all 6 pipeline stages |
| **Unified Pipeline** | [`ml_backend/docs/UNIFIED_PIPELINE.md`](ml_backend/docs/UNIFIED_PIPELINE.md) | How to use the SynthosOrchestrator |
| **API Architecture** | [`ml_backend/docs/synthos-api-architecture.md`](ml_backend/docs/synthos-api-architecture.md) | Full API design and contracts |
| **Validation Method** | [`ml_backend/docs/synthos-validation-method.md`](ml_backend/docs/synthos-validation-method.md) | Scientific methodology behind validation |
| **Strategic Plan** | [`ml_backend/docs/synthos-strategic-plan.md`](ml_backend/docs/synthos-strategic-plan.md) | Product roadmap and business strategy |
| **Architecture History** | [`ml_backend/RESONANCE_NN_INTEGRATION.md`](ml_backend/RESONANCE_NN_INTEGRATION.md) | Legacy Resonance NN → TEN migration notes |
| **RunPod Deployment** | [`ml_backend/RUNPOD_DEPLOYMENT.md`](ml_backend/RUNPOD_DEPLOYMENT.md) | GPU cloud deployment guide |
| **Testing Guide** | [`ml_backend/TESTING_GUIDE.md`](ml_backend/TESTING_GUIDE.md) | How to run and write tests |
| **Security Audit** | [`SECURITY_AUDIT_REPORT.md`](SECURITY_AUDIT_REPORT.md) | Security review and remediation status |

---

## License

Proprietary. Copyright © 2025–2026 Genovo Technologies.
