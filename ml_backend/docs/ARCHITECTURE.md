# ML Backend Architecture

> **Status: Alpha** — Core implementation complete. Comprehensive testing in progress.

---

## Overview

The ML Backend is the computational core of Synthos. It runs the validation pipeline that detects model collapse in AI training data. It is written in Python with PyTorch and exposes two gRPC services: **Validation Engine** (port 50051) and **Collapse Engine** (port 50052).

**Hardware targets:**
- **Development**: CPU or single GPU
- **Production**: 1–4× NVIDIA A10G/A100/H100/H200 GPUs
- **GPU Cloud**: RunPod, GCP Cloud Run with GPU, or on-premise clusters

---

## Custom Architectures

### 1. Temporal Eigenstate Networks — TEN (Primary) — O(T log T)

FFT-based and Triton-accelerated spectral eigenstate decomposition with **no attention mechanism**. Up to 9.8x speedups over transformers:

| Component | Purpose |
|-----------|---------|
| `TENFFTLayer` | FFT convolution for sequences ≤ 2048 tokens |
| `TENProLayer` | Depth-Adaptive Pro layers for sequences > 2048 |
| `Gated Spectral Gate` | Output projection (NOT attention) |
| `Cross-Layer Memory` | State passing between layers |
| `Triton Kernels` | GPU-accelerated sequence evolution (T ≤ 512) |

**Model sizes:**

| Size | Params | Layers | Freq. Heads | Context | Memory Capacity |
|------|--------|--------|-------------|---------|-----------------|
| tiny | 76M | 4 | 8 | 2K | 100 |
| small | 454M | 8 | 16 | 4K | 500 |
| base | 983M | 12 | 12 | 8K | 1,000 |
| medium | 1.8B | 16 | 24 | 16K | 2,000 |
| large | 3.9B | 24 | 32 | 32K | 5,000 |

### 2. Temporal Eigenstate Networks (Secondary)

Eigenstate-based temporal processing for time-series and sequential data:
- `TemporalFlowCell` — Temporal dynamics modeling
- `EigenstateAttention` — Eigenstate-based (NOT self-attention)
- `ResonanceBlock` — Coupling between eigenstates (legacy naming)
- `HierarchicalTEN` — Multi-scale temporal hierarchies

---

## Source Layout

```
ml_backend/
├── src/
│   ├── orchestrator.py                  # 6-stage pipeline coordinator (959 lines)
│   ├── model_architectures.py           # TEN wrappers + legacy Resonance aliases (612 lines)
│   │
│   ├── validation_engine/
│   │   ├── cascade_trainer.py           # Multi-scale training: 18 models, 3 tiers
│   │   ├── diversity_analyzer.py        # Stratified diversity analysis + scoring
│   │   └── universal_validator.py       # Multi-modal dataset validator
│   │
│   ├── collapse_engine/
│   │   ├── detector.py                  # 8-dimensional collapse detector (1294 lines)
│   │   ├── localizer.py                 # Gradient-based problem localization
│   │   ├── recommender.py               # Prioritized fix recommendations
│   │   ├── recommender_advanced.py      # Causal analysis + ML-driven recs
│   │   ├── signature_library.py         # Known collapse pattern database
│   │   └── signature_library_advanced.py # FAISS-indexed signature matching
│   │
│   ├── synthos_kernel/
│   │   ├── spectral.cu                  # Fused spectral entropy CUDA kernel
│   │   ├── spectral.h                   # C++ header
│   │   ├── python_wrapper.py            # ctypes Python bindings
│   │   ├── arch_dispatch.cuh            # GPU architecture dispatch (sm_60–sm_90)
│   │   └── kernels/                     # Additional CUDA kernels
│   │
│   ├── grpc_services/
│   │   ├── validation_server_complete.py # Full servicer (836 lines)
│   │   ├── validation_pb2.py            # Generated protobuf
│   │   └── validation_pb2_grpc.py       # Generated gRPC stubs
│   │
│   ├── data_processors/
│   │   └── dataset_loader.py            # Multi-format loader (CSV, Parquet, HDF5, etc.)
│   │
│   ├── storage/
│   │   ├── storage_provider.py          # Abstract base class
│   │   ├── s3_provider.py               # AWS S3
│   │   ├── gcs_provider.py              # Google Cloud Storage
│   │   ├── local_provider.py            # Local filesystem
│   │   └── factory.py                   # Storage factory
│   │
│   ├── connections/
│   │   └── db.py                        # PostgreSQL (asyncpg) + Redis connection manager
│   │
│   └── utils/
│       ├── gpu_optimizer.py             # GPU memory management
│       ├── gpu_auto_config.py           # Auto-detect GPU tier (Pascal → Hopper)
│       ├── error_handling.py            # Retry decorators, circuit breakers
│       └── metrics.py                   # Prometheus metrics collection
│
├── config/
│   ├── ml_config.yaml                   # Model sizes, cascade, collapse thresholds
│   └── hardware_config.yaml             # GPU tier configurations
│
├── tests/
│   ├── unit/                            # CollapseDetector, DiversityAnalyzer tests
│   ├── integration/                     # Full pipeline tests
│   ├── load/                            # Performance benchmarks
│   ├── fixtures/                        # Test data files
│   └── conftest.py                      # Shared pytest fixtures
│
├── server.py                            # Development gRPC entry point
└── server_production.py                 # Production server (async, DB, monitoring)
```

---

## Validation Pipeline

The `SynthosOrchestrator.validate()` method runs a 6-stage pipeline:

```
Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5 → Stage 6 → Decision
 Load     Diversity  Cascade   Collapse  Localize  Recommend  Approve/Reject
 ~5s       ~10s       ~30s      ~15s      ~20s       ~5s       instant
```

**Total**: ~85 seconds for 1M rows on 4× H200 GPUs.

### Stage 1: Data Loading
- Supports CSV, TSV, JSON, JSONL, Parquet, HDF5, Arrow, Feather, Excel
- Streaming for large datasets
- Fast metadata extraction

### Stage 2: Diversity Analysis
- Semantic, statistical, and structural diversity scoring
- HDBSCAN clustering for stratification
- Rare pattern oversampling (3×)
- Output: diversity score (0–100)

### Stage 3: Cascade Training
- Trains 18 TEN models across 3 tiers:
  - **Tier 1**: 10× tiny (76M) — fast screening
  - **Tier 2**: 5× small (454M) — correlation analysis
  - **Tier 3**: 3× base (983M) — final validation
- AdamW + cosine warmup scheduler
- Streams progress every 10 seconds via gRPC

### Stage 4: Collapse Detection
- 8-dimensional scoring (see table in root README)
- Custom CUDA kernel for spectral coherence analysis
- Signature library matching (FAISS)
- Output: collapse score (0–100), collapse_detected flag

### Stage 5: Problem Localization
- Gradient-based row-level scoring
- Groups consecutive indices into regions
- Returns exact problematic row indices

### Stage 6: Recommendations
- Prioritized fix suggestions with cost-benefit analysis
- Projects expected score improvement after each fix
- Categories: augmentation, filtering, re-sampling, re-collection

### Final Decision Logic

```
APPROVED if:
  collapse_score >= 65 (configurable) AND
  diversity_score >= 50 (configurable) AND
  no critical dimension < 40
```

---

## gRPC Services

### ValidationEngine (port 50051)

| RPC | Type | Description |
|-----|------|-------------|
| `AnalyzeDiversity` | Unary | Run diversity analysis on dataset |
| `PreScreenRisk` | Unary | Match against collapse signature library |
| `TrainCascade` | Server streaming | Train 18 models, stream progress |
| `GetPredictions` | Unary | Get final accuracy + confidence intervals |

### CollapseEngine (port 50052)

| RPC | Type | Description |
|-----|------|-------------|
| `DetectCollapse` | Unary | Run 8-dimensional collapse detection |
| `LocalizeProblems` | Unary | Pinpoint problematic data regions |
| `GenerateRecommendations` | Unary | Generate actionable fix plans |

### Server Configuration

- Max message size: 100MB send/receive
- Keepalive: 30s interval, 10s timeout
- Rate limiting: token bucket (60 req/min, burst 10)
- mTLS: enabled by default in production (`ENABLE_MTLS=true`)

---

## Error Handling

| Code Range | Category | Retryable | Example |
|------------|----------|-----------|---------|
| 1000–1999 | Data Errors | No | Invalid format, corrupt file |
| 2000–2999 | Model Errors | Yes | Training divergence, OOM |
| 3000–3999 | Resource Errors | Yes | GPU memory exhausted |
| 4000–4999 | Timeout Errors | Yes | Operation too slow |
| 5000+ | Internal Errors | No | Unexpected bugs |

The `@with_retries` decorator provides exponential backoff with configurable max retries.

---

## GPU Configuration

`gpu_auto_config.py` auto-detects GPU architecture and adjusts parameters:

| GPU Family | Compute Capability | Precision | Batch Multiplier |
|------------|-------------------|-----------|-----------------|
| Pascal (GTX 10xx) | sm_60 | FP32 | 1× |
| Volta (V100) | sm_70 | FP16 | 2× |
| Turing (RTX 20xx) | sm_75 | FP16 | 1.5× |
| Ampere (A100/RTX 30) | sm_80/86 | BF16 | 3× |
| Ada Lovelace (RTX 40) | sm_89 | BF16 | 3.5× |
| Hopper (H100/H200) | sm_90 | BF16 | 4× |

---

## Performance Targets

| Metric | Target | Status |
|--------|--------|--------|
| Validation Accuracy | >90% | Testing needed |
| Turnaround (500M rows) | <48 hours | Benchmark pending |
| False Positive Rate | <5% | Testing needed |
| False Negative Rate | <2% | Testing needed |
| GPU Utilization | >80% | Profiling needed |
| Test Coverage | >70% | ~30% current |

### Cost Estimates

| Dataset Size | Time | Cost (4× H100) |
|--------------|------|----------------|
| 1K rows | <1s | $0.01 |
| 100K rows | ~30s | $0.37 |
| 1M rows | ~5 min | $3.70 |
| 10M rows | ~30 min | $22 |
| 100M rows | ~4 hours | $177 |
| 1B rows | ~36 hours | $1,597 |

---

## Quick Reference

```bash
# Install dependencies
pip install -r requirements.txt

# Generate gRPC stubs (if proto files change)
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/grpc_services \
    --grpc_python_out=./src/grpc_services \
    ./proto/validation.proto

# Start development server
python server.py

# Start production server
python server_production.py

# Run tests
pytest tests/ -v --cov=src --cov-report=html
```

---

*Version: 0.1.0-alpha | Architecture: TEN (Temporal Eigenstate Networks) | Last Updated: June 2026*
