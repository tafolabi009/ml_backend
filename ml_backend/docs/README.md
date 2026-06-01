# ML Backend - Validation & Collapse Engine

**Advanced Spectral Neural Networks for AI Training Data Validation**

> ⚠️ **Status: Alpha** - Core implementation complete, comprehensive testing in progress.

**Hardware Target:** 4x NVIDIA H200 (80GB each) | **Architecture:** FFT-based Spectral Processing (NO attention)

---

## 🎯 What We Build

The **ML validation engine** that detects model collapse BEFORE training begins. We're the ML team - the backend team handles everything else (API, UI, auth, warranties, etc.).

### Our Responsibility

**Phase 2-6 of the Validation Pipeline:**
1. **Diversity Analysis** → Stratified sampling (NOT random)
2. **Pre-Screening** → Match against collapse signature library
3. **Cascade Training** → Train 18 models across 3 tiers (streaming progress every 10s)
4. **Collapse Detection** → Multi-dimensional analysis (8 dimensions)
5. **Localization & Recommendations** → Pinpoint exact problematic rows

---

## 🏗️ Architecture Overview

### Our Custom Architectures (NO Transformers!)

#### 1. **Temporal Eigenstate Networks — TEN (Primary)**
- **Type:** FFT-based spectral processing
- **Key Components:**
  - `HierarchicalFFT` - Multi-scale frequency decomposition
  - `MultiHeadFrequencyLayer` - Processes different frequency bands
  - `AdvancedSpectralGating (ASG)` - **NOT attention!** Pure spectral gating
  - `SpectralFFN` - Feed-forward in frequency domain
  - `HolographicMemory` - Pattern storage with provable capacity
- **Complexity:** O(n log n) via FFT
- **Context Length:** Up to 260K+ tokens
- **NO attention mechanism** - pure frequency-domain processing

#### 2. **Temporal Eigenstate Networks (Secondary)**
- **Type:** Eigenstate-based temporal processing
- **Key Components:**
  - `TemporalFlowCell` - Models temporal dynamics
  - `EigenstateAttention` - Eigenstate-based (NOT self-attention)
  - `ResonanceBlock` - Resonance coupling between eigenstates
  - `HierarchicalTEN` - Multi-scale temporal hierarchies
- **Use Case:** Time-series and sequential data validation

---

## 📊 Multi-Scale Cascade Training

### Tier 1: Micro Models (76M params)
- **Model:** TEN "tiny"
- **Count:** 10 variants
- **Training Data:** 2M rows
- **GPUs:** 2x H200
- **Batch Size:** 256
- **Time:** ~30 minutes
- **Purpose:** Fast screening for obvious collapse signals

### Tier 2: Mini Models (454M params)
- **Model:** TEN "small"  
- **Count:** 5 variants
- **Training Data:** 10M rows
- **GPUs:** 3x H200
- **Batch Size:** 128
- **Time:** ~3 hours
- **Purpose:** Correlation preservation analysis

### Tier 3: Medium Models (983M params)
- **Model:** TEN "base"
- **Count:** 3 variants
- **Training Data:** 20M rows (full sample)
- **GPUs:** 4x H200 (all)
- **Batch Size:** 64
- **Time:** ~24 hours
- **Purpose:** Final validation, extrapolation to billions of parameters

**Total:** 18 models trained in ~30 hours

---

## 🔌 Integration with Backend

### What Backend Sends Us (via gRPC)

```protobuf
message CascadeRequest {
  string dataset_id = 1;
  string validation_id = 2;
  string sample_s3_path = 3;  // S3 path to dataset
  CascadeConfig config = 4;   // Model config
}
```

### What We Stream Back (every 10 seconds)

```protobuf
message CascadeProgress {
  string dataset_id = 1;
  string validation_id = 2;
  int32 current_tier = 3;           // 1, 2, or 3
  int32 current_variant = 4;         // Which model in tier
  int32 models_completed = 5;        // e.g., 7/18
  int32 models_total = 6;            // 18
  double progress_percent = 7;       // e.g., 38.9%
  double current_loss = 8;           // Real-time training loss
  map<int32, double> gpu_utilization = 9;  // % per GPU
  string estimated_completion = 10;  // ISO timestamp
  ModelResult result = 11;           // When model completes
}
```

### Final Results We Return

```protobuf
message PredictionResponse {
  string dataset_id = 1;
  double predicted_accuracy = 3;      // e.g., 0.87 (87%)
  ConfidenceInterval confidence = 4;  // [0.84, 0.90]
  int32 final_risk_score = 6;        // 0-100 (lower = better)
}

message CollapseResponse {
  bool collapse_detected = 3;
  string collapse_type = 4;          // "Type A", "Type B", etc.
  repeated DimensionScore dimensions = 6;
  repeated RootCause root_causes = 7;
}

message RecommendationResponse {
  repeated Recommendation recommendations = 3;  // Prioritized fixes
  CombinedImpact combined_impact = 4;          // Expected improvement
}
```

---

## 🔒 Security: mTLS (Service-to-Service)

All communication uses **mutual TLS (mTLS)**:

```python
# Server loads certificates
server_credentials = grpc.ssl_server_credentials(
    [(server_key, server_cert)],
    root_certificates=ca_cert,
    require_client_auth=True  # Enforce mTLS
)

# Backend must authenticate with client certificate
client_credentials = grpc.ssl_channel_credentials(
    root_certificates=ca_cert,
    private_key=client_key,
    certificate_chain=client_cert
)
```

**Certificate Locations:**
```
/etc/synthos/certs/
├── ca.crt         # CA certificate
├── server.crt     # Our server cert
├── server.key     # Our private key
└── client.crt     # Backend's client cert (for verification)
```

---

## 🚨 Error Handling

### Error Categories

| Code Range | Category | Retryable | Example |
|------------|----------|-----------|---------|
| 1000-1999 | Data Errors | ❌ No | Invalid format, corrupt file |
| 2000-2999 | Model Errors | ✅ Yes | Training divergence, OOM during training |
| 3000-3999 | Resource Errors | ✅ Yes | GPU memory exhausted |
| 4000-4999 | Timeout Errors | ✅ Yes | Operation took too long |
| 5000+ | Internal Errors | ❌ No | Unexpected bugs |

### Error Response Format

```protobuf
message ErrorInfo {
  int32 code = 1;                   // Error code
  string message = 2;               // Human-readable
  string details = 3;               // Stack trace
  bool retryable = 4;               // Can retry?
  int32 retry_after_seconds = 5;    // Wait time
}
```

### Automatic Retry Logic

```python
# Backend should implement exponential backoff
def call_with_retry(stub_method, request, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = await stub_method(request)
            if response.error and response.error.retryable:
                await asyncio.sleep(response.error.retry_after_seconds)
                continue
            return response
        except grpc.RpcError as e:
            if e.code() in [grpc.StatusCode.UNAVAILABLE, 
                           grpc.StatusCode.DEADLINE_EXCEEDED]:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
                continue
            raise
    raise MaxRetriesExceeded()
```

---

## 📁 Project Structure

```
ml_backend/
├── src/
│   ├── validation_engine/
│   │   ├── cascade_trainer.py       # Multi-scale training (18 models)
│   │   └── diversity_analyzer.py    # Stratified sampling & scoring
│   │
│   ├── collapse_engine/
│   │   ├── detector.py              # 8-dimensional collapse detection
│   │   ├── localizer.py             # Gradient-based localization
│   │   ├── recommender.py           # Fix generation
│   │   ├── recommender_advanced.py  # Advanced recommendations with causality
│   │   ├── signature_library.py     # Collapse patterns DB (FAISS)
│   │   └── signature_library_advanced.py # Enhanced signature matching
│   │
│   ├── data_processors/
│   │   └── dataset_loader.py        # Universal format loader
│   │
│   ├── grpc_services/
│   │   ├── validation_server.py     # gRPC server
│   │   ├── validation_server_complete.py # Full servicer implementation
│   │   ├── validation_pb2.py        # Generated protobuf
│   │   └── validation_pb2_grpc.py   # Generated gRPC stubs
│   │
│   ├── storage/
│   │   ├── factory.py               # Storage provider factory
│   │   ├── local_provider.py        # Local filesystem storage
│   │   ├── s3_provider.py           # AWS S3 storage
│   │   └── gcs_provider.py          # Google Cloud Storage
│   │
│   ├── connections/                 # External service connections
│   │
│   ├── model_architectures.py       # TEN model definitions (legacy Resonance NN aliases)
│   ├── orchestrator.py              # Unified pipeline coordinator
│   │
│   └── utils/
│       ├── gpu_optimizer.py         # GPU memory & mixed precision
│       └── error_handling.py        # Retries, circuit breakers
│
├── proto/
│   └── validation.proto             # gRPC service definitions
│
├── config/
│   ├── hardware_config.yaml         # 4x H200 setup
│   └── ml_config.yaml               # Model configurations
│
├── tests/
│   ├── unit/                        # Unit tests
│   ├── integration/                 # Integration tests
│   └── load/                        # Load/benchmark tests
│
├── examples/
│   └── complete_pipeline.py         # End-to-end demo
│
├── server.py                        # Unified gRPC server entry point
├── server_production.py             # Production server with monitoring
├── requirements.txt
└── Makefile
```

---

## 🎮 Dataset Format Support

We handle **ALL major formats** via `dataset_loader.py`:

| Format | Extension | Streaming | Notes |
|--------|-----------|-----------|-------|
| CSV | `.csv` | ✅ Yes | Chunked reading |
| TSV | `.tsv` | ✅ Yes | Tab-separated |
| JSON | `.json` | ❌ No | Load full |
| JSONL | `.jsonl` | ✅ Yes | Line-delimited |
| Parquet | `.parquet` | ✅ Yes | **Recommended** |
| HDF5 | `.h5`, `.hdf5` | ✅ Yes | Scientific data |
| Arrow | `.arrow` | ✅ Yes | Columnar |
| Feather | `.feather` | ✅ Yes | Fast I/O |
| Excel | `.xlsx`, `.xls` | ❌ No | Small files only |

**Example Usage:**
```python
from src.data_processors.dataset_loader import DatasetLoader

loader = DatasetLoader(chunk_size=100000)

# Get metadata (fast - doesn't load full data)
metadata = loader.get_metadata("dataset.parquet")
print(f"Rows: {metadata.total_rows:,}")
print(f"Memory: {metadata.estimated_memory_mb:.1f} MB")

# Stream large dataset
for chunk in loader.stream_chunks("dataset.parquet"):
    process_chunk(chunk)  # Process 100K rows at a time
```

---

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd ml_backend
pip install -r requirements.txt

# Install TEN (Temporal Eigenstate Networks)
pip install temporal-eigenstate-networks
```

### 2. Generate gRPC Code (if needed)

```bash
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/grpc_services \
    --grpc_python_out=./src/grpc_services \
    ./proto/validation.proto
```

### 3. Start gRPC Server

```bash
# Development mode
python server.py

# Production mode with monitoring
python server_production.py
```

### 4. Use the Orchestrator (Recommended)

```python
import asyncio
from src.orchestrator import SynthosOrchestrator

async def validate_dataset():
    orchestrator = SynthosOrchestrator(
        collapse_threshold=65.0,      # Minimum quality score
        diversity_threshold=50.0,     # Minimum diversity score
    )
    
    result = await orchestrator.validate(
        dataset_path="data.parquet",
        dataset_format="parquet",
        output_report_path="report.json"
    )
    
    if result.approved_for_training:
        print(f"✅ APPROVED! Score: {result.collapse_score:.1f}/100")
    else:
        print(f"❌ REJECTED: {result.reason}")
        for rec in result.recommendations:
            print(f"  💡 {rec['description']}")
    
    return result

result = asyncio.run(validate_dataset())
```

---

## 📈 Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **Validation Accuracy** | >90% | Predictions vs actual |
| **Turnaround Time** | <48 hours | For 500M row dataset |
| **False Positive Rate** | <5% | Incorrectly flagged datasets |
| **False Negative Rate** | <2% | Missed collapse risks |
| **Compute Cost** | <$2,000 | Per validation |
| **GPU Utilization** | >80% | Average across 4 GPUs |

---

## 🔧 8-Dimensional Collapse Detection

The collapse detector analyzes these dimensions:

| Dimension | Description |
|-----------|-------------|
| **Mode Collapse** | Repeated/clustered outputs |
| **Spectral Degradation** | Loss of frequency content |
| **Gradient Pathology** | Vanishing/exploding gradients |
| **Distribution Shift** | Data distribution changes |
| **Diversity Loss** | Reduction in output variety |
| **Memorization** | Overfitting to training data |
| **Quality Degradation** | Output quality decline |
| **Pattern Repetition** | Repeating patterns |

Each dimension scored 0-100 (higher = better).

---

## 🧪 Testing

```bash
# Run all tests with coverage
./run_tests.sh

# Or run manually
pytest tests/ -v --cov=src --cov-report=html

# Unit tests only
pytest tests/unit/ -v

# Integration tests
pytest tests/integration/ -v

# Load/benchmark tests
python tests/load/test_load.py
```

---

## 📝 What Backend Handles (NOT us)

❌ **We DON'T handle:**
- Customer authentication & authorization
- Dataset upload to S3
- Job queue management
- Report PDF generation
- Certificate generation
- Warranty logic
- Billing & payments
- Web UI / Dashboard
- Customer API Gateway

✅ **We ONLY handle:**
- ML validation algorithms
- Cascade training
- Collapse detection
- Localization & recommendations
- gRPC service endpoints

---

## 🐛 Common Issues

### Issue: "CUDA out of memory"
**Solution:** Reduce batch size or set GPU_MEMORY_FRACTION lower:
```bash
export GPU_MEMORY_FRACTION=0.7
```

### Issue: "gRPC deadline exceeded"
**Solution:** Increase timeout in client or set GRPC_MAX_MESSAGE_SIZE:
```bash
export GRPC_MAX_MESSAGE_SIZE=100000000
```

### Issue: Model training fails
**Solution:** Check logs and ensure ENABLE_MIXED_PRECISION is set correctly for your hardware.

---

**Built with ❤️ and FFT | O(n log n) > O(n²) | No attention, just science**

*Last Updated: June 2026*
