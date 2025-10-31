# ML Backend - Validation & Collapse Engine

**Advanced Spectral Neural Networks for AI Training Data Validation**

**Hardware:** 4x NVIDIA H200 (80GB each) | **Architectures:** FFT-based Spectral Processing (NO attention)

---

## 🎯 What We Build

The **ML validation engine** that detects model collapse BEFORE training begins. We're the ML team - the backend team handles everything else (API, UI, auth, warranties, etc.).

### Our Responsibility

**Phase 2-6 of the Validation Pipeline:**
1. **Diversity Analysis** → Stratified sampling (NOT random)
2. **Pre-Screening** → Match against collapse signature library
3. **Cascade Training** → Train 18 models across 3 tiers (streaming progress every 10s)
4. **Collapse Detection** → Multi-dimensional analysis
5. **Localization & Recommendations** → Pinpoint exact problematic rows

---

## 🏗️ Architecture Overview

### Our Custom Architectures (NO Transformers!)

#### 1. **Resonance NN (Primary)**
- **Type:** FFT-based spectral processing
- **Key Components:**
  - `HierarchicalFFT` - Multi-scale frequency decomposition
  - `MultiHeadFrequencyLayer` - Processes different frequency bands
  - `AdvancedSpectralGating (ASG)` - **NOT attention!** Pure spectral gating
  - `SpectralFFN` - Feed-forward in frequency domain
- **Complexity:** O(n log n) via FFT
- **Context Length:** Up to 131K tokens (base model)
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
- **Model:** Resonance NN "tiny"
- **Count:** 10 variants
- **Training Data:** 2M rows
- **GPUs:** 2x H200
- **Batch Size:** 256
- **Time:** ~30 minutes
- **Purpose:** Fast screening for obvious collapse signals

### Tier 2: Mini Models (454M params)
- **Model:** Resonance NN "small"  
- **Count:** 5 variants
- **Training Data:** 10M rows
- **GPUs:** 3x H200
- **Batch Size:** 128
- **Time:** ~3 hours
- **Purpose:** Correlation preservation analysis

### Tier 3: Medium Models (983M params)
- **Model:** Resonance NN "base"
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
│   │   ├── cascade_trainer.py       # Multi-scale training
│   │   ├── diversity_analyzer.py    # Stratified sampling
│   │   └── signature_library.py     # Collapse patterns DB
│   │
│   ├── collapse_engine/
│   │   ├── detector.py              # Multi-dimensional detection
│   │   ├── localizer.py             # Gradient-based localization
│   │   └── recommender.py           # Fix generation
│   │
│   ├── data_processors/
│   │   ├── dataset_loader.py        # Universal format loader
│   │   └── sampler.py               # Stratified sampling logic
│   │
│   ├── grpc_services/
│   │   ├── validation_server.py     # gRPC server (mTLS)
│   │   └── client_example.py        # Example client
│   │
│   └── utils/
│       ├── gpu_manager.py           # 4x H200 orchestration
│       ├── metrics.py               # FFT-specific metrics
│       └── logging_config.py        # Structured logging
│
├── proto/
│   └── validation.proto             # gRPC service definitions
│
├── config/
│   ├── hardware_config.yaml         # 4x H200 setup
│   └── ml_config.yaml               # Model configurations
│
├── tests/
│   ├── test_cascade.py
│   ├── test_collapse.py
│   └── test_grpc.py
│
├── resonance_nn-0.1.0-py3-none-any.whl          # FFT-based models
├── temporal_eigenstate_networks-0.1.0-py3-none-any.whl
└── README.md                        # This file
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
# Install custom architectures
pip install resonance_nn-0.1.0-py3-none-any.whl
pip install temporal_eigenstate_networks-0.1.0-py3-none-any.whl

# Install other requirements
pip install torch>=2.0.0 grpcio grpcio-tools
pip install pandas pyarrow h5py pyyaml
```

### 2. Generate gRPC Code

```bash
python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./src/grpc_services \
    --grpc_python_out=./src/grpc_services \
    ./proto/validation.proto
```

### 3. Start gRPC Server

```bash
# With mTLS (production)
python -m src.grpc_services.validation_server \
    --port 50051 \
    --use-mtls \
    --cert-dir /etc/synthos/certs

# Without mTLS (development only)
python -m src.grpc_services.validation_server \
    --port 50051 \
    --no-mtls
```

### 4. Test with Sample Data

```python
import grpc
from src.grpc_services import validation_pb2, validation_pb2_grpc

# Create channel with mTLS
credentials = grpc.ssl_channel_credentials(
    root_certificates=open('/etc/synthos/certs/ca.crt', 'rb').read(),
    private_key=open('/etc/synthos/certs/client.key', 'rb').read(),
    certificate_chain=open('/etc/synthos/certs/client.crt', 'rb').read()
)
channel = grpc.secure_channel('localhost:50051', credentials)
stub = validation_pb2_grpc.ValidationEngineStub(channel)

# Call service
request = validation_pb2.CascadeRequest(
    dataset_id="ds_test123",
    validation_id="val_test456",
    sample_s3_path="s3://bucket/sample.parquet",
    config=validation_pb2.CascadeConfig(
        target_architecture="resonance_nn",
        vocab_size=50257
    )
)

# Stream progress updates
for progress in stub.TrainCascade(request):
    print(f"Progress: {progress.progress_percent:.1f}% "
          f"({progress.models_completed}/{progress.models_total})")
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

## 🔧 GPU Optimization (4x H200)

### Memory Management

```yaml
# config/hardware_config.yaml
memory_optimization:
  gradient_accumulation_steps: 2
  max_memory_usage_percent: 90
  enable_cpu_offload: false      # Not needed with 320GB total
  clear_cache_between_models: true
```

### Parallel Training Strategy

- **Tier 1:** 2 GPUs, 5 models per GPU (10 total in parallel)
- **Tier 2:** 3 GPUs, distribute 5 models across GPUs
- **Tier 3:** 4 GPUs, Data Parallel (DDP) for each large model

### FFT Optimization

```python
# Use cuFFT (CUDA FFT library) for H200
config = {
    'fft_backend': 'cufft',  # Optimized for NVIDIA
    'use_fused_ops': True,   # Fuse FFT operations
    'compile_models': True    # torch.compile for additional speed
}
```

---

## 🧪 Testing

```bash
# Run unit tests
pytest tests/test_cascade.py -v

# Test gRPC server
pytest tests/test_grpc.py -v

# Load test (simulate cascade training)
python tests/load_test_cascade.py --num-models 18

# GPU memory test
python tests/test_gpu_memory.py --gpus 4
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

## 🤝 Team Communication

### Questions for Backend Team

1. **Data Format:** What format will datasets be in S3? (Recommend Parquet)
2. **Authentication:** How do we get mTLS certificates? (cert rotation?)
3. **Error Handling:** Should we implement circuit breaker pattern?
4. **Cost Tracking:** Do you need real-time GPU cost updates?
5. **Monitoring:** What metrics should we expose? (Prometheus?)

### What We Need from Backend

- S3 read credentials
- mTLS certificates (server cert, CA cert)
- Dataset IDs and validation IDs (UUID format?)
- gRPC endpoint to call back for status updates
- Error escalation process (who to notify on failures?)

---

## 📚 Additional Resources

- **Resonance NN Docs:** See `INSTALLATION_GUIDE.md`
- **Temporal Eigenstate:** Architecture in `src/model.py`
- **gRPC Best Practices:** Official guide
- **mTLS Setup:** See `docs/mtls_setup.md` (TODO)

---

## 🐛 Common Issues

### Issue: "CUDA out of memory"
**Solution:** Reduce batch size in `config/hardware_config.yaml` or enable gradient checkpointing

### Issue: "gRPC deadline exceeded"
**Solution:** Increase timeout in client:
```python
channel = grpc.secure_channel(
    'localhost:50051', 
    credentials,
    options=[('grpc.max_receive_message_length', 100 * 1024 * 1024)]
)
```

### Issue: "mTLS handshake failed"
**Solution:** Check certificate validity:
```bash
openssl verify -CAfile ca.crt server.crt
openssl verify -CAfile ca.crt client.crt
```

---

**Built with ❤️ and FFT | O(n log n) > O(n²) | No attention, just science**

*Last Updated: October 31, 2025*
