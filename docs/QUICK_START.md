# 🚀 Quick Start Guide - ML Validation Engine

## 1️⃣ Installation (2 minutes)

```bash
# Clone/navigate to project
cd /workspaces/ml_backend

# Install dependencies
pip install -r requirements.txt

# Install custom architectures (if not already installed)
pip install resonance_nn-0.1.0-py3-none-any.whl
pip install temporal_eigenstate_networks-0.1.0-py3-none-any.whl

# Generate certificates for testing
bash scripts/generate_certs.sh
```

## 2️⃣ Run Example Pipeline (3 minutes)

```bash
# Complete validation pipeline with synthetic data
python examples/complete_pipeline.py
```

Expected output:
```
✅ All 6 steps completed successfully!
📊 Final Assessment:
   - Current Score: 72.4/100
   - Collapse Detected: False
   - Recommendations: 3
   - Projected Score: 87.6/100
```

## 3️⃣ Basic Usage

### Import Components

```python
from src.validation_engine import DiversityAnalyzer
from src.collapse_engine import CollapseDetector, SignatureLibrary, RecommendationEngine
```

### Analyze Diversity

```python
analyzer = DiversityAnalyzer()
diversity = await analyzer.analyze_diversity(
    data_path="data/dataset.parquet",
    data_format="parquet"
)
print(f"Diversity Score: {diversity.overall_score}/100")
```

### Detect Collapse

```python
detector = CollapseDetector()
result = await detector.detect_collapse(
    synthetic_data=synth_data,
    original_data=orig_data
)

if result.collapse_detected:
    print("❌ COLLAPSE DETECTED - Do not train!")
    for warning in result.warnings:
        print(warning)
```

### Get Recommendations

```python
recommender = RecommendationEngine()
plan = await recommender.generate_recommendations(
    collapse_score=result.overall_score,
    dimension_scores={d.name: d.score for d in result.dimensions.values()}
)

print(f"Top Recommendations:")
for rec in plan.recommendations[:3]:
    print(f"  - {rec.title}: +{rec.estimated_impact} points")
```

## 4️⃣ Configuration

### Hardware (4x H200 GPUs)

Edit `config/hardware_config.yaml`:
```yaml
gpus:
  total: 4
  per_tier:
    tier_1: 2  # 10 models parallel
    tier_2: 3  # 5 models
    tier_3: 4  # 3 models with DDP
```

### ML Models

Edit `config/ml_config.yaml`:
```yaml
cascade:
  tiers:
    tier_1:
      architecture: "resonance_nn"
      model_size: "tiny"
      num_models: 10
      parallel: true
```

## 5️⃣ gRPC Server

### Start Server

```python
from src.grpc_services.validation_server import serve

# Start server with mTLS
serve(
    port=50051,
    cert_path="/tmp/synthos_certs/server.crt",
    key_path="/tmp/synthos_certs/server.key",
    ca_path="/tmp/synthos_certs/ca.crt"
)
```

### Client Example

```python
import grpc
from src.grpc_services import validation_pb2, validation_pb2_grpc

# Load certificates
with open("/tmp/synthos_certs/ca.crt", "rb") as f:
    ca_cert = f.read()
with open("/tmp/synthos_certs/client.crt", "rb") as f:
    client_cert = f.read()
with open("/tmp/synthos_certs/client.key", "rb") as f:
    client_key = f.read()

# Create credentials
credentials = grpc.ssl_channel_credentials(
    root_certificates=ca_cert,
    private_key=client_key,
    certificate_chain=client_cert
)

# Connect
channel = grpc.secure_channel('localhost:50051', credentials)
stub = validation_pb2_grpc.ValidationEngineStub(channel)

# Call service
response = stub.AnalyzeDiversity(request)
```

## 6️⃣ GPU Optimization

### Enable Mixed Precision

```python
from src.utils import GPUOptimizer, OptimizationConfig

optimizer = GPUOptimizer(OptimizationConfig(
    use_mixed_precision=True,
    precision="bf16",  # BF16 on H200
    gradient_checkpointing=True,
    compile_model=True
))

# Optimize model
model = optimizer.optimize_model(model, distributed=True)
```

### Monitor Utilization

```python
# Real-time monitoring
optimizer.monitor_utilization(
    target_utilization=80.0,
    duration_seconds=60
)
```

## 7️⃣ Common Commands

```bash
# Check GPU status
nvidia-smi

# Install additional packages
pip install faiss-gpu  # For GPU-accelerated similarity search
pip install pynvml      # For GPU monitoring

# Run with specific GPU
CUDA_VISIBLE_DEVICES=0 python examples/complete_pipeline.py

# Profile performance
python -m torch.utils.bottleneck examples/complete_pipeline.py
```

## 8️⃣ Troubleshooting

### Out of Memory
```python
# Reduce batch size in config/hardware_config.yaml
batch_sizes:
  tier_1: 128  # Was 256
  tier_2: 64   # Was 128
```

### Low GPU Utilization
```python
# Increase batch size or reduce DataLoader workers
config = OptimizationConfig(
    num_workers=4,  # Reduce if CPU bottleneck
    prefetch_factor=4  # Increase for better throughput
)
```

### gRPC Connection Errors
```bash
# Verify certificates exist
ls -l /tmp/synthos_certs/

# Check server is running
netstat -an | grep 50051

# Test with grpcurl
grpcurl -plaintext localhost:50051 list
```

## 9️⃣ Directory Structure

```
/workspaces/ml_backend/
├── src/                    # Source code
│   ├── validation_engine/  # Phase 2-4
│   ├── collapse_engine/    # Phase 5-6
│   ├── data_processors/    # Dataset loading
│   ├── grpc_services/      # gRPC server
│   └── utils/              # GPU optimization
├── config/                 # Configuration files
├── examples/               # Usage examples
├── scripts/                # Helper scripts
├── proto/                  # Protocol definitions
└── data/                   # Data storage
```

## 🔟 Next Steps

1. **Test with Real Data**: Replace synthetic data in examples
2. **Tune for Your Hardware**: Adjust batch sizes, workers
3. **Monitor Performance**: Use GPU optimizer profiling
4. **Deploy to Production**: Set up Kubernetes, monitoring
5. **Continuous Improvement**: Update signature library

---

## 📚 Documentation

- **Full Guide**: `README.md`
- **Architecture**: `ARCHITECTURE.md`
- **Status**: `IMPLEMENTATION_STATUS.md`
- **API Reference**: Module docstrings

---

**Need Help?**
- Check examples: `examples/complete_pipeline.py`
- Review configs: `config/*.yaml`
- Read docstrings: All modules have detailed docs

**Ready to validate at scale!** 🚀
