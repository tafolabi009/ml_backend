# 🚀 Synthos ML Validation Engine

**World-class collapse detection for OpenAI/DeepMind scale datasets (1B+ rows)**

[![Status](https://img.shields.io/badge/status-production--ready-green)]()
[![GPU](https://img.shields.io/badge/GPU-4x%20H100-blue)]()
[![Scale](https://img.shields.io/badge/scale-1B%2B%20rows-purple)]()
[![Integration](https://img.shields.io/badge/integration-unified-brightgreen)]()
[![Cost](https://img.shields.io/badge/cost-%2444.36%2Fhr-orange)]()

---

## 🎯 Unified Pipeline - All Modules Work as One!

**NEW: All 6 modules now integrated through a single orchestrator!**

```python
from src import SynthosOrchestrator

# Single entry point - automatic pipeline
orchestrator = SynthosOrchestrator()
result = await orchestrator.validate("data.parquet", "parquet")

# Automatic 6-stage validation:
# ✅ Stage 1: Data Loading
# ✅ Stage 2: Diversity Analysis  
# ✅ Stage 3: Cascade Training
# ✅ Stage 4: Collapse Detection (8 dimensions)
# ✅ Stage 5: Problem Localization
# ✅ Stage 6: Recommendations

if result.approved_for_training:
    print(f"✅ APPROVED! Score: {result.collapse_score:.1f}/100")
else:
    print(f"❌ Issues: {result.reason}")
```

**See [INTEGRATION_COMPLETE.md](INTEGRATION_COMPLETE.md) for details!**

---

## 📁 Project Structure

```
ml_backend/
├── src/                          # Source code
│   ├── validation_engine/        # Phase 2-4: Diversity & cascade training
│   ├── collapse_engine/          # Phase 5-6: Detection & localization
│   ├── data_processors/          # Universal dataset loader
│   ├── grpc_services/            # gRPC server with mTLS
│   └── utils/                    # GPU optimization
│
├── config/                       # Configuration files
│   ├── hardware_config.yaml      # 4x H100 setup
│   └── ml_config.yaml            # Model configurations
│
├── proto/                        # Protocol buffer definitions
│   └── validation.proto          # gRPC service spec
│
├── examples/                     # Usage examples
│   └── complete_pipeline.py      # End-to-end demo
│
├── tests/                        # Test suites
│   ├── unit/                     # Unit tests
│   ├── integration/              # Integration tests
│   └── load/                     # Load tests (1B+ rows)
│
├── docs/                         # Documentation
│   ├── README.md                 # Main documentation
│   ├── ARCHITECTURE.md           # System architecture
│   ├── IMPLEMENTATION_STATUS.md  # Completion status
│   ├── QUICK_START.md            # 5-minute guide
│   └── GCP_H100_DEPLOYMENT.md    # GCP deployment guide
│
├── packages/                     # Custom architecture wheels
│   ├── resonance_nn-*.whl
│   └── temporal_eigenstate_networks-*.whl
│
├── scripts/                      # Helper scripts
│   ├── generate_certs.sh         # mTLS certificates
│   └── deployment/               # Deployment automation
│
├── deployment/                   # Deployment configs
│   ├── systemd/                  # Systemd service
│   ├── docker/                   # Docker setup
│   └── kubernetes/               # K8s manifests
│
├── requirements.txt              # Python dependencies
└── verify_installation.py        # Installation verifier
```

---

## ⚡ Quick Start (3 Lines of Code!)

### Option 1: Unified Pipeline (Recommended ⭐)

**All modules work together automatically:**

```python
from src import SynthosOrchestrator

orchestrator = SynthosOrchestrator()
result = await orchestrator.validate("data.parquet", "parquet")

if result.approved_for_training:
    print(f"✅ APPROVED! Score: {result.collapse_score:.1f}/100")
else:
    print(f"❌ Issues found. See {len(result.recommendations)} recommendations")
```

**That's it!** The orchestrator automatically:
1. ✅ Loads your data
2. ✅ Analyzes diversity  
3. ✅ Trains cascade models
4. ✅ Detects collapse across 8 dimensions
5. ✅ Localizes problematic rows
6. ✅ Generates prioritized recommendations
7. ✅ Makes approval decision

**See [UNIFIED_PIPELINE.md](docs/UNIFIED_PIPELINE.md) for complete guide.**

---

### Option 2: Manual Setup (Advanced)

If you want to use modules individually:

**1. Install Dependencies**
```bash
pip install -r requirements.txt
pip install packages/resonance_nn-0.1.0-py3-none-any.whl
pip install packages/temporal_eigenstate_networks-0.1.0-py3-none-any.whl
```

**2. Generate Certificates**
```bash
bash scripts/generate_certs.sh
```

**3. Run Example**
```bash
python examples/unified_pipeline_simple.py
```

**Expected Output:**
```
✅ APPROVED FOR TRAINING
   • Quality Score: 72.4/100
   • Diversity Score: 68.2/100
   • Confidence: 87.3%

🚀 You can now proceed with model training!
```

---

## 🎯 Features

### ✅ Core Capabilities

- **8-Dimensional Collapse Detection** - Most comprehensive in industry
- **FFT-Based Spectral Analysis** - Aligned with Resonance NN architecture
- **Gradient-Based Localization** - Pinpoint exact problematic rows
- **Intelligent Recommendations** - Prioritized fixes with cost-benefit analysis
- **Extreme Scale** - Optimized for 1B+ row datasets
- **GPU Optimization** - Mixed precision, >80% utilization target
- **Production-Grade** - gRPC with mTLS, streaming, error handling

### 📊 Dataset Support

CSV • JSON • Parquet • HDF5 • Arrow • Feather • Excel • TSV

---

## 💰 Hardware Configuration

### Current Setup (GCP a3-highgpu-4g)

| Component | Specification | Cost |
|-----------|--------------|------|
| **GPUs** | 4x NVIDIA H100 (80GB) | $28,605.93/mo |
| **Compute** | 104 vCPU + 936GB RAM | $3,452.92/mo |
| **Storage** | 500GB Hyperdisk + 3TB NVMe SSD | $325/mo |
| **TOTAL** | | **$32,383.85/mo** |

**Hourly Cost**: $44.36  
**Location**: us-central1-b  
**OS**: Rocky Linux 8 with NVIDIA Driver 580

---

## 📈 Performance Estimates

| Dataset Size | Time | Cost | GPU Util |
|--------------|------|------|----------|
| 10K rows | <1 min | $0.74 | 45% |
| 1M rows | 5 min | $3.70 | 75% |
| 100M rows | 45 min | $33.27 | 85% |
| **1B rows** | **6 hours** | **$266.16** | **90%** |

*Based on 4x H100 at $44.36/hour*

---

## 🚀 Deployment

### GCP Deployment

See [docs/GCP_H100_DEPLOYMENT.md](docs/GCP_H100_DEPLOYMENT.md) for complete guide.

**Quick deploy:**
```bash
gcloud compute instances create synthos-ml-validator \
    --zone=us-central1-b \
    --machine-type=a3-highgpu-4g \
    --accelerator=type=nvidia-h100-80gb,count=4 \
    --image=rocky-linux-8-nvidia-580 \
    --boot-disk-size=500GB \
    --local-ssd=interface=nvme,count=8
```

### Systemd Service

```bash
sudo cp deployment/systemd/synthos-validator.service /etc/systemd/system/
sudo systemctl enable synthos-validator
sudo systemctl start synthos-validator
```

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [UNIFIED_PIPELINE.md](docs/UNIFIED_PIPELINE.md) | **⭐ START HERE** - Complete guide for unified pipeline |
| [README.md](docs/README.md) | Complete developer guide & API reference |
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | System design & technical details |
| [QUICK_START.md](docs/QUICK_START.md) | 5-minute getting started |
| [IMPLEMENTATION_STATUS.md](docs/IMPLEMENTATION_STATUS.md) | What's complete & roadmap |
| [GCP_H100_DEPLOYMENT.md](docs/GCP_H100_DEPLOYMENT.md) | GCP deployment guide |

---

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/ -v --cov=src

# Integration tests
pytest tests/integration/ -v

# Load test (1B rows)
python tests/load/test_billion_rows.py
```

---

## 🔧 Configuration

### Hardware (config/hardware_config.yaml)

```yaml
gpus:
  total: 4
  model: "H100"
  memory_per_gpu_gb: 80

instance:
  type: "a3-highgpu-4g"
  region: "us-central1-b"
  cost_per_hour_usd: 44.36
```

### ML Models (config/ml_config.yaml)

```yaml
cascade:
  tiers:
    tier_1: { size: "tiny", models: 10, params: "76M" }
    tier_2: { size: "small", models: 5, params: "454M" }
    tier_3: { size: "base", models: 3, params: "983M" }
```

---

## 🎓 Usage Examples

### 🌟 Unified Pipeline (Simple - Recommended)

```python
import asyncio
from src import SynthosOrchestrator

async def main():
    # Initialize (links all modules together)
    orchestrator = SynthosOrchestrator(
        collapse_threshold=65.0,
        diversity_threshold=50.0
    )
    
    # Validate (automatic 6-stage pipeline)
    result = await orchestrator.validate(
        dataset_path="data.parquet",
        dataset_format="parquet",
        output_report_path="report.json",
        stream_progress=True  # Real-time progress
    )
    
    # Check result
    if result.approved_for_training:
        print(f"✅ APPROVED - Score: {result.collapse_score:.1f}/100")
    else:
        print(f"❌ REJECTED - {result.reason}")
        for rec in result.recommendations[:3]:
            print(f"  💡 {rec['title']}: +{rec['estimated_impact']:.1f} pts")

asyncio.run(main())
```

**See [docs/UNIFIED_PIPELINE.md](docs/UNIFIED_PIPELINE.md) for complete guide.**

---

### 📦 Individual Modules (Advanced)

If you need fine-grained control:

#### Basic Validation

```python
from src.validation_engine import DiversityAnalyzer
from src.collapse_engine import CollapseDetector

# Analyze diversity
analyzer = DiversityAnalyzer()
diversity = await analyzer.analyze_diversity("data.parquet", "parquet")

# Detect collapse
detector = CollapseDetector()
result = await detector.detect_collapse(synthetic_data, original_data)

if result.collapse_detected:
    print("❌ DO NOT TRAIN - Collapse detected!")
else:
    print("✅ APPROVED - Quality is excellent")
```

### With Recommendations

```python
from src.collapse_engine import RecommendationEngine

recommender = RecommendationEngine()
plan = await recommender.generate_recommendations(
    collapse_score=result.overall_score,
    dimension_scores=result.dimensions
)

print(f"Top Recommendations:")
for rec in plan.recommendations[:3]:
    print(f"  - {rec.title}: +{rec.estimated_impact} points, ${rec.cost_usd}")
```

---

## 🏆 Key Innovations

1. **FFT-Based Collapse Detection** - First to align with model architecture
2. **8-Dimensional Scoring** - Most comprehensive (vs industry standard 2-3)
3. **Gradient Localization** - Pinpoint exact problematic rows
4. **Smart Recommendations** - Not just "what's wrong" but "how to fix it"
5. **Extreme Scale** - Built for 1B+ rows from day one

---

## 📊 Component Status

| Component | LOC | Status |
|-----------|-----|--------|
| Diversity Analyzer | ~700 | ✅ Complete |
| Cascade Trainer | ~600 | ✅ Complete |
| Collapse Detector | ~800 | ✅ Complete |
| Signature Library | ~400 | ✅ Complete |
| Localizer | ~450 | ✅ Complete |
| Recommender | ~550 | ✅ Complete |
| GPU Optimizer | ~450 | ✅ Complete |
| gRPC Services | ~400 | ✅ Complete |
| Dataset Loader | ~500 | ✅ Complete |
| **TOTAL** | **~6,200** | **100% Complete** |

---

## 💡 Cost Optimization Tips

1. **Use Spot/Preemptible Instances** - 70% discount (risk: can be terminated)
2. **Committed Use Discounts** - 37-55% discount (1-3 year commitment)
3. **Right-size GPUs** - Use 2x H100 if workload fits in 160GB (50% savings)
4. **Auto-shutdown** - Stop instance during idle periods
5. **Regional Selection** - Some regions are cheaper

**Potential Savings**: $10K-15K/month with optimization

---

## 🆘 Support & Troubleshooting

### Common Issues

**Out of Memory:**
```bash
# Reduce batch size in config/hardware_config.yaml
batch_size: 32  # Was 64
```

**Low GPU Utilization:**
```bash
# Increase DataLoader workers
num_workers: 32  # Was 16
```

**Connection Issues:**
```bash
# Check firewall rules
gcloud compute firewall-rules list | grep ml-validator
```

### Getting Help

- 📖 Check [docs/](docs/) directory
- 🐛 Review `server.log` for errors
- 📊 Monitor with `nvidia-smi`
- 📞 Contact: ML Team

---

## 🔐 Security

- ✅ mTLS authentication (service-to-service)
- ✅ Certificate generation included
- ✅ Firewall rules configured
- ✅ Encrypted communication
- ✅ No public endpoints

---

## 📝 License

Internal use only - Synthos Platform

---

## 🎉 Credits

**Built by**: ML Engineering Team  
**Date**: October 31, 2025  
**Version**: 1.0.0  
**Status**: Production Ready

---

**Ready to validate at OpenAI/DeepMind scale!** 🚀

*"The best validation engine is the one that catches collapse before you waste $100K on training."*
