# 🔗 Unified Pipeline Guide

**All modules working together as one system**

---

## 🎯 Overview

The **SynthosOrchestrator** automatically links all 6 modules together:

```
Data → Diversity → Cascade → Collapse → Localization → Recommendations → Decision
  ↓        ↓          ↓          ↓            ↓               ↓             ↓
Load    Analyze    Train     Detect      Pinpoint         Fix         Approve/Reject
```

**You only need 3 lines of code:**

```python
from src import SynthosOrchestrator

orchestrator = SynthosOrchestrator()
result = await orchestrator.validate("data.parquet", "parquet")
```

That's it! The orchestrator handles everything else automatically.

---

## 🚀 Quick Start

### 1. Import the Orchestrator

```python
import asyncio
from src import SynthosOrchestrator
```

### 2. Initialize with Your Settings

```python
orchestrator = SynthosOrchestrator(
    collapse_threshold=65.0,        # Minimum quality score (0-100)
    diversity_threshold=50.0,       # Minimum diversity score (0-100)
    gpu_memory_fraction=0.9,        # Use 90% of GPU memory
    enable_mixed_precision=True,    # Use BF16 on H100 GPUs
    use_cache=True                  # Cache intermediate results
)
```

### 3. Run Validation

```python
async def validate_dataset():
    result = await orchestrator.validate(
        dataset_path="data/my_data.parquet",
        dataset_format="parquet",
        output_report_path="report.json",
        stream_progress=True  # Show real-time updates
    )
    return result

# Run it
result = asyncio.run(validate_dataset())
```

### 4. Check the Result

```python
if result.approved_for_training:
    print(f"✅ APPROVED! Score: {result.collapse_score:.1f}/100")
else:
    print(f"❌ REJECTED: {result.reason}")
    print(f"💡 {len(result.recommendations)} recommendations available")
```

---

## 📊 What Happens Automatically

When you call `orchestrator.validate()`, it automatically:

### Stage 1: Data Loading ⏱️ ~5s for 1M rows
- Loads dataset in any format (CSV, Parquet, JSON, etc.)
- Validates data integrity
- Prepares for processing

### Stage 2: Diversity Analysis ⏱️ ~10s for 1M rows
- Calculates semantic diversity
- Measures statistical spread
- Analyzes structural patterns
- **Score**: 0-100 (higher = more diverse)

### Stage 3: Cascade Training ⏱️ ~30s for 1M rows
- Trains 18 models (10 tiny + 5 small + 3 base)
- Uses Resonance NN architecture
- Learns data patterns
- Prepares for collapse detection

### Stage 4: Collapse Detection ⏱️ ~15s for 1M rows
- Checks 8 dimensions:
  - Mode collapse
  - Spectral degradation  
  - Gradient pathology
  - Distribution shift
  - Diversity loss
  - Memorization
  - Quality degradation
  - Pattern repetition
- **Score**: 0-100 (higher = better quality)

### Stage 5: Problem Localization ⏱️ ~20s for 1M rows
- If issues found, identifies exact problematic rows
- Uses gradient-based scoring
- Returns row indices for manual inspection

### Stage 6: Recommendations ⏱️ ~5s
- Generates prioritized fixes
- Calculates cost-benefit for each
- Projects improvement after fixes
- Provides actionable steps

### Final Decision ⏱️ instant
- Approves or rejects dataset
- Provides confidence score
- Explains reasoning
- Suggests next steps

**Total Time**: ~85 seconds for 1M rows

---

## 🎓 Complete Examples

### Example 1: Basic Validation

```python
import asyncio
from src import SynthosOrchestrator

async def main():
    orchestrator = SynthosOrchestrator()
    
    result = await orchestrator.validate(
        dataset_path="data.csv",
        dataset_format="csv"
    )
    
    print(f"Approved: {result.approved_for_training}")
    print(f"Score: {result.collapse_score:.1f}/100")

asyncio.run(main())
```

### Example 2: With Reference Dataset

```python
# Compare synthetic data against original
result = await orchestrator.validate(
    dataset_path="synthetic_data.parquet",
    dataset_format="parquet",
    reference_dataset_path="original_data.parquet"  # Compare against this
)
```

### Example 3: Custom Thresholds

```python
# Stricter requirements
orchestrator = SynthosOrchestrator(
    collapse_threshold=80.0,    # Need 80+ score (vs default 65)
    diversity_threshold=70.0    # Need 70+ diversity (vs default 50)
)

result = await orchestrator.validate("data.parquet", "parquet")
```

### Example 4: With Full Reporting

```python
result = await orchestrator.validate(
    dataset_path="data.parquet",
    dataset_format="parquet",
    output_report_path="validation_report.json",
    stream_progress=True
)

# Report saved to validation_report.json with:
# - All stage results
# - Dimension scores
# - Problematic row indices
# - Full recommendations
# - GPU metrics
# - Timing breakdown
```

### Example 5: CI/CD Integration

```python
#!/usr/bin/env python3
"""Validate dataset in CI/CD pipeline."""

import asyncio
import sys
from src import SynthosOrchestrator

async def main():
    orchestrator = SynthosOrchestrator()
    
    result = await orchestrator.validate(
        dataset_path=sys.argv[1],
        dataset_format=sys.argv[2],
        output_report_path="ci_validation_report.json"
    )
    
    if result.approved_for_training:
        print("✅ VALIDATION PASSED")
        return 0
    else:
        print(f"❌ VALIDATION FAILED: {result.reason}")
        return 1

exit_code = asyncio.run(main())
sys.exit(exit_code)
```

**Usage in CI:**
```bash
python validate_ci.py data/train.parquet parquet
```

---

## 📈 Understanding the Result

The `ValidationResult` object contains everything:

```python
result = await orchestrator.validate(...)

# Final decision
result.approved_for_training  # True/False
result.confidence             # 0-100 confidence score
result.reason                 # Explanation

# Quality metrics
result.collapse_score         # 0-100 overall quality
result.diversity_score        # 0-100 diversity
result.dimension_scores       # Dict of 8 dimension scores

# Problem details
result.problematic_rows       # List of row indices with issues
len(result.problematic_rows)  # Count of problematic rows

# Recommendations
result.recommendations        # List of fixes
result.projected_improvement  # Expected score increase

# Performance
result.total_time_seconds     # Total pipeline time
result.total_rows             # Rows processed
result.gpu_utilization_avg    # Average GPU utilization %
result.gpu_memory_used_gb     # Peak GPU memory

# Stage timings
result.load_time_seconds
result.diversity_time_seconds
result.cascade_time_seconds
result.collapse_time_seconds
result.localization_time_seconds
result.recommendation_time_seconds
```

### Save Report to JSON

```python
result.save_report("my_report.json")
```

---

## 🔧 Configuration Options

### Orchestrator Parameters

```python
SynthosOrchestrator(
    # Quality thresholds
    collapse_threshold=65.0,        # 0-100, reject if score below this
    diversity_threshold=50.0,       # 0-100, reject if diversity below this
    
    # GPU settings
    gpu_memory_fraction=0.9,        # 0.0-1.0, fraction of GPU memory to use
    enable_mixed_precision=True,    # Use BF16 for H100 efficiency
    
    # Performance
    use_cache=True                  # Cache intermediate results
)
```

### Validation Parameters

```python
orchestrator.validate(
    dataset_path="data.parquet",           # Required: path to dataset
    dataset_format="parquet",              # Required: csv, json, parquet, etc.
    reference_dataset_path=None,           # Optional: compare against this
    output_report_path=None,               # Optional: save JSON report here
    stream_progress=True                   # Optional: print progress updates
)
```

---

## 🎯 Decision Logic

The orchestrator approves a dataset if:

✅ **Collapse score ≥ threshold** (default 65)  
✅ **Diversity score ≥ threshold** (default 50)  
✅ **No critical dimensions < 40**

Critical dimensions:
- Mode collapse
- Spectral degradation
- Gradient pathology

---

## 💰 Performance at Scale

| Dataset Size | Time | Cost (4x H100) |
|--------------|------|----------------|
| 1K rows | <1s | $0.01 |
| 10K rows | ~5s | $0.06 |
| 100K rows | ~30s | $0.37 |
| 1M rows | ~5 min | $3.70 |
| 10M rows | ~30 min | $22.18 |
| 100M rows | ~4 hours | $177.44 |
| **1B rows** | **~36 hours** | **$1,597** |

*Based on $44.36/hour for GCP a3-highgpu-4g*

---

## 🐛 Troubleshooting

### Out of Memory

```python
# Reduce GPU memory usage
orchestrator = SynthosOrchestrator(
    gpu_memory_fraction=0.7  # Use only 70%
)
```

### Slow Processing

```python
# Enable mixed precision
orchestrator = SynthosOrchestrator(
    enable_mixed_precision=True  # 2x faster on H100
)
```

### False Rejections

```python
# Lower thresholds
orchestrator = SynthosOrchestrator(
    collapse_threshold=50.0,  # More lenient (was 65)
    diversity_threshold=40.0  # More lenient (was 50)
)
```

### Missing Dependencies

```bash
pip install -r requirements.txt
pip install packages/resonance_nn-0.1.0-py3-none-any.whl
pip install packages/temporal_eigenstate_networks-0.1.0-py3-none-any.whl
```

---

## 📚 Next Steps

1. **Try it**: Run `examples/unified_pipeline_simple.py`
2. **Integrate**: Add to your training pipeline
3. **Customize**: Adjust thresholds for your needs
4. **Scale**: Test with larger datasets
5. **Monitor**: Track GPU utilization and costs

---

## 🔗 Related Documentation

- [Architecture](ARCHITECTURE.md) - System design details
- [GCP Deployment](GCP_H100_DEPLOYMENT.md) - Production setup
- [Quick Start](QUICK_START.md) - 5-minute tutorial
- [API Reference](README.md) - Full API docs

---

**Questions?** Check the main [README.md](README.md) or review `examples/` folder.
