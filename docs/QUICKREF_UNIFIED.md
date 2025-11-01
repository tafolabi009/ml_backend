# 🎯 Unified Pipeline - Quick Reference Card

## ⚡ Basic Usage (3 Lines!)

```python
from src import SynthosOrchestrator

orchestrator = SynthosOrchestrator()
result = await orchestrator.validate("data.parquet", "parquet")
```

---

## 📋 Automatic 6-Stage Pipeline

```
Stage 1: LOAD       → DatasetLoader
Stage 2: ANALYZE    → DiversityAnalyzer
Stage 3: TRAIN      → CascadeTrainer (18 models)
Stage 4: DETECT     → CollapseDetector (8 dimensions)
Stage 5: LOCALIZE   → GradientLocalizer
Stage 6: RECOMMEND  → RecommendationEngine
         ↓
      DECISION (✅ or ❌)
```

**Total Time**: ~85s for 1M rows

---

## 📊 Result Object

```python
result.approved_for_training   # True/False
result.collapse_score          # 0-100
result.diversity_score         # 0-100
result.confidence              # 0-100%
result.reason                  # Explanation
result.problematic_rows        # List[int]
result.recommendations         # List[Dict]
result.total_time_seconds      # float
result.gpu_utilization_avg     # 0-100%
```

---

## ⚙️ Common Configurations

### Stricter Quality
```python
SynthosOrchestrator(
    collapse_threshold=80.0,
    diversity_threshold=70.0
)
```

### H100 Optimization
```python
SynthosOrchestrator(
    enable_mixed_precision=True,  # 2x faster
    gpu_memory_fraction=0.9
)
```

### With Progress
```python
result = await orchestrator.validate(
    "data.parquet", "parquet",
    stream_progress=True,
    output_report_path="report.json"
)
```

---

## ✅ Check Results

```python
if result.approved_for_training:
    print(f"✅ Score: {result.collapse_score:.1f}/100")
else:
    print(f"❌ {result.reason}")
    for rec in result.recommendations[:3]:
        print(f"💡 {rec['title']}: +{rec['estimated_impact']:.1f} pts")
```

---

## 📚 Documentation

- **User Guide**: [UNIFIED_PIPELINE.md](UNIFIED_PIPELINE.md)
- **Architecture**: [MODULE_INTEGRATION.md](MODULE_INTEGRATION.md)
- **Visual Diagrams**: [DATA_FLOW.md](DATA_FLOW.md)

---

## 🧪 Test

```bash
python test_unified_pipeline.py
python examples/unified_pipeline_simple.py
```

---

## 💰 Performance

| Rows | Time | Cost (4x H100) |
|------|------|----------------|
| 1M | 85s | $1.05 |
| 10M | 15min | $11.09 |
| 100M | 2.5h | $110.90 |
| **1B** | **25h** | **$1,109** |

---

**All modules work as one!** 🎉
