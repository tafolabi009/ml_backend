# 🔄 Data Flow Visualization

## Unified Pipeline: From Data to Decision

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│                          YOUR CODE (3 lines)                             │
│                                                                          │
│   orchestrator = SynthosOrchestrator()                                  │
│   result = await orchestrator.validate("data.parquet", "parquet")       │
│   # Done! Everything else is automatic ✅                               │
│                                                                          │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                    SYNTHOS ORCHESTRATOR                                  │
│                  (Automatic Coordination)                                │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ↓
        ┌───────────────────────────────────────┐
        │    AUTOMATIC 6-STAGE PIPELINE          │
        │    (No manual coordination needed)     │
        └───────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ↓                               ↓
┏━━━━━━━━━━━━━━━━━━━┓                  ┏━━━━━━━━━━━━━━━━━━━┓
┃   STAGE 1: LOAD   ┃ ─────────────→   ┃  STAGE 2: ANALYZE ┃
┃                   ┃   Pass DataFrame  ┃    DIVERSITY      ┃
┃  Dataset Loader   ┃                  ┃                   ┃
┃                   ┃                  ┃  • Semantic       ┃
┃ • CSV, Parquet    ┃                  ┃  • Statistical    ┃
┃ • JSON, HDF5      ┃                  ┃  • Structural     ┃
┃ • Arrow, Excel    ┃                  ┃                   ┃
┃                   ┃                  ┃  Score: 0-100     ┃
┗━━━━━━━━━━━━━━━━━━━┛                  ┗━━━━━━━━┯━━━━━━━━━━┛
                                                 │
                                                 ↓
                                      ┏━━━━━━━━━━━━━━━━━━━┓
                                      ┃  STAGE 3: TRAIN   ┃
                                      ┃    CASCADE        ┃
                                      ┃                   ┃
                                      ┃  18 TEN models    ┃
                                      ┃  Models:          ┃
                                      ┃  • 10 tiny (76M)  ┃
                                      ┃  • 5 small (454M) ┃
                                      ┃  • 3 base (983M)  ┃
                                      ┗━━━━━━━━━┯━━━━━━━━━┛
                                                 │
                                                 ↓
                                      ┏━━━━━━━━━━━━━━━━━━━┓
                                      ┃ STAGE 4: DETECT   ┃
                                      ┃    COLLAPSE       ┃
                                      ┃                   ┃
                                      ┃  8 Dimensions:    ┃
                                      ┃  ✓ Mode collapse  ┃
                                      ┃  ✓ Spectral deg.  ┃
                                      ┃  ✓ Gradient path. ┃
                                      ┃  ✓ Distribution   ┃
                                      ┃  ✓ Diversity loss ┃
                                      ┃  ✓ Memorization   ┃
                                      ┃  ✓ Quality deg.   ┃
                                      ┃  ✓ Pattern repeat ┃
                                      ┃                   ┃
                                      ┃  Score: 0-100     ┃
                                      ┗━━━━━━━━━┯━━━━━━━━━┛
                                                 │
                                                 ↓
                                      ┏━━━━━━━━━━━━━━━━━━━┓
                                      ┃ STAGE 5: LOCALIZE ┃
                                      ┃    PROBLEMS       ┃
                                      ┃                   ┃
                                      ┃  Gradient-based   ┃
                                      ┃  row scoring      ┃
                                      ┃                   ┃
                                      ┃  Output:          ┃
                                      ┃  [42, 157, 891,   ┃
                                      ┃   1034, 2048...]  ┃
                                      ┗━━━━━━━━━┯━━━━━━━━━┛
                                                 │
                                                 ↓
                                      ┏━━━━━━━━━━━━━━━━━━━┓
                                      ┃ STAGE 6: RECOMMEND┃
                                      ┃    FIXES          ┃
                                      ┃                   ┃
                                      ┃  Prioritized:     ┃
                                      ┃  1. Augmentation  ┃
                                      ┃  2. Filtering     ┃
                                      ┃  3. Re-sampling   ┃
                                      ┃                   ┃
                                      ┃  With cost-benefit┃
                                      ┗━━━━━━━━━┯━━━━━━━━━┛
                                                 │
                                                 ↓
                                      ┏━━━━━━━━━━━━━━━━━━━┓
                                      ┃  FINAL DECISION   ┃
                                      ┃                   ┃
                                      ┃  Logic:           ┃
                                      ┃  • Score >= 65?   ┃
                                      ┃  • Diversity>=50? ┃
                                      ┃  • Critical OK?   ┃
                                      ┃                   ┃
                                      ┃  ✅ APPROVED      ┃
                                      ┃     or            ┃
                                      ┃  ❌ REJECTED      ┃
                                      ┗━━━━━━━━━┯━━━━━━━━━┛
                                                 │
                                                 ↓
┌─────────────────────────────────────────────────────────────────────────┐
│                      VALIDATION RESULT                                   │
│                                                                          │
│  result.approved_for_training    # True/False                           │
│  result.collapse_score           # 72.4/100                             │
│  result.diversity_score          # 68.2/100                             │
│  result.confidence               # 87.3%                                │
│  result.reason                   # "All metrics passed"                 │
│  result.problematic_rows         # [42, 157, 891...]                    │
│  result.recommendations          # [rec1, rec2, rec3...]                │
│  result.total_time_seconds       # 85.2s                                │
│  result.gpu_utilization_avg      # 82.4%                                │
│                                                                          │
│  result.save_report("report.json")  # Save full report                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Module Communication

```
┌──────────────────────────────────────────────────────────────────┐
│                     DATA FLOW BETWEEN MODULES                     │
└──────────────────────────────────────────────────────────────────┘

DatasetLoader
     │
     │ Returns: pandas.DataFrame
     │          shape: (1000000, 50)
     ↓
DiversityAnalyzer  
     │
     │ Returns: {
     │   'overall_score': 68.2,
     │   'semantic_diversity': 72.1,
     │   'statistical_diversity': 65.8,
     │   'structural_diversity': 66.7
     │ }
     ↓
CascadeTrainer
     │ 
     │ Takes: torch.Tensor (converted from DataFrame)
     │        shape: (1000000, 50)
     │
     │ Returns: {
     │   'models_per_tier': {'tier_1': 10, 'tier_2': 5, 'tier_3': 3},
     │   'predictions': torch.Tensor(1000000, 50),
     │   'training_time': 30.5
     │ }
     ↓
CollapseDetector
     │
     │ Takes: synthetic_data: torch.Tensor
     │        original_data: torch.Tensor
     │
     │ Returns: {
     │   'overall_score': 72.4,
     │   'collapse_detected': False,
     │   'dimensions': {
     │     'mode_collapse': 78.2,
     │     'spectral_degradation': 74.5,
     │     'gradient_pathology': 69.8,
     │     'distribution_shift': 71.2,
     │     'diversity_loss': 70.1,
     │     'memorization': 75.3,
     │     'quality_degradation': 73.6,
     │     'pattern_repetition': 76.9
     │   }
     │ }
     ↓
GradientLocalizer (if score < threshold)
     │
     │ Takes: dataset: torch.Tensor
     │        collapse_dimensions: Dict[str, float]
     │
     │ Returns: {
     │   'problematic_indices': [42, 157, 891, 1034, 2048, ...],
     │   'severity': 'medium',
     │   'top_contributors': ['row_42', 'row_157', 'row_891']
     │ }
     ↓
RecommendationEngine
     │
     │ Takes: collapse_score: 72.4
     │        dimension_scores: Dict[str, float]
     │        diversity_score: 68.2
     │
     │ Returns: {
     │   'recommendations': [
     │     {
     │       'title': 'Add data augmentation',
     │       'estimated_impact': 8.5,
     │       'cost_usd': 2500,
     │       'priority': 'high'
     │     },
     │     ...
     │   ],
     │   'projected_score': 85.1
     │ }
     ↓
ORCHESTRATOR DECISION
     │
     │ Logic: collapse_score >= 65 AND
     │        diversity_score >= 50 AND
     │        no critical_dimension < 40
     │
     └─→ APPROVED ✅ or REJECTED ❌
```

---

## Error Handling Flow

```
┌────────────────────────────────────────────────────────────┐
│                    ERROR PROPAGATION                        │
└────────────────────────────────────────────────────────────┘

Stage 1: DatasetLoader.load_dataset()
     │
     ├─→ Success → Continue to Stage 2
     │
     └─→ Exception → Orchestrator catches
                      ↓
                   Log error
                      ↓
                   Raise with context
                      ↓
                   User sees clear error message

Stage 2-6: Similar pattern
     │
     ├─→ Success → Continue
     │
     └─→ Exception → Orchestrator catches
                      ↓
                   Save partial results
                      ↓
                   Generate report with what succeeded
                      ↓
                   Return ValidationResult (partial=True)
```

---

## Performance Tracking

```
┌────────────────────────────────────────────────────────────┐
│                   TIMING BREAKDOWN                          │
└────────────────────────────────────────────────────────────┘

Total: 85.2s for 1M rows

├─ Stage 1: Load           5.2s  ( 6%)  ████
├─ Stage 2: Diversity     10.1s  (12%)  ████████
├─ Stage 3: Cascade       30.5s  (36%)  ████████████████████████
├─ Stage 4: Collapse      15.3s  (18%)  ████████████
├─ Stage 5: Localize      19.8s  (23%)  ███████████████
└─ Stage 6: Recommend      4.3s  ( 5%)  ███

GPU Utilization: 82.4% average
Peak Memory: 45.2 GB / 320 GB (14%)
Throughput: 11,737 rows/second
```

---

## Parallel Execution (Future)

```
Current (Sequential):
Stage 1 → Stage 2 → Stage 3 → Stage 4 → Stage 5 → Stage 6
  5s      10s       30s       15s       20s       5s
                Total: 85s

Future (Parallel where possible):
         ┌─→ Stage 2 (10s) ─┐
Stage 1  │                  ├─→ Stage 3 (30s) → Stage 4 (15s)
  (5s)   └─→ Validation ────┘                        ↓
                                              Stage 5 (20s) → Stage 6 (5s)
                Total: ~55s (35% faster!)
```

---

## Configuration Flow

```
┌────────────────────────────────────────────────────────────┐
│               USER CONFIGURATION → MODULES                  │
└────────────────────────────────────────────────────────────┘

SynthosOrchestrator(
    collapse_threshold=65.0,      ──→  Used in final decision
    diversity_threshold=50.0,     ──→  Used in final decision
    gpu_memory_fraction=0.9,      ──→  Passed to GPUOptimizer
    enable_mixed_precision=True,  ──→  Passed to all GPU modules
    use_cache=True               ──→  Used by all modules
)

GPUOptimizer gets config ──→ Sets up PyTorch
                              ├─ torch.cuda.set_per_process_memory_fraction(0.9)
                              ├─ Enable BF16 autocast
                              └─ Configure CUDA streams

All modules inherit optimized environment ✅
```

---

## Summary

**Single Call:**
```python
result = await orchestrator.validate("data.parquet", "parquet")
```

**Automatic Flow:**
1. Load → 2. Analyze → 3. Train → 4. Detect → 5. Localize → 6. Recommend → Decision

**Complete Result:**
- Approval decision (✅/❌)
- Quality scores (0-100)
- Problem locations (row indices)
- Fix recommendations (prioritized)
- Performance metrics (time, GPU, throughput)

**All coordinated automatically by the orchestrator!** 🚀

---

*Last Updated: June 2026*
