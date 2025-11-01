# 🔗 Module Integration Map

**How All Components Work Together**

---

## 🎯 The Big Picture

```
┌─────────────────────────────────────────────────────────────────┐
│                     SYNTHOS ORCHESTRATOR                         │
│                  (Single Entry Point)                            │
│                                                                   │
│  orchestrator.validate("data.parquet", "parquet")                │
└─────────────────────┬───────────────────────────────────────────┘
                      │
                      ↓
    ┌─────────────────────────────────────────────────┐
    │         AUTOMATIC 6-STAGE PIPELINE               │
    └─────────────────────────────────────────────────┘
                      │
    ┌─────────────────┴─────────────────┐
    ↓                                    ↓
┌────────────┐                    ┌─────────────┐
│  STAGE 1   │ ─────────────────→ │  STAGE 2    │
│   LOAD     │    Pass data       │  DIVERSITY  │
│            │                    │  ANALYSIS   │
└────────────┘                    └──────┬──────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │  STAGE 3    │
                                  │  CASCADE    │
                                  │  TRAINING   │
                                  └──────┬──────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │  STAGE 4    │
                                  │  COLLAPSE   │
                                  │  DETECTION  │
                                  └──────┬──────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │  STAGE 5    │
                                  │  PROBLEM    │
                                  │  LOCALIZE   │
                                  └──────┬──────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │  STAGE 6    │
                                  │  RECOMMEND  │
                                  └──────┬──────┘
                                         │
                                         ↓
                                  ┌─────────────┐
                                  │   FINAL     │
                                  │  DECISION   │
                                  │ ✅ or ❌    │
                                  └─────────────┘
```

---

## 📦 Module Responsibilities

### 🎛️ Orchestrator (src/orchestrator.py)
**The Master Controller**

```python
class SynthosOrchestrator:
    """
    Coordinates all modules automatically.
    You only interact with this class.
    """
```

**Responsibilities:**
- Initialize all sub-modules
- Manage pipeline flow
- Handle errors gracefully
- Track performance metrics
- Make final approval decision
- Generate reports

**Key Methods:**
- `__init__()` - Initialize all modules
- `validate()` - Run complete pipeline
- `_make_final_decision()` - Approve/reject logic

---

### 1️⃣ Data Loader (src/data_processors/dataset_loader.py)

```python
class DatasetLoader:
    """Universal dataset loader - handles all formats."""
```

**Input:** File path + format  
**Output:** Pandas DataFrame or PyTorch Tensor  
**Used by:** Orchestrator (Stage 1)

**Formats Supported:**
- CSV, JSON, Parquet
- HDF5, Arrow, Feather
- Excel, TSV, SQLite

**Flow:**
```
data.parquet → DatasetLoader → DataFrame → (convert to tensor) → Next Stage
```

---

### 2️⃣ Diversity Analyzer (src/validation_engine/diversity_analyzer.py)

```python
class DiversityAnalyzer:
    """Measures dataset diversity across multiple dimensions."""
```

**Input:** Dataset path + format  
**Output:** Diversity scores (0-100)  
**Used by:** Orchestrator (Stage 2)

**Calculates:**
- Semantic diversity (embeddings)
- Statistical diversity (distributions)
- Structural diversity (patterns)

**Flow:**
```
DataFrame → Analyze Features → Calculate Scores → diversity_result dict
```

---

### 3️⃣ Cascade Trainer (src/validation_engine/cascade_trainer.py)

```python
class CascadeTrainer:
    """Trains 3-tier cascade of Resonance NN models."""
```

**Input:** Data tensor  
**Output:** Trained models (18 total)  
**Used by:** Orchestrator (Stage 3)

**Architecture:**
- Tier 1: 10 tiny models (76M params each)
- Tier 2: 5 small models (454M params each)
- Tier 3: 3 base models (983M params each)

**Flow:**
```
Tensor → Train Tier 1 → Train Tier 2 → Train Tier 3 → Models + Predictions
```

---

### 4️⃣ Collapse Detector (src/collapse_engine/collapse_detector.py)

```python
class CollapseDetector:
    """Detects collapse across 8 dimensions using FFT analysis."""
```

**Input:** Synthetic data + Original data  
**Output:** Collapse scores (0-100)  
**Used by:** Orchestrator (Stage 4)

**8 Dimensions:**
1. Mode collapse
2. Spectral degradation
3. Gradient pathology
4. Distribution shift
5. Diversity loss
6. Memorization
7. Quality degradation
8. Pattern repetition

**Flow:**
```
Data → FFT Analysis → Score Each Dimension → Overall Score → collapse_result
```

---

### 5️⃣ Gradient Localizer (src/collapse_engine/gradient_localizer.py)

```python
class GradientLocalizer:
    """Pinpoints exact rows causing problems."""
```

**Input:** Dataset + Dimension scores  
**Output:** List of problematic row indices  
**Used by:** Orchestrator (Stage 5)

**Method:**
- Compute gradients for each row
- Score impact on collapse
- Rank by severity
- Return top offenders

**Flow:**
```
Tensor → Compute Gradients → Score Rows → Rank → problematic_indices[]
```

---

### 6️⃣ Recommendation Engine (src/collapse_engine/recommendation_engine.py)

```python
class RecommendationEngine:
    """Generates prioritized fixes with cost-benefit analysis."""
```

**Input:** Collapse scores + Diversity scores  
**Output:** List of recommendations  
**Used by:** Orchestrator (Stage 6)

**Recommendation Types:**
- Data augmentation
- Filtering strategies
- Re-sampling methods
- Model adjustments
- Hyperparameter tuning

**Flow:**
```
Scores → Analyze Issues → Generate Fixes → Prioritize → recommendations[]
```

---

### 🔧 GPU Optimizer (src/utils/gpu_optimizer.py)

```python
class GPUOptimizer:
    """Optimizes GPU usage for H100 efficiency."""
```

**Input:** Memory fraction + precision settings  
**Output:** Optimized PyTorch config  
**Used by:** All modules that use GPU

**Features:**
- Mixed precision (BF16/FP16)
- Memory management
- Multi-GPU support
- Gradient checkpointing

**Flow:**
```
Config → Set Memory → Enable BF16 → Configure CUDA → Return optimized env
```

---

## 🔄 Data Flow Example

Let's trace a validation request through the system:

### User Code:
```python
orchestrator = SynthosOrchestrator()
result = await orchestrator.validate("data.parquet", "parquet")
```

### What Happens:

#### 1. Orchestrator Initialization
```python
# Orchestrator.__init__()
self.gpu_optimizer = GPUOptimizer()           # Setup GPU
self.dataset_loader = DatasetLoader()         # Ready to load
self.diversity_analyzer = DiversityAnalyzer() # Ready to analyze
self.cascade_trainer = CascadeTrainer()       # Ready to train
self.collapse_detector = CollapseDetector()   # Ready to detect
self.gradient_localizer = GradientLocalizer() # Ready to localize
self.recommendation_engine = RecommendationEngine() # Ready to recommend
```

#### 2. Stage 1: Load Data
```python
# orchestrator.validate() calls:
dataset = await self.dataset_loader.load_dataset("data.parquet", "parquet")
# Returns: pandas DataFrame with all data
```

#### 3. Stage 2: Analyze Diversity
```python
# orchestrator.validate() calls:
diversity_result = await self.diversity_analyzer.analyze_diversity(
    "data.parquet", "parquet"
)
# Returns: {'overall_score': 68.2, 'semantic': 72.1, ...}
```

#### 4. Stage 3: Train Cascade
```python
# orchestrator.validate() calls:
data_tensor = torch.tensor(dataset.values)  # Convert to tensor
cascade_result = await self.cascade_trainer.train_cascade(
    data_tensor, num_tiers=3, models_per_tier=[10, 5, 3]
)
# Returns: {'models_per_tier': {...}, 'predictions': [...]}
```

#### 5. Stage 4: Detect Collapse
```python
# orchestrator.validate() calls:
collapse_result = await self.collapse_detector.detect_collapse(
    synthetic_data=data_tensor,
    original_data=reference_tensor
)
# Returns: {
#   'overall_score': 72.4,
#   'collapse_detected': False,
#   'dimensions': {'mode_collapse': 78.2, ...}
# }
```

#### 6. Stage 5: Localize Problems
```python
# orchestrator.validate() calls (if score < threshold):
localization_result = await self.gradient_localizer.localize_collapse(
    dataset=data_tensor,
    collapse_dimensions=collapse_result['dimensions']
)
# Returns: {
#   'problematic_indices': [42, 157, 891, ...],
#   'severity': 'medium'
# }
```

#### 7. Stage 6: Generate Recommendations
```python
# orchestrator.validate() calls:
recommendation_result = await self.recommendation_engine.generate_recommendations(
    collapse_score=72.4,
    dimension_scores=collapse_result['dimensions'],
    diversity_score=68.2
)
# Returns: {
#   'recommendations': [
#     {'title': 'Add data augmentation', 'impact': 8.5, ...},
#     ...
#   ],
#   'projected_score': 85.1
# }
```

#### 8. Final Decision
```python
# orchestrator._make_final_decision() calls:
approved = self._make_final_decision(
    collapse_score=72.4,
    diversity_score=68.2,
    dimension_scores={...}
)
# Returns: {
#   'approved': True,
#   'confidence': 87.3,
#   'reason': 'All quality metrics passed thresholds'
# }
```

#### 9. Return Result
```python
# orchestrator.validate() returns:
result = ValidationResult(
    validation_id='val_20251031_143022',
    approved_for_training=True,
    collapse_score=72.4,
    diversity_score=68.2,
    recommendations=[...],
    ...
)
# User receives: complete result object
```

---

## 🎯 Module Dependencies

```
orchestrator.py
    ├── requires: ALL modules below
    ├── data_processors/dataset_loader.py
    ├── validation_engine/diversity_analyzer.py
    ├── validation_engine/cascade_trainer.py
    │       └── requires: resonance_nn (external package)
    ├── collapse_engine/collapse_detector.py
    │       └── requires: resonance_nn (external package)
    ├── collapse_engine/gradient_localizer.py
    ├── collapse_engine/recommendation_engine.py
    └── utils/gpu_optimizer.py

resonance_nn (external)
    └── provides: ResonanceNN, HierarchicalFFT, etc.

temporal_eigenstate_networks (external)
    └── provides: TemporalFlowCell, EigenstateAttention
```

---

## 💡 Key Integration Points

### 1. Data Conversion
Orchestrator converts between formats automatically:
```python
# DataFrame → Tensor (for training)
if isinstance(dataset, pd.DataFrame):
    numeric_cols = dataset.select_dtypes(include=[np.number]).columns
    data_tensor = torch.tensor(dataset[numeric_cols].values, dtype=torch.float32)
```

### 2. Error Propagation
All modules raise exceptions that orchestrator catches:
```python
try:
    dataset = await self.dataset_loader.load_dataset(...)
except Exception as e:
    logger.error(f"❌ Data loading failed: {e}")
    raise  # Orchestrator handles gracefully
```

### 3. Progress Streaming
Orchestrator can stream progress to user:
```python
if stream_progress:
    print(f"✅ Loaded {total_rows:,} rows in {load_time:.2f}s")
```

### 4. Result Aggregation
Orchestrator collects all results into one object:
```python
result = ValidationResult(
    # From Stage 1
    data_loaded=True,
    load_time_seconds=5.2,
    
    # From Stage 2
    diversity_score=68.2,
    
    # ... all stages ...
)
```

---

## 🚀 Adding New Modules

To integrate a new module:

### 1. Create Module File
```python
# src/new_module/my_analyzer.py
class MyAnalyzer:
    async def analyze(self, data):
        # Your logic here
        return result
```

### 2. Add to Orchestrator Init
```python
# src/orchestrator.py
def __init__(self):
    # ... existing modules ...
    self.my_analyzer = MyAnalyzer()  # Add this
```

### 3. Add to Pipeline
```python
# src/orchestrator.py
async def validate(self, ...):
    # ... existing stages ...
    
    # NEW STAGE
    my_result = await self.my_analyzer.analyze(data_tensor)
```

### 4. Update ValidationResult
```python
# src/orchestrator.py
@dataclass
class ValidationResult:
    # ... existing fields ...
    my_analysis_score: float  # Add new field
```

### 5. Export in __init__.py
```python
# src/__init__.py
from src.new_module.my_analyzer import MyAnalyzer

__all__ = [
    # ... existing exports ...
    'MyAnalyzer',
]
```

---

## 📊 Performance Monitoring

Each module tracks its own performance:

```python
# In orchestrator.validate()
stage_start = asyncio.get_event_loop().time()

# ... run stage ...

stage_time = asyncio.get_event_loop().time() - stage_start

# Stored in result
result.diversity_time_seconds = stage_time
```

Total performance:
```python
result.total_time_seconds = sum([
    result.load_time_seconds,
    result.diversity_time_seconds,
    result.cascade_time_seconds,
    result.collapse_time_seconds,
    result.localization_time_seconds,
    result.recommendation_time_seconds
])
```

---

## 🎓 Summary

**Single Entry Point:**
```python
orchestrator = SynthosOrchestrator()
result = await orchestrator.validate(...)
```

**Automatic Pipeline:**
1. Load → 2. Diversity → 3. Cascade → 4. Collapse → 5. Localize → 6. Recommend

**All Modules Linked:**
- Orchestrator coordinates everything
- Each module focuses on one task
- Results flow automatically
- No manual coordination needed

**Simple API:**
- One class to import: `SynthosOrchestrator`
- One method to call: `validate()`
- One result object: `ValidationResult`

**That's the power of the unified pipeline!** 🚀
