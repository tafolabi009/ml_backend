import sys
import types
import importlib
import asyncio


def _make_fake_modules(low_diversity=False, collapse_detected=False):
    """Create fake modules parametrized to simulate negative scenarios.
    If low_diversity is True, the DiversityAnalyzer returns a low score.
    If collapse_detected is True, CollapseDetector returns low overall_score.
    """
    # --- fake torch ---
    torch_mod = types.ModuleType("torch")

    def fake_tensor(x, dtype=None):
        return x

    torch_mod.tensor = fake_tensor
    torch_mod.float32 = object()

    torch_cuda = types.ModuleType("torch.cuda")
    torch_cuda.is_available = lambda: False
    torch_cuda.max_memory_allocated = lambda: 0
    torch_mod.cuda = torch_cuda

    # --- fake pandas ---
    pandas_mod = types.ModuleType("pandas")
    pandas_mod.DataFrame = type("DataFrame", (object,), {})

    # DatasetLoader
    dl_mod = types.ModuleType("src.data_processors.dataset_loader")

    class FakeDatasetLoader:
        def __init__(self, chunk_size: int = 100000):
            self.chunk_size = chunk_size

        async def load_dataset(self, path, fmt):
            return [{"feature_1": 0.1}, {"feature_1": 0.2}]

    dl_mod.DatasetLoader = FakeDatasetLoader

    # DiversityAnalyzer
    da_mod = types.ModuleType("src.validation_engine.diversity_analyzer")

    class FakeDiversityAnalyzer:
        async def analyze_diversity(self, dataset_path, dataset_format):
            score = 30.0 if low_diversity else 80.0
            return {
                "overall_score": score,
                "semantic_diversity": score,
                "statistical_diversity": score,
                "structural_diversity": score,
            }

    da_mod.DiversityAnalyzer = FakeDiversityAnalyzer

    # CascadeTrainer
    ct_mod = types.ModuleType("src.validation_engine.cascade_trainer")

    class FakeCascadeTrainer:
        async def train_cascade(self, data_tensor, num_tiers=3, models_per_tier=None):
            return {"models_per_tier": {"tier_1": 1, "tier_2": 1, "tier_3": 1}}

    ct_mod.CascadeTrainer = FakeCascadeTrainer

    # CollapseDetector
    cd_mod = types.ModuleType("src.collapse_engine.collapse_detector")

    class FakeCollapseDetector:
        async def detect_collapse(self, synthetic_data, original_data):
            overall = 30.0 if collapse_detected else 85.0
            return {
                "overall_score": overall,
                "collapse_detected": collapse_detected,
                "dimensions": {
                    "mode_collapse": 30.0 if collapse_detected else 90.0,
                    "spectral_degradation": 30.0 if collapse_detected else 88.0,
                    "gradient_pathology": 30.0 if collapse_detected else 92.0,
                },
            }

    cd_mod.CollapseDetector = FakeCollapseDetector

    # SignatureLibrary
    sl_mod = types.ModuleType("src.collapse_engine.signature_library")
    sl_mod.SignatureLibrary = type("SignatureLibrary", (), {})

    # GradientLocalizer
    gl_mod = types.ModuleType("src.collapse_engine.gradient_localizer")

    class FakeGradientLocalizer:
        async def localize_collapse(self, dataset, collapse_dimensions):
            return {"problematic_indices": [0], "top_contributors": ["feature_1"], "severity": 0}

    gl_mod.GradientLocalizer = FakeGradientLocalizer

    # RecommendationEngine
    re_mod = types.ModuleType("src.collapse_engine.recommendation_engine")

    class FakeRecommendationEngine:
        async def generate_recommendations(self, collapse_score, dimension_scores, diversity_score):
            return {"recommendations": [{"title": "Fix X", "estimated_impact": 5.0, "cost_usd": 100}], "projected_score": collapse_score + 5}

    re_mod.RecommendationEngine = FakeRecommendationEngine

    # GPUOptimizer
    go_mod = types.ModuleType("src.utils.gpu_optimizer")

    class FakeGPUOptimizer:
        def __init__(self, memory_fraction=0.9, enable_mixed_precision=True):
            pass

    go_mod.GPUOptimizer = FakeGPUOptimizer

    # Create lightweight package modules to avoid importing on-disk packages
    pkg_src = types.ModuleType("src")
    pkg_data_processors = types.ModuleType("src.data_processors")
    pkg_validation_engine = types.ModuleType("src.validation_engine")
    pkg_collapse_engine = types.ModuleType("src.collapse_engine")
    pkg_utils = types.ModuleType("src.utils")

    pkg_data_processors.DatasetLoader = dl_mod.DatasetLoader
    pkg_validation_engine.DiversityAnalyzer = da_mod.DiversityAnalyzer
    pkg_validation_engine.CascadeTrainer = ct_mod.CascadeTrainer
    pkg_collapse_engine.CollapseDetector = cd_mod.CollapseDetector
    pkg_collapse_engine.SignatureLibrary = sl_mod.SignatureLibrary
    pkg_collapse_engine.GradientLocalizer = gl_mod.GradientLocalizer
    pkg_collapse_engine.RecommendationEngine = re_mod.RecommendationEngine
    pkg_utils.GPUOptimizer = go_mod.GPUOptimizer

    # Mark pkg_src as a package pointing to the real src/ directory so
    # submodule imports (like src.orchestrator) can be loaded from disk.
    from pathlib import Path
    project_root = str(Path(__file__).resolve().parents[2])
    pkg_src.__path__ = [str(Path(project_root) / "src")]

    sys.modules.update({
        "torch": torch_mod,
        "torch.cuda": torch_cuda,
        "pandas": pandas_mod,
        "src": pkg_src,
        "src.data_processors": pkg_data_processors,
        "src.validation_engine": pkg_validation_engine,
        "src.collapse_engine": pkg_collapse_engine,
        "src.utils": pkg_utils,
        "src.data_processors.dataset_loader": dl_mod,
        "src.validation_engine.diversity_analyzer": da_mod,
        "src.validation_engine.cascade_trainer": ct_mod,
        "src.collapse_engine.collapse_detector": cd_mod,
        "src.collapse_engine.signature_library": sl_mod,
        "src.collapse_engine.gradient_localizer": gl_mod,
        "src.collapse_engine.recommendation_engine": re_mod,
        "src.utils.gpu_optimizer": go_mod,
    })


def _import_orchestrator_local():
    # Ensure local repo 'src' is preferred
    from pathlib import Path
    project_root = str(Path(__file__).resolve().parents[2])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    return importlib.import_module("src.orchestrator")


def _clear_src_modules():
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key, None)


def test_low_diversity_rejects():
    _clear_src_modules()
    _make_fake_modules(low_diversity=True, collapse_detected=False)
    orchestrator_mod = _import_orchestrator_local()
    orch = orchestrator_mod.SynthosOrchestrator(enable_mixed_precision=False, use_cache=False)

    async def run():
        res = await orch.validate("fake.csv", "csv", stream_progress=False)
        assert res.approved_for_training is False
        assert "Diversity" in res.reason or res.diversity_score < orch.diversity_threshold

    asyncio.run(run())


def test_collapse_detected_rejects():
    _clear_src_modules()
    _make_fake_modules(low_diversity=False, collapse_detected=True)
    orchestrator_mod = _import_orchestrator_local()
    orch = orchestrator_mod.SynthosOrchestrator(enable_mixed_precision=False, use_cache=False)

    async def run():
        res = await orch.validate("fake.csv", "csv", stream_progress=False)
        assert res.approved_for_training is False
        assert "Collapse" in res.reason or res.collapse_score < orch.collapse_threshold

    asyncio.run(run())
