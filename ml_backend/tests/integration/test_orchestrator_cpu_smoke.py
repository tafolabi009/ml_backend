import sys
import types
import importlib
import asyncio


def _make_fake_modules():
    """Create lightweight fake modules for torch, pandas and the src submodules
    so we can import `src.orchestrator` without heavy ML dependencies.
    """
    # --- fake torch ---
    torch_mod = types.ModuleType("torch")

    def fake_tensor(x, dtype=None):
        # return input as-is for lightweight tensor stub
        return x

    torch_mod.tensor = fake_tensor
    # minimal dtype placeholder
    torch_mod.float32 = object()

    # fake cuda namespace
    torch_cuda = types.ModuleType("torch.cuda")
    torch_cuda.is_available = lambda: False
    torch_cuda.max_memory_allocated = lambda: 0
    torch_mod.cuda = torch_cuda

    # --- fake pandas ---
    pandas_mod = types.ModuleType("pandas")
    pandas_mod.DataFrame = type("DataFrame", (object,), {})

    # --- minimal fake submodules for src ---
    # DatasetLoader
    dl_mod = types.ModuleType("src.data_processors.dataset_loader")

    class FakeDatasetLoader:
        def __init__(self, chunk_size: int = 100000):
            self.chunk_size = chunk_size

        async def load_dataset(self, path, fmt):
            # Return a small list of dicts (not a pandas.DataFrame) so
            # orchestrator follows the non-DataFrame branch.
            return [
                {"feature_1": 0.1, "feature_2": 1.0},
                {"feature_1": 0.2, "feature_2": 0.9},
            ]

    dl_mod.DatasetLoader = FakeDatasetLoader

    # DiversityAnalyzer
    da_mod = types.ModuleType("src.validation_engine.diversity_analyzer")

    class FakeDiversityAnalyzer:
        async def analyze_diversity(self, dataset_path, dataset_format):
            return {
                "overall_score": 80.0,
                "semantic_diversity": 80.0,
                "statistical_diversity": 80.0,
                "structural_diversity": 80.0,
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
            return {
                "overall_score": 85.0,
                "collapse_detected": False,
                "dimensions": {
                    "mode_collapse": 90.0,
                    "spectral_degradation": 88.0,
                    "gradient_pathology": 92.0,
                },
            }

    cd_mod.CollapseDetector = FakeCollapseDetector

    # SignatureLibrary (not used heavily in orchestrator at import)
    sl_mod = types.ModuleType("src.collapse_engine.signature_library")

    class FakeSignatureLibrary:
        pass

    sl_mod.SignatureLibrary = FakeSignatureLibrary

    # GradientLocalizer
    gl_mod = types.ModuleType("src.collapse_engine.gradient_localizer")

    class FakeGradientLocalizer:
        async def localize_collapse(self, dataset, collapse_dimensions):
            return {"problematic_indices": [], "top_contributors": [], "severity": 0}

    gl_mod.GradientLocalizer = FakeGradientLocalizer

    # RecommendationEngine
    re_mod = types.ModuleType("src.collapse_engine.recommendation_engine")

    class FakeRecommendationEngine:
        async def generate_recommendations(self, collapse_score, dimension_scores, diversity_score):
            return {"recommendations": [], "projected_score": collapse_score}

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

    # Attach classes to their package modules where appropriate
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

    # Insert into sys.modules
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


def test_orchestrator_cpu_smoke():
    """Import the orchestrator with fake dependencies and run a tiny
    CPU-only validation flow.
    """
    # Ensure local repository 'src' package takes precedence over any
    # similarly-named installed packages. Clear any previous imports first.
    from pathlib import Path
    project_root = str(Path(__file__).resolve().parents[2])
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    for key in list(sys.modules.keys()):
        if key == "src" or key.startswith("src."):
            sys.modules.pop(key, None)

    # Install fakes and import orchestrator
    _make_fake_modules()
    orchestrator_mod = importlib.import_module("src.orchestrator")

    # Instantiate the orchestrator (uses our fake GPU optimizer and submodules)
    orch = orchestrator_mod.SynthosOrchestrator(
        gpu_memory_fraction=0.1,
        enable_mixed_precision=False,
        collapse_threshold=65.0,
        diversity_threshold=50.0,
        use_cache=False,
    )

    async def run_and_assert():
        result = await orch.validate(
            dataset_path="fake.csv",
            dataset_format="csv",
            output_report_path=None,
            stream_progress=False,
        )

        # As our fakes report high scores, the dataset should be approved
        assert result.approved_for_training is True
        assert result.collapse_score >= 80.0
        assert result.diversity_score >= 80.0

    asyncio.run(run_and_assert())
