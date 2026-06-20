import pytest
import numpy as np
import torch
from src.collapse_engine.detector import CollapseDetector, CollapseConfig


async def _run_detection_once():
    """Run collapse detection with a fixed seed and return the result.

    Seeds are reset on every call so repeated invocations are directly
    comparable (the property the determinism test relies on).
    """
    np.random.seed(42)
    torch.manual_seed(42)

    n_samples = 1000
    n_features = 10

    # Original data: Gaussian mixture
    original_data = np.concatenate([
        np.random.normal(0, 1, (n_samples // 2, n_features)),
        np.random.normal(5, 2, (n_samples // 2, n_features)),
    ])

    # Synthetic data: slightly collapsed (smaller variance)
    synthetic_data = np.concatenate([
        np.random.normal(0, 0.8, (n_samples // 2, n_features)),
        np.random.normal(5, 1.5, (n_samples // 2, n_features)),
    ])

    config = CollapseConfig(use_gpu=False)  # Force CPU for cross-environment determinism
    detector = CollapseDetector(config)
    return await detector.detect_collapse(synthetic_data, original_data)


@pytest.mark.asyncio
async def test_collapse_is_deterministic():
    """The same seed must produce identical scores across runs."""
    result1 = await _run_detection_once()
    result2 = await _run_detection_once()

    assert result1.overall_score == pytest.approx(result2.overall_score, rel=1e-9)
    assert set(result1.dimensions) == set(result2.dimensions)
    for name in result1.dimensions:
        assert result1.dimensions[name].score == pytest.approx(
            result2.dimensions[name].score, rel=1e-9
        ), f"Dimension {name} is not deterministic"


@pytest.mark.asyncio
async def test_scores_within_documented_range():
    """Every dimension score and the overall score must lie within [0, 100]."""
    result = await _run_detection_once()

    assert 0.0 <= result.overall_score <= 100.0, (
        f"overall_score {result.overall_score} out of [0, 100]"
    )
    for name, dim in result.dimensions.items():
        assert 0.0 <= dim.score <= 100.0, (
            f"Dimension {name} score {dim.score} out of [0, 100]"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(test_collapse_is_deterministic())
    asyncio.run(test_scores_within_documented_range())
