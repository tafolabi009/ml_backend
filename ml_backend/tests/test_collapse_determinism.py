import pytest
import numpy as np
import torch
from src.collapse_engine.detector import CollapseDetector, CollapseConfig

@pytest.mark.asyncio
async def test_collapse_determinism():
    """
    Verify that the collapse detection is deterministic given a fixed seed.
    This ensures that the math verification is consistent.
    """
    # Set seeds
    np.random.seed(42)
    torch.manual_seed(42)
    
    # Generate fixed synthetic and original data
    # 1000 samples, 10 features
    n_samples = 1000
    n_features = 10
    
    # Original data: Gaussian mixture
    original_data = np.concatenate([
        np.random.normal(0, 1, (n_samples // 2, n_features)),
        np.random.normal(5, 2, (n_samples // 2, n_features))
    ])
    
    # Synthetic data: Slightly collapsed (smaller variance)
    synthetic_data = np.concatenate([
        np.random.normal(0, 0.8, (n_samples // 2, n_features)),
        np.random.normal(5, 1.5, (n_samples // 2, n_features))
    ])
    
    # Initialize detector
    config = CollapseConfig(use_gpu=False) # Force CPU for determinism across environments
    detector = CollapseDetector(config)
    
    # Run detection
    result = await detector.detect_collapse(synthetic_data, original_data)
    
    # Check overall score
    print(f"Overall Score: {result.overall_score}")
    
    # Assertions for determinism
    # We expect a specific score based on the fixed seed.
    # Using approx to handle minor floating point differences across architectures
    expected_overall_score = 71.94210935084276
    assert result.overall_score == pytest.approx(expected_overall_score, rel=1e-6)
    
    # Check dimension scores
    expected_scores = {
        'distribution_fidelity': 131.15373630909923,
        'correlation_preservation': 73.89455984636021,
        'entropy_stability': 86.70519852963062,
        'gradient_health': 60.0,
        'loss_landscape': 65.0,
        'spectral_coherence': 56.68344524749641,
        'generalization_gap': 54.36666666666666,
        'statistical_consistency': 47.73326820748895
    }
    
    for dim_name, expected_score in expected_scores.items():
        actual_score = result.dimensions[dim_name].score
        print(f"{dim_name}: {actual_score}")
        assert actual_score == pytest.approx(expected_score, rel=1e-6), f"Dimension {dim_name} mismatch"

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_collapse_determinism())
