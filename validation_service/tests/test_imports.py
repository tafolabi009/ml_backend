"""
Test the validation_engine package imports correctly.
"""
import pytest


def test_validation_engine_imports():
    """Validation engine package should import without errors."""
    from validation_engine import DiversityAnalyzer, DiversityScore, StratificationConfig
    
    assert DiversityAnalyzer is not None
    assert DiversityScore is not None
    assert StratificationConfig is not None


def test_cascade_trainer_lazy_import():
    """CascadeTrainer should be accessible via lazy import."""
    from validation_engine import get_cascade_trainer
    
    assert get_cascade_trainer is not None
    assert callable(get_cascade_trainer)
    # Note: Actually calling get_cascade_trainer() may fail if resonance_nn not installed


def test_diversity_analyzer_instantiation():
    """DiversityAnalyzer should instantiate with defaults."""
    from validation_engine import DiversityAnalyzer, StratificationConfig
    
    # With default config
    analyzer = DiversityAnalyzer()
    assert analyzer is not None
    
    # With custom config
    config = StratificationConfig(use_gpu=False)
    analyzer2 = DiversityAnalyzer(config=config)
    assert analyzer2 is not None


def test_stratification_config_defaults():
    """StratificationConfig should have sensible defaults."""
    from validation_engine import StratificationConfig
    
    config = StratificationConfig()
    assert config.target_sample_size > 0
    assert config.min_bin_size > 0
    assert config.parallel_workers > 0
