"""
Real Unit Tests for CollapseDetector
====================================

Tests the actual collapse detection logic with real data.
"""

import pytest
import numpy as np
import torch
from src.collapse_engine.detector import CollapseDetector, CollapseConfig, DimensionScore


@pytest.fixture
def detector():
    """Create a CollapseDetector instance for testing"""
    config = CollapseConfig(use_gpu=False)  # Force CPU for CI/CD
    return CollapseDetector(config)


@pytest.fixture
def healthy_data():
    """Generate healthy synthetic data that matches original distribution"""
    np.random.seed(42)
    # Create varied, realistic data
    return np.random.randn(1000, 10).astype(np.float32)


@pytest.fixture
def collapsed_data():
    """Generate collapsed data (mode collapse - repetitive patterns)"""
    np.random.seed(42)
    # Create repetitive data (mode collapse)
    base_pattern = np.random.randn(10, 10).astype(np.float32)
    return np.tile(base_pattern, (100, 1))  # Repeat pattern 100 times


@pytest.fixture
def original_data():
    """Generate original training data"""
    np.random.seed(42)
    return np.random.randn(1000, 10).astype(np.float32)


class TestCollapseDetector:
    """Test suite for CollapseDetector"""

    def test_initialization(self, detector):
        """Test detector initializes correctly"""
        assert detector is not None
        assert detector.config is not None
        assert detector.device is not None

    @pytest.mark.asyncio
    async def test_healthy_data_passes(self, detector, healthy_data, original_data):
        """Test that healthy data is not flagged as collapsed"""
        result = await detector.detect_collapse(
            synthetic_data=healthy_data,
            original_data=original_data
        )
        
        assert result is not None
        assert hasattr(result, 'overall_score')
        assert hasattr(result, 'collapse_detected')
        
        # Healthy data should have good score
        assert result.overall_score > 50.0, "Healthy data should score above 50"
        assert not result.collapse_detected, "Healthy data should not be marked as collapsed"

    @pytest.mark.asyncio
    async def test_collapsed_data_fails(self, detector, collapsed_data, original_data):
        """Test that collapsed data is properly detected"""
        result = await detector.detect_collapse(
            synthetic_data=collapsed_data,
            original_data=original_data
        )
        
        assert result is not None
        # Collapsed data should have lower score
        assert result.overall_score < 80.0, "Collapsed data should have reduced score"

    @pytest.mark.asyncio
    async def test_dimension_scores_present(self, detector, healthy_data, original_data):
        """Test that all dimensions are computed"""
        result = await detector.detect_collapse(
            synthetic_data=healthy_data,
            original_data=original_data
        )
        
        assert hasattr(result, 'dimensions')
        assert len(result.dimensions) > 0, "Should have dimension scores"
        
        # Check that dimension scores are valid
        for dim_name, dim_score in result.dimensions.items():
            assert hasattr(dim_score, 'score') or isinstance(dim_score, (int, float))
            score_val = dim_score.score if hasattr(dim_score, 'score') else dim_score
            assert 0 <= score_val <= 100, f"Score {dim_name} should be 0-100"

    @pytest.mark.asyncio
    async def test_empty_data_handling(self, detector):
        """Test handling of empty data"""
        empty_data = np.array([]).reshape(0, 10).astype(np.float32)
        original_data = np.random.randn(100, 10).astype(np.float32)
        
        with pytest.raises(Exception):
            await detector.detect_collapse(
                synthetic_data=empty_data,
                original_data=original_data
            )

    @pytest.mark.asyncio
    async def test_mismatched_dimensions(self, detector):
        """Test handling of mismatched feature dimensions"""
        synthetic = np.random.randn(100, 10).astype(np.float32)
        original = np.random.randn(100, 20).astype(np.float32)
        
        # Should handle gracefully (trim to common dimensions)
        result = await detector.detect_collapse(
            synthetic_data=synthetic,
            original_data=original
        )
        assert result is not None

    @pytest.mark.asyncio
    async def test_constant_data_detection(self, detector):
        """Test detection of constant (zero variance) data"""
        constant_data = np.ones((100, 10), dtype=np.float32)
        original_data = np.random.randn(100, 10).astype(np.float32)
        
        result = await detector.detect_collapse(
            synthetic_data=constant_data,
            original_data=original_data
        )
        
        # Constant data should be flagged
        assert result.overall_score < 50.0, "Constant data should have low score"

    @pytest.mark.asyncio
    async def test_nan_handling(self, detector):
        """Test handling of NaN values"""
        data_with_nan = np.random.randn(100, 10).astype(np.float32)
        data_with_nan[0, 0] = np.nan
        original_data = np.random.randn(100, 10).astype(np.float32)
        
        # Should handle NaN gracefully
        result = await detector.detect_collapse(
            synthetic_data=data_with_nan,
            original_data=original_data
        )
        assert result is not None

    def test_config_customization(self):
        """Test custom configuration"""
        custom_config = CollapseConfig(
            distribution_fidelity_threshold=80.0,
            use_gpu=False,
            batch_size=5000
        )
        detector = CollapseDetector(custom_config)
        
        assert detector.config.distribution_fidelity_threshold == 80.0
        assert detector.config.batch_size == 5000

    @pytest.mark.asyncio
    async def test_deterministic_results(self, detector, healthy_data, original_data):
        """Test that same input gives same output (determinism)"""
        result1 = await detector.detect_collapse(
            synthetic_data=healthy_data.copy(),
            original_data=original_data.copy()
        )
        
        result2 = await detector.detect_collapse(
            synthetic_data=healthy_data.copy(),
            original_data=original_data.copy()
        )
        
        # Results should be identical
        assert abs(result1.overall_score - result2.overall_score) < 1e-5


class TestDimensionScore:
    """Test DimensionScore dataclass"""

    def test_dimension_score_creation(self):
        """Test creating a dimension score"""
        score = DimensionScore(
            name="test_dimension",
            score=85.5,
            threshold=70.0,
            passed=True,
            metrics={"mean": 0.5, "std": 0.1},
            severity="ok"
        )
        
        assert score.name == "test_dimension"
        assert score.score == 85.5
        assert score.passed is True
        assert score.severity == "ok"

    def test_dimension_score_validation(self):
        """Test dimension score thresholds"""
        passing_score = DimensionScore(
            name="test",
            score=75.0,
            threshold=70.0,
            passed=True,
            metrics={},
            severity="ok"
        )
        
        failing_score = DimensionScore(
            name="test",
            score=65.0,
            threshold=70.0,
            passed=False,
            metrics={},
            severity="warning"
        )
        
        assert passing_score.passed is True
        assert failing_score.passed is False


class TestCollapseConfig:
    """Test CollapseConfig dataclass"""

    def test_default_config(self):
        """Test default configuration values"""
        config = CollapseConfig()
        
        assert config.distribution_fidelity_threshold == 70.0
        assert config.overall_threshold == 65.0
        assert config.use_gpu is True
        assert config.batch_size == 10000

    def test_custom_config(self):
        """Test custom configuration"""
        config = CollapseConfig(
            distribution_fidelity_threshold=80.0,
            overall_threshold=75.0,
            batch_size=5000
        )
        
        assert config.distribution_fidelity_threshold == 80.0
        assert config.overall_threshold == 75.0
        assert config.batch_size == 5000
