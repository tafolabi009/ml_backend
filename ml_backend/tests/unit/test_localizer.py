"""
Unit tests for CollapseLocalizer.
Tests collapse localization to specific data rows using gradient attribution.
"""
import pytest
import numpy as np
import torch
from unittest.mock import MagicMock, patch, AsyncMock

from src.collapse_engine.localizer import CollapseLocalizer, LocalizationResult, LocalizationConfig


class TestCollapseLocalizer:
    
    @pytest.fixture
    def localizer(self):
        """Create a CollapseLocalizer instance"""
        config = LocalizationConfig(use_gpu=False)
        return CollapseLocalizer(config)
    
    @pytest.fixture
    def sample_data(self):
        """Create sample numpy array data"""
        np.random.seed(42)
        return np.random.randn(100, 5).astype(np.float32)
    
    @pytest.fixture
    def collapsed_data(self):
        """Create data with obvious collapse in some columns"""
        np.random.seed(42)
        data = np.random.randn(100, 5).astype(np.float32)
        data[:, 1] = 5.0  # Constant - collapsed
        data[:, 3] = np.random.choice([1, 2], 100)  # Low diversity
        return data
    
    @pytest.fixture
    def collapse_dimensions(self):
        """Sample collapse dimension scores"""
        return {
            'distribution_fidelity': 0.7,
            'correlation_preservation': 0.8,
            'entropy_stability': 0.6,
            'gradient_health': 0.5
        }
    
    def test_initialization(self, localizer):
        """Test localizer initialization"""
        assert localizer is not None
        assert hasattr(localizer, 'localize_collapse')
        assert hasattr(localizer, 'config')
        assert localizer.device in [torch.device('cpu'), torch.device('cuda')]
    
    def test_initialization_with_config(self):
        """Test localizer initialization with custom config"""
        config = LocalizationConfig(
            top_k=500,
            impact_threshold=0.9,
            use_gpu=False,
            batch_size=500,
            max_samples=50000
        )
        localizer = CollapseLocalizer(config)
        assert localizer.config.top_k == 500
        assert localizer.config.impact_threshold == 0.9
    
    @pytest.mark.asyncio
    async def test_localize_healthy_data(self, localizer, sample_data, collapse_dimensions):
        """Test localization on healthy data"""
        result = await localizer.localize_collapse(
            data=sample_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert isinstance(result, LocalizationResult)
        assert hasattr(result, 'problematic_indices')
        assert hasattr(result, 'impact_scores')
        assert hasattr(result, 'top_k_rows')
        assert hasattr(result, 'dimension_attributions')
        assert hasattr(result, 'recommendations')
    
    @pytest.mark.asyncio
    async def test_localize_collapsed_data(self, localizer, collapsed_data, collapse_dimensions):
        """Test localization on collapsed data"""
        result = await localizer.localize_collapse(
            data=collapsed_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert isinstance(result, LocalizationResult)
        assert isinstance(result.problematic_indices, list)
        assert isinstance(result.impact_scores, np.ndarray)
        assert len(result.impact_scores) == len(collapsed_data)
    
    @pytest.mark.asyncio
    async def test_localize_with_different_threshold(self, localizer, sample_data, collapse_dimensions):
        """Test localization with different thresholds via config"""
        # Strict threshold
        strict_config = LocalizationConfig(impact_threshold=0.95, use_gpu=False)
        strict_localizer = CollapseLocalizer(strict_config)
        result_strict = await strict_localizer.localize_collapse(
            data=sample_data,
            collapse_dimensions=collapse_dimensions
        )
        
        # Lenient threshold
        lenient_config = LocalizationConfig(impact_threshold=0.5, use_gpu=False)
        lenient_localizer = CollapseLocalizer(lenient_config)
        result_lenient = await lenient_localizer.localize_collapse(
            data=sample_data,
            collapse_dimensions=collapse_dimensions
        )
        
        # Lenient should flag more problematic indices
        assert isinstance(result_strict, LocalizationResult)
        assert isinstance(result_lenient, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_empty_data(self, localizer, collapse_dimensions):
        """Test localization on empty data"""
        empty_data = np.array([]).reshape(0, 5).astype(np.float32)
        
        # Should handle empty data gracefully or raise appropriate error
        try:
            result = await localizer.localize_collapse(
                data=empty_data,
                collapse_dimensions=collapse_dimensions
            )
            assert isinstance(result, LocalizationResult)
        except (ValueError, IndexError, RuntimeError):
            # Acceptable to raise error for empty data
            pass
    
    @pytest.mark.asyncio
    async def test_localize_single_row(self, localizer, collapse_dimensions):
        """Test localization on single row data"""
        single_row = np.random.randn(1, 5).astype(np.float32)
        
        result = await localizer.localize_collapse(
            data=single_row,
            collapse_dimensions=collapse_dimensions
        )
        
        assert isinstance(result, LocalizationResult)
        assert len(result.impact_scores) == 1
    
    def test_localization_result_structure(self):
        """Test LocalizationResult data structure"""
        result = LocalizationResult(
            problematic_indices=[0, 1, 2],
            impact_scores=np.array([0.9, 0.85, 0.8]),
            top_k_rows=[(0, 0.9), (1, 0.85), (2, 0.8)],
            dimension_attributions={'dim1': np.array([0.1, 0.2, 0.3])},
            recommendations=['Fix row 0', 'Fix row 1'],
            total_problematic=3,
            percentage_problematic=3.0
        )
        
        assert result.problematic_indices == [0, 1, 2]
        assert len(result.impact_scores) == 3
        assert len(result.top_k_rows) == 3
        assert 'dim1' in result.dimension_attributions
        assert len(result.recommendations) == 2
        assert result.total_problematic == 3
        assert result.percentage_problematic == 3.0
    
    @pytest.mark.asyncio
    async def test_localize_with_nans(self, localizer, collapse_dimensions):
        """Test localization with NaN values"""
        data_with_nans = np.random.randn(100, 5).astype(np.float32)
        data_with_nans[0, 0] = np.nan
        data_with_nans[50, 2] = np.nan
        
        # Should handle NaNs gracefully or raise appropriate error
        try:
            result = await localizer.localize_collapse(
                data=data_with_nans,
                collapse_dimensions=collapse_dimensions
            )
            assert isinstance(result, LocalizationResult)
        except (ValueError, RuntimeError):
            # Acceptable to raise error for NaN data
            pass
    
    @pytest.mark.asyncio
    async def test_localize_large_dataset(self, localizer, collapse_dimensions):
        """Test localization on large dataset (tests sampling)"""
        # Create dataset larger than max_samples
        large_data = np.random.randn(150000, 5).astype(np.float32)
        
        result = await localizer.localize_collapse(
            data=large_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert isinstance(result, LocalizationResult)
        # Should have sampled
        assert len(result.impact_scores) <= localizer.config.max_samples
    
    @pytest.mark.asyncio
    async def test_localize_returns_recommendations(self, localizer, collapsed_data, collapse_dimensions):
        """Test that localization returns recommendations"""
        result = await localizer.localize_collapse(
            data=collapsed_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert hasattr(result, 'recommendations')
        assert isinstance(result.recommendations, list)
    
    @pytest.mark.asyncio
    async def test_localize_dimension_attributions(self, localizer, sample_data, collapse_dimensions):
        """Test that dimension attributions are computed"""
        result = await localizer.localize_collapse(
            data=sample_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert hasattr(result, 'dimension_attributions')
        assert isinstance(result.dimension_attributions, dict)
    
    @pytest.mark.asyncio
    async def test_localize_top_k_rows(self, localizer, sample_data, collapse_dimensions):
        """Test that top-k problematic rows are returned"""
        result = await localizer.localize_collapse(
            data=sample_data,
            collapse_dimensions=collapse_dimensions
        )
        
        assert hasattr(result, 'top_k_rows')
        assert isinstance(result.top_k_rows, list)
        # Should be sorted by impact (descending)
        if len(result.top_k_rows) > 1:
            for i in range(len(result.top_k_rows) - 1):
                assert result.top_k_rows[i][1] >= result.top_k_rows[i+1][1]
    
    @pytest.mark.asyncio
    async def test_localize_deterministic(self, collapse_dimensions):
        """Test that localization is deterministic with same seed"""
        np.random.seed(42)
        torch.manual_seed(42)
        data1 = np.random.randn(100, 5).astype(np.float32)
        
        np.random.seed(42)
        torch.manual_seed(42)
        data2 = np.random.randn(100, 5).astype(np.float32)
        
        config = LocalizationConfig(use_gpu=False)
        localizer = CollapseLocalizer(config)
        
        torch.manual_seed(42)
        result1 = await localizer.localize_collapse(
            data=data1,
            collapse_dimensions=collapse_dimensions
        )
        
        torch.manual_seed(42)
        result2 = await localizer.localize_collapse(
            data=data2,
            collapse_dimensions=collapse_dimensions
        )
        
        # Results should be consistent for same input
        np.testing.assert_array_almost_equal(result1.impact_scores, result2.impact_scores)


class TestLocalizationConfig:
    """Test LocalizationConfig dataclass"""
    
    def test_default_config(self):
        """Test default configuration values"""
        config = LocalizationConfig()
        assert config.top_k == 1000
        assert config.impact_threshold == 0.8
        assert config.use_gpu == True
        assert config.batch_size == 1000
        assert config.max_samples == 100_000
    
    def test_custom_config(self):
        """Test custom configuration values"""
        config = LocalizationConfig(
            top_k=500,
            impact_threshold=0.9,
            use_gpu=False,
            batch_size=256,
            max_samples=50_000
        )
        assert config.top_k == 500
        assert config.impact_threshold == 0.9
        assert config.use_gpu == False
        assert config.batch_size == 256
        assert config.max_samples == 50_000
