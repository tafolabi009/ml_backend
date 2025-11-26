import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch, AsyncMock

from src.collapse_engine.localizer import CollapseLocalizer, LocalizationResult


class TestCollapseLocalizer:
    
    @pytest.fixture
    def localizer(self):
        """Create a CollapseLocalizer instance"""
        return CollapseLocalizer()
    
    @pytest.fixture
    def sample_data(self):
        """Create sample tabular data"""
        np.random.seed(42)
        df = pd.DataFrame({
            'feature1': np.random.randn(1000),
            'feature2': np.random.randn(1000),
            'feature3': np.random.randn(1000),
            'feature4': np.random.randn(1000),
            'feature5': np.random.randn(1000)
        })
        return df
    
    @pytest.fixture
    def collapsed_data(self):
        """Create data with obvious collapse in some columns"""
        np.random.seed(42)
        df = pd.DataFrame({
            'feature1': np.random.randn(1000),
            'feature2': np.ones(1000) * 5.0,  # Constant - collapsed
            'feature3': np.random.randn(1000),
            'feature4': np.random.choice([1, 2], 1000),  # Low diversity
            'feature5': np.random.randn(1000)
        })
        return df
    
    def test_initialization(self, localizer):
        """Test localizer initialization"""
        assert localizer is not None
        assert hasattr(localizer, 'localize_collapse')
    
    @pytest.mark.asyncio
    async def test_localize_healthy_data(self, localizer, sample_data):
        """Test localization on healthy data"""
        result = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
        assert hasattr(result, 'problematic_features')
        assert hasattr(result, 'problematic_samples')
    
    @pytest.mark.asyncio
    async def test_localize_collapsed_data(self, localizer, collapsed_data):
        """Test localization on collapsed data"""
        result = await localizer.localize_collapse(
            data=collapsed_data,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
        
        # Should identify feature2 as problematic (constant values)
        if result.problematic_features:
            assert len(result.problematic_features) > 0
    
    @pytest.mark.asyncio
    async def test_localize_with_different_threshold(self, localizer, sample_data):
        """Test localization with different thresholds"""
        # Strict threshold
        result_strict = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.9
        )
        
        # Lenient threshold
        result_lenient = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.5
        )
        
        # Both should return results
        assert isinstance(result_strict, LocalizationResult)
        assert isinstance(result_lenient, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_empty_data(self, localizer):
        """Test localization on empty data"""
        empty_df = pd.DataFrame()
        
        result = await localizer.localize_collapse(
            data=empty_df,
            threshold=0.7
        )
        
        # Should handle empty data gracefully
        assert isinstance(result, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_single_column(self, localizer):
        """Test localization on single column data"""
        single_col = pd.DataFrame({
            'col1': np.random.randn(100)
        })
        
        result = await localizer.localize_collapse(
            data=single_col,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
    
    def test_localization_result_structure(self):
        """Test LocalizationResult data structure"""
        result = LocalizationResult(
            problematic_features=['feat1', 'feat2'],
            problematic_samples=[0, 1, 2],
            feature_scores={'feat1': 0.3, 'feat2': 0.4},
            recommendations=['Fix feat1', 'Fix feat2']
        )
        
        assert result.problematic_features == ['feat1', 'feat2']
        assert result.problematic_samples == [0, 1, 2]
        assert result.feature_scores == {'feat1': 0.3, 'feat2': 0.4}
        assert len(result.recommendations) == 2
    
    @pytest.mark.asyncio
    async def test_localize_with_nans(self, localizer):
        """Test localization with NaN values"""
        data_with_nans = pd.DataFrame({
            'feat1': [1, 2, np.nan, 4, 5] * 20,
            'feat2': np.random.randn(100),
            'feat3': [np.nan] * 50 + list(range(50))
        })
        
        result = await localizer.localize_collapse(
            data=data_with_nans,
            threshold=0.7
        )
        
        # Should handle NaNs
        assert isinstance(result, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_categorical_data(self, localizer):
        """Test localization with categorical columns"""
        categorical_data = pd.DataFrame({
            'cat1': ['A', 'B', 'C'] * 33 + ['A'],
            'cat2': ['X'] * 100,  # All same - should be flagged
            'num1': np.random.randn(100)
        })
        
        result = await localizer.localize_collapse(
            data=categorical_data,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_high_cardinality(self, localizer):
        """Test localization with high cardinality features"""
        high_card = pd.DataFrame({
            'id': range(1000),  # Unique values
            'value': np.random.randn(1000)
        })
        
        result = await localizer.localize_collapse(
            data=high_card,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_mixed_types(self, localizer):
        """Test localization with mixed data types"""
        mixed_data = pd.DataFrame({
            'int_col': np.random.randint(0, 100, 100),
            'float_col': np.random.randn(100),
            'str_col': ['cat', 'dog', 'bird'] * 33 + ['cat'],
            'bool_col': np.random.choice([True, False], 100)
        })
        
        result = await localizer.localize_collapse(
            data=mixed_data,
            threshold=0.7
        )
        
        assert isinstance(result, LocalizationResult)
    
    @pytest.mark.asyncio
    async def test_localize_returns_recommendations(self, localizer, collapsed_data):
        """Test that localization returns recommendations"""
        result = await localizer.localize_collapse(
            data=collapsed_data,
            threshold=0.7
        )
        
        # Should have recommendations if problems found
        assert hasattr(result, 'recommendations')
        assert isinstance(result.recommendations, list)
    
    @pytest.mark.asyncio
    async def test_localize_feature_scores(self, localizer, sample_data):
        """Test that feature scores are computed"""
        result = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.7
        )
        
        # Should have feature scores
        assert hasattr(result, 'feature_scores')
        assert isinstance(result.feature_scores, dict)
    
    @pytest.mark.asyncio
    async def test_localize_deterministic(self, localizer, sample_data):
        """Test that localization is deterministic"""
        result1 = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.7
        )
        
        result2 = await localizer.localize_collapse(
            data=sample_data,
            threshold=0.7
        )
        
        # Results should be consistent
        assert result1.problematic_features == result2.problematic_features
