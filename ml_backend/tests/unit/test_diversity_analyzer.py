"""
Real Unit Tests for DiversityAnalyzer
======================================

Tests the actual diversity analysis logic with real data.
"""

import pytest
import numpy as np
import pandas as pd
import tempfile
import os
from pathlib import Path
from src.validation_engine.diversity_analyzer import DiversityAnalyzer, StratificationConfig


@pytest.fixture
def analyzer():
    """Create a DiversityAnalyzer instance for testing"""
    config = StratificationConfig(
        target_sample_size=1000,
        use_gpu=False,  # Force CPU for CI/CD
        parallel_workers=2
    )
    return DiversityAnalyzer(config)


@pytest.fixture
def diverse_dataset():
    """Generate a diverse dataset"""
    np.random.seed(42)
    return pd.DataFrame({
        'feature_1': np.random.randn(5000),
        'feature_2': np.random.uniform(0, 100, 5000),
        'feature_3': np.random.exponential(2, 5000),
        'feature_4': np.random.choice(['A', 'B', 'C', 'D'], 5000),
        'feature_5': np.random.randint(0, 1000, 5000)
    })


@pytest.fixture
def skewed_dataset():
    """Generate a skewed dataset (low diversity)"""
    np.random.seed(42)
    # Most values concentrated in narrow range
    return pd.DataFrame({
        'feature_1': np.random.randn(5000) * 0.1,  # Low variance
        'feature_2': np.full(5000, 50.0),  # Constant
        'feature_3': np.random.choice([1.0, 2.0], 5000),  # Only 2 values
        'feature_4': np.random.choice(['A', 'A', 'A', 'B'], 5000),  # Highly imbalanced
        'feature_5': np.random.randint(490, 510, 5000)  # Narrow range
    })


@pytest.fixture
def temp_csv_file(diverse_dataset):
    """Create a temporary CSV file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        diverse_dataset.to_csv(f.name, index=False)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_parquet_file(diverse_dataset):
    """Create a temporary Parquet file for testing"""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        diverse_dataset.to_parquet(f.name, index=False)
        yield f.name
    os.unlink(f.name)


class TestDiversityAnalyzer:
    """Test suite for DiversityAnalyzer"""

    def test_initialization(self, analyzer):
        """Test analyzer initializes correctly"""
        assert analyzer is not None
        assert analyzer.config is not None
        assert analyzer.device is not None

    @pytest.mark.asyncio
    async def test_diverse_data_high_score(self, analyzer, temp_csv_file):
        """Test that diverse data gets high diversity score"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert result is not None
        assert hasattr(result, 'overall_score')
        assert result.overall_score > 30.0, "Diverse data should have decent score"

    @pytest.mark.asyncio
    async def test_skewed_data_low_score(self, analyzer, skewed_dataset):
        """Test that skewed data gets lower diversity score"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            skewed_dataset.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            result = await analyzer.analyze_diversity(
                data_path=temp_file,
                data_format='csv',
                streaming=False
            )
            
            # Skewed data should have lower score than diverse data
            assert result.overall_score < 80.0, "Skewed data should have lower score"
        finally:
            os.unlink(temp_file)

    @pytest.mark.asyncio
    async def test_dimension_scores_present(self, analyzer, temp_csv_file):
        """Test that dimension scores are computed"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'dimension_scores')
        assert len(result.dimension_scores) > 0, "Should have dimension scores"

    @pytest.mark.asyncio
    async def test_parquet_format(self, analyzer, temp_parquet_file):
        """Test loading Parquet format"""
        result = await analyzer.analyze_diversity(
            data_path=temp_parquet_file,
            data_format='parquet',
            streaming=False
        )
        
        assert result is not None
        assert result.overall_score >= 0

    @pytest.mark.asyncio
    async def test_skew_detection(self, analyzer, temp_csv_file):
        """Test skewness detection"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'skew_factors')
        assert isinstance(result.skew_factors, dict)

    @pytest.mark.asyncio
    async def test_outlier_detection(self, analyzer, temp_csv_file):
        """Test outlier detection"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'outlier_percentages')
        assert isinstance(result.outlier_percentages, dict)

    @pytest.mark.asyncio
    async def test_correlation_matrix(self, analyzer, temp_csv_file):
        """Test correlation matrix computation"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'correlation_matrix')
        assert result.correlation_matrix is not None

    @pytest.mark.asyncio
    async def test_recommendations(self, analyzer, temp_csv_file):
        """Test that recommendations are generated"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'recommendations')
        assert isinstance(result.recommendations, list)

    @pytest.mark.asyncio
    async def test_sample_quality_metric(self, analyzer, temp_csv_file):
        """Test sample quality metric"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            streaming=False
        )
        
        assert hasattr(result, 'sample_quality')
        assert 0 <= result.sample_quality <= 100

    @pytest.mark.asyncio
    async def test_target_columns_filter(self, analyzer, temp_csv_file):
        """Test filtering specific columns"""
        result = await analyzer.analyze_diversity(
            data_path=temp_csv_file,
            data_format='csv',
            target_columns=['feature_1', 'feature_2'],
            streaming=False
        )
        
        assert result is not None

    @pytest.mark.asyncio
    async def test_empty_file_handling(self, analyzer):
        """Test handling of empty file"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1,col2\n")  # Header only
            temp_file = f.name
        
        try:
            with pytest.raises(Exception):
                await analyzer.analyze_diversity(
                    data_path=temp_file,
                    data_format='csv',
                    streaming=False
                )
        finally:
            os.unlink(temp_file)

    def test_config_customization(self):
        """Test custom configuration"""
        custom_config = StratificationConfig(
            target_sample_size=5000,
            min_bin_size=50,
            parallel_workers=8,
            use_gpu=False
        )
        analyzer = DiversityAnalyzer(custom_config)
        
        assert analyzer.config.target_sample_size == 5000
        assert analyzer.config.min_bin_size == 50
        assert analyzer.config.parallel_workers == 8


class TestStratificationConfig:
    """Test StratificationConfig dataclass"""

    def test_default_config(self):
        """Test default configuration values"""
        config = StratificationConfig()
        
        assert config.target_sample_size == 1_000_000
        assert config.min_bin_size == 100
        assert config.parallel_workers == 16
        assert config.use_gpu is True

    def test_custom_config(self):
        """Test custom configuration"""
        config = StratificationConfig(
            target_sample_size=500_000,
            max_bins_per_dimension=25,
            memory_limit_gb=16.0
        )
        
        assert config.target_sample_size == 500_000
        assert config.max_bins_per_dimension == 25
        assert config.memory_limit_gb == 16.0
