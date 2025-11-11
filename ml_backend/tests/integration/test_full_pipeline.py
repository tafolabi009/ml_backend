"""
Real Integration Tests for Full Pipeline
=========================================

Tests the complete orchestrator pipeline end-to-end with real data.
"""

import pytest
import numpy as np
import pandas as pd
import tempfile
import os
import asyncio
from pathlib import Path
from src.orchestrator import SynthosOrchestrator


@pytest.fixture
def small_test_dataset():
    """Generate a small realistic dataset for testing"""
    np.random.seed(42)
    return pd.DataFrame({
        'user_id': range(1000),
        'age': np.random.randint(18, 80, 1000),
        'income': np.random.lognormal(10, 1, 1000),
        'spending': np.random.gamma(2, 1000, 1000),
        'category': np.random.choice(['A', 'B', 'C', 'D'], 1000),
        'score': np.random.randn(1000) * 10 + 50,
    })


@pytest.fixture
def temp_dataset_file(small_test_dataset):
    """Create temporary dataset file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        small_test_dataset.to_csv(f.name, index=False)
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def orchestrator():
    """Create orchestrator instance for testing"""
    return SynthosOrchestrator(
        gpu_memory_fraction=0.1,
        enable_mixed_precision=False,
        collapse_threshold=50.0,  # Lenient for testing
        diversity_threshold=30.0,  # Lenient for testing
        use_cache=False,
        skip_cascade_training=True  # Skip slow cascade training in tests
    )


class TestFullPipeline:
    """Integration tests for complete pipeline"""

    @pytest.mark.asyncio
    @pytest.mark.timeout(120)  # 2 minute timeout
    async def test_complete_pipeline_execution(self, orchestrator, temp_dataset_file):
        """Test that complete pipeline executes without errors"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            output_report_path=None,
            stream_progress=False
        )
        
        assert result is not None
        assert hasattr(result, 'validation_id')
        assert hasattr(result, 'dataset_id')
        assert hasattr(result, 'status')
        assert result.status == 'completed'

    @pytest.mark.asyncio
    async def test_all_stages_complete(self, orchestrator, temp_dataset_file):
        """Test that all pipeline stages complete"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        # Check all stages completed
        assert result.data_loaded is True
        assert result.load_time_seconds > 0
        assert result.diversity_score >= 0
        assert result.diversity_time_seconds > 0
        assert result.collapse_score >= 0
        assert result.collapse_time_seconds > 0

    @pytest.mark.asyncio
    async def test_result_structure(self, orchestrator, temp_dataset_file):
        """Test that result has correct structure"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        # Check required fields
        assert hasattr(result, 'validation_id')
        assert hasattr(result, 'dataset_id')
        assert hasattr(result, 'approved_for_training')
        assert hasattr(result, 'confidence')
        assert hasattr(result, 'reason')
        assert hasattr(result, 'total_time_seconds')
        assert hasattr(result, 'recommendations')

    @pytest.mark.asyncio
    async def test_to_dict_conversion(self, orchestrator, temp_dataset_file):
        """Test conversion to dictionary format"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        result_dict = result.to_dict()
        
        # Check API-compliant structure
        assert 'validation_id' in result_dict
        assert 'dataset_id' in result_dict
        assert 'status' in result_dict
        assert 'results' in result_dict
        assert 'internal' in result_dict
        
        # Check results section
        assert 'risk_score' in result_dict['results']
        assert 'risk_level' in result_dict['results']
        assert 'predicted_performance' in result_dict['results']
        assert 'dimensions' in result_dict['results']

    @pytest.mark.asyncio
    async def test_report_saving(self, orchestrator, temp_dataset_file):
        """Test saving validation report to file"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as f:
            report_path = f.name
        
        try:
            result = await orchestrator.validate(
                dataset_path=temp_dataset_file,
                dataset_format='csv',
                output_report_path=report_path,
                stream_progress=False
            )
            
            # Check file was created
            assert os.path.exists(report_path)
            assert os.path.getsize(report_path) > 0
            
            # Check it's valid JSON
            import json
            with open(report_path, 'r') as f:
                report_data = json.load(f)
            
            assert 'validation_id' in report_data
            assert 'results' in report_data
        finally:
            if os.path.exists(report_path):
                os.unlink(report_path)

    @pytest.mark.asyncio
    async def test_custom_validation_id(self, orchestrator, temp_dataset_file):
        """Test using custom validation and dataset IDs"""
        custom_val_id = "test_val_12345"
        custom_ds_id = "test_ds_67890"
        
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            validation_id=custom_val_id,
            dataset_id=custom_ds_id,
            stream_progress=False
        )
        
        assert result.validation_id == custom_val_id
        assert result.dataset_id == custom_ds_id

    @pytest.mark.asyncio
    async def test_error_handling_invalid_path(self, orchestrator):
        """Test error handling for invalid file path"""
        with pytest.raises(Exception):
            await orchestrator.validate(
                dataset_path="/nonexistent/path/data.csv",
                dataset_format='csv',
                stream_progress=False
            )

    @pytest.mark.asyncio
    async def test_error_handling_invalid_format(self, orchestrator, temp_dataset_file):
        """Test error handling for invalid format"""
        with pytest.raises(Exception):
            await orchestrator.validate(
                dataset_path=temp_dataset_file,
                dataset_format='invalid_format',
                stream_progress=False
            )

    @pytest.mark.asyncio
    async def test_timing_metrics(self, orchestrator, temp_dataset_file):
        """Test that timing metrics are captured"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        # All stage times should be positive
        assert result.load_time_seconds > 0
        assert result.diversity_time_seconds >= 0
        assert result.collapse_time_seconds >= 0
        assert result.total_time_seconds > 0
        
        # Total time should be sum of stages (approximately)
        estimated_total = (
            result.load_time_seconds +
            result.diversity_time_seconds +
            result.cascade_time_seconds +
            result.collapse_time_seconds +
            result.localization_time_seconds +
            result.recommendation_time_seconds
        )
        assert abs(result.total_time_seconds - estimated_total) < 1.0

    @pytest.mark.asyncio
    async def test_recommendation_generation(self, orchestrator, temp_dataset_file):
        """Test that recommendations are generated"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        assert hasattr(result, 'recommendations')
        assert isinstance(result.recommendations, list)

    @pytest.mark.asyncio
    async def test_dimension_scores(self, orchestrator, temp_dataset_file):
        """Test that dimension scores are computed"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        assert hasattr(result, 'dimension_scores')
        assert len(result.dimension_scores) > 0

    @pytest.mark.asyncio
    async def test_approval_decision_logic(self, orchestrator, temp_dataset_file):
        """Test approval decision logic"""
        result = await orchestrator.validate(
            dataset_path=temp_dataset_file,
            dataset_format='csv',
            stream_progress=False
        )
        
        # Should have a clear approval decision
        assert isinstance(result.approved_for_training, bool)
        assert 0 <= result.confidence <= 100
        assert len(result.reason) > 0


class TestPipelineRobustness:
    """Test pipeline robustness and edge cases"""

    @pytest.mark.asyncio
    async def test_small_dataset(self, orchestrator):
        """Test with very small dataset"""
        tiny_data = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': [4, 5, 6]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            tiny_data.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            result = await orchestrator.validate(
                dataset_path=temp_file,
                dataset_format='csv',
                stream_progress=False
            )
            assert result is not None
        finally:
            os.unlink(temp_file)

    @pytest.mark.asyncio
    async def test_missing_values(self, orchestrator):
        """Test dataset with missing values"""
        data_with_nan = pd.DataFrame({
            'col1': [1, 2, np.nan, 4, 5],
            'col2': [np.nan, 2, 3, 4, 5],
            'col3': [1, 2, 3, 4, 5]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            data_with_nan.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            result = await orchestrator.validate(
                dataset_path=temp_file,
                dataset_format='csv',
                stream_progress=False
            )
            assert result is not None
        finally:
            os.unlink(temp_file)

    @pytest.mark.asyncio
    async def test_mixed_datatypes(self, orchestrator):
        """Test dataset with mixed data types"""
        mixed_data = pd.DataFrame({
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'str_col': ['a', 'b', 'c', 'd', 'e'],
            'bool_col': [True, False, True, False, True]
        })
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            mixed_data.to_csv(f.name, index=False)
            temp_file = f.name
        
        try:
            result = await orchestrator.validate(
                dataset_path=temp_file,
                dataset_format='csv',
                stream_progress=False
            )
            assert result is not None
        finally:
            os.unlink(temp_file)
