import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from datetime import datetime
import pandas as pd
import numpy as np
import torch

from src.orchestrator import SynthosOrchestrator, ValidationResult
from src.validation_engine.diversity_analyzer import DiversityScore
from src.collapse_engine.detector import CollapseScore, DimensionScore

@pytest.fixture
def mock_dataset_loader():
    loader = MagicMock()
    loader.load_dataset = AsyncMock(return_value=pd.DataFrame({
        'col1': np.random.randn(100),
        'col2': np.random.randn(100)
    }))
    return loader

@pytest.fixture
def mock_diversity_analyzer():
    analyzer = MagicMock()
    analyzer.analyze_diversity = AsyncMock(return_value=DiversityScore(
        overall_score=80.0,
        dimension_scores={'dim1': 80.0},
        skew_factors={},
        outlier_percentages={},
        correlation_matrix=np.eye(2),
        recommendations=[],
        sample_quality=90.0
    ))
    return analyzer

@pytest.fixture
def mock_collapse_detector():
    detector = MagicMock()
    detector.detect_collapse = AsyncMock(return_value=CollapseScore(
        overall_score=85.0,
        collapse_detected=False,
        confidence=90.0,
        dimensions={'dim1': DimensionScore(name='dim1', score=85.0, threshold=60.0, passed=True, metrics={}, severity='ok')},
        warnings=[],
        predictions={}
    ))
    return detector

@pytest.fixture
def mock_localizer():
    localizer = MagicMock()
    localizer.localize_collapse = AsyncMock(return_value={
        'problematic_indices': [],
        'top_contributors': [],
        'severity': 'low'
    })
    return localizer

@pytest.fixture
def mock_recommender():
    recommender = MagicMock()
    recommender.generate_recommendations = AsyncMock(return_value=MagicMock(
        recommendations=[],
        projected_improvement=0.0,
        projected_score=85.0
    ))
    return recommender

@pytest.fixture
def orchestrator(mock_dataset_loader, mock_diversity_analyzer, mock_collapse_detector, mock_localizer, mock_recommender):
    with patch('src.orchestrator.DatasetLoader', return_value=mock_dataset_loader), \
         patch('src.orchestrator.DiversityAnalyzer', return_value=mock_diversity_analyzer), \
         patch('src.orchestrator.CollapseDetector', return_value=mock_collapse_detector), \
         patch('src.orchestrator.CollapseLocalizer', return_value=mock_localizer), \
         patch('src.orchestrator.RecommendationEngine', return_value=mock_recommender), \
         patch('src.orchestrator.GPUOptimizer'), \
         patch('src.orchestrator.SignatureLibrary'):
        
        orch = SynthosOrchestrator(skip_cascade_training=True)
        return orch

@pytest.mark.asyncio
async def test_validate_success(orchestrator):
    result = await orchestrator.validate(
        dataset_path="dummy.csv",
        dataset_format="csv",
        stream_progress=False
    )
    
    assert isinstance(result, ValidationResult)
    assert result.status == 'completed'
    assert result.approved_for_training is True
    assert result.collapse_score == 85.0
    assert result.diversity_score == 80.0

@pytest.mark.asyncio
async def test_validate_failure(orchestrator, mock_collapse_detector):
    # Mock collapse detection failure
    mock_collapse_detector.detect_collapse = AsyncMock(return_value=CollapseScore(
        overall_score=40.0,
        collapse_detected=True,
        confidence=90.0,
        dimensions={'dim1': DimensionScore(name='dim1', score=40.0, threshold=60.0, passed=False, metrics={}, severity='critical')},
        warnings=['Collapse detected'],
        predictions={}
    ))
    
    result = await orchestrator.validate(
        dataset_path="dummy.csv",
        dataset_format="csv",
        stream_progress=False
    )
    
    assert isinstance(result, ValidationResult)
    assert result.approved_for_training is False
    assert result.collapse_score == 40.0

@pytest.mark.asyncio
async def test_validate_with_reference(orchestrator, mock_dataset_loader):
    await orchestrator.validate(
        dataset_path="dummy.csv",
        dataset_format="csv",
        reference_dataset_path="ref.csv",
        stream_progress=False
    )
    
    # Check that load_dataset was called twice (once for main, once for reference)
    assert mock_dataset_loader.load_dataset.call_count == 2

def test_validation_result_to_dict():
    result = ValidationResult(
        validation_id="val_123",
        dataset_id="ds_123",
        status="completed",
        created_at=datetime.now(),
        completed_at=datetime.now(),
        timestamp=datetime.now(),
        dataset_path="data.csv",
        dataset_format="csv",
        total_rows=100,
        total_time_seconds=10.0,
        data_loaded=True,
        load_time_seconds=1.0,
        diversity_score=80.0,
        diversity_metrics={},
        diversity_time_seconds=2.0,
        cascade_trained=False,
        cascade_models=0,
        cascade_time_seconds=0.0,
        collapse_detected=False,
        collapse_score=90.0,
        dimension_scores={},
        collapse_time_seconds=3.0,
        problematic_rows=[],
        localization_time_seconds=1.0,
        recommendations=[],
        projected_improvement=0.0,
        recommendation_time_seconds=1.0,
        approved_for_training=True,
        confidence=95.0,
        reason="All good",
        gpu_utilization_avg=50.0,
        gpu_memory_used_gb=4.0
    )
    
    data = result.to_dict()
    assert data['validation_id'] == "val_123"
    assert data['results']['risk_score'] == 10  # 100 - 90
    assert data['results']['recommendation'] == 'approved'
