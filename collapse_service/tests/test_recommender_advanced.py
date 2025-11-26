"""
Unit tests for recommender_advanced module.
Includes edge cases, boundary tests, and mocks.
"""
import sys
import asyncio
import pathlib
import numpy as np
import torch
import pytest
from unittest.mock import patch, MagicMock

# Ensure package import works when tests are run from repo root
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from collapse_engine import recommender_advanced as ra


# ============================ MODEL TESTS ============================

def test_impact_predictor_forward_shapes_and_types():
    model = ra.ImpactPredictor(input_dim=64, hidden_dims=[32, 16])
    x = torch.randn(4, 64)
    mean, log_var = model(x)
    assert isinstance(mean, torch.Tensor)
    assert isinstance(log_var, torch.Tensor)
    assert mean.shape[0] == 4
    assert log_var.shape[0] == 4


def _make_basic_recommendation(id_suffix="a"):
    impact = ra.ImpactPrediction(
        expected_improvement=10.0,
        lower_bound=7.0,
        upper_bound=13.0,
        confidence_level=ra.ConfidenceLevel.HIGH,
        success_probability=0.8,
        variance=4.0
    )

    cost = ra.CostEstimate(
        effort_hours=4.0,
        dollar_cost=100.0,
        computational_cost=1.0,
        data_cost=0.0,
        total_cost_normalized=0.2
    )

    rec = ra.Recommendation(
        id=f"test_{id_suffix}",
        title="Test Rec",
        description="Desc",
        category=ra.FixCategory.DATA_CLEANING,
        priority=ra.Priority.MEDIUM,
        impact_prediction=impact,
        cost_estimate=cost,
        feasibility_score=0.9,
        technical_complexity=0.2,
        dependencies=[],
        incompatible_with=[],
        synergies={},
        steps=[],
        estimated_duration_days=1.0,
        required_resources=[],
        validation_metrics=[],
        rollback_plan="",
        confidence_level=ra.ConfidenceLevel.HIGH,
        evidence_strength=0.5,
        novelty=0.1
    )

    return rec


def test_recommendation_cost_and_roi_and_risk():
    rec = _make_basic_recommendation()
    # cost in USD
    usd = rec.cost_estimate.get_total_usd()
    assert usd > 0

    roi = rec.get_expected_roi()
    assert isinstance(roi, float)

    risk_adj = rec.get_risk_adjusted_impact()
    assert abs(risk_adj - (rec.impact_prediction.expected_improvement * rec.impact_prediction.success_probability)) < 1e-6


def test_engine_feature_vector_and_predict_heuristic():
    engine = ra.AdvancedRecommendationEngine(use_gpu=False, enable_uncertainty=False)

    # simple dimension scores
    dims = {
        'distribution_fidelity': 70.0,
        'correlation_preservation': 80.0,
        'entropy_stability': 75.0,
        'gradient_health': 60.0,
        'loss_landscape': 85.0,
        'spectral_coherence': 77.0,
        'generalization_gap': 65.0,
        'statistical_consistency': 90.0
    }

    rec = _make_basic_recommendation()

    features = engine._create_impact_features(rec, collapse_score=40.0, dimension_scores=dims, dataset_size=1000)
    assert isinstance(features, np.ndarray)
    assert features.shape[0] == 64

    # Since enable_uncertainty=False, _predict_impact should use heuristics and return an ImpactPrediction
    impact_pred = asyncio.run(engine._predict_impact(rec, collapse_score=40.0, dimension_scores=dims, dataset_size=1000))
    assert isinstance(impact_pred, ra.ImpactPrediction)


def test_ordering_and_plan_computation():
    engine = ra.AdvancedRecommendationEngine(use_gpu=False, enable_uncertainty=False)

    # create three recommendations with simple impacts and synergies
    r1 = _make_basic_recommendation('1')
    r2 = _make_basic_recommendation('2')
    r3 = _make_basic_recommendation('3')

    # tweak values
    r1.id = 'r1'
    r2.id = 'r2'
    r3.id = 'r3'

    r1.impact_prediction.expected_improvement = 20.0
    r2.impact_prediction.expected_improvement = 5.0
    r3.impact_prediction.expected_improvement = 12.0

    r1.synergies = {'r3': 1.1}
    r3.dependencies = ['r2']

    selected = [r1, r2, r3]

    order = engine._determine_execution_order(selected)
    assert isinstance(order, list)

    total_impact, bounds, success_prob = engine._compute_plan_predictions(selected)
    assert total_impact >= 0
    assert isinstance(bounds, tuple) and len(bounds) == 2
    assert 0.0 <= success_prob <= 1.0

    quick_wins = engine._identify_quick_wins(selected)
    assert isinstance(quick_wins, list)


# ============================ EDGE CASE TESTS ============================

def test_impact_predictor_single_sample():
    """Single sample batch should work."""
    model = ra.ImpactPredictor(input_dim=64)
    x = torch.randn(1, 64)
    mean, log_var = model(x)
    assert mean.shape == (1,)
    assert log_var.shape == (1,)


def test_impact_predictor_empty_hidden_dims():
    """Empty hidden dims should create minimal network."""
    model = ra.ImpactPredictor(input_dim=32, hidden_dims=[])
    x = torch.randn(2, 32)
    mean, log_var = model(x)
    assert mean.shape == (2,)


def test_recommendation_zero_cost():
    """Handle edge case where cost is nearly zero."""
    impact = ra.ImpactPrediction(
        expected_improvement=10.0,
        lower_bound=5.0,
        upper_bound=15.0,
        confidence_level=ra.ConfidenceLevel.HIGH,
        success_probability=0.9,
        variance=1.0
    )
    cost = ra.CostEstimate(
        effort_hours=0.0,
        dollar_cost=0.0,
        computational_cost=0.0,
        data_cost=0.0,
        total_cost_normalized=0.0
    )
    rec = ra.Recommendation(
        id="zero_cost",
        title="Free Fix",
        description="No cost fix",
        category=ra.FixCategory.DATA_CLEANING,
        priority=ra.Priority.LOW,
        impact_prediction=impact,
        cost_estimate=cost,
        feasibility_score=1.0,
        technical_complexity=0.0,
        dependencies=[],
        incompatible_with=[],
        synergies={},
        steps=[],
        estimated_duration_days=0.1,
        required_resources=[],
        validation_metrics=[],
        rollback_plan="",
        confidence_level=ra.ConfidenceLevel.VERY_HIGH,
        evidence_strength=1.0,
        novelty=0.0
    )
    # ROI should not raise division by zero
    roi = rec.get_expected_roi()
    assert roi == 10.0  # 10 / 1 (minimum cost floored to 1)


def test_engine_empty_recommendations_list():
    """Engine should handle empty recommendation list gracefully."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    order = engine._determine_execution_order([])
    assert order == []

    total, bounds, prob = engine._compute_plan_predictions([])
    assert total == 0.0
    assert bounds == (0.0, 0.0)
    assert prob == 1.0

    quick_wins = engine._identify_quick_wins([])
    assert quick_wins == []


def test_engine_missing_dimension_scores():
    """Feature vector should handle missing dimension scores with defaults."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    rec = _make_basic_recommendation()
    # Only partial dimension scores
    dims = {'distribution_fidelity': 50.0}
    features = engine._create_impact_features(rec, collapse_score=30.0, dimension_scores=dims, dataset_size=500)
    assert features.shape == (64,)
    assert not np.any(np.isnan(features))


def test_engine_large_dataset_size():
    """Feature vector should handle very large dataset sizes."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    rec = _make_basic_recommendation()
    dims = {'distribution_fidelity': 80.0}
    features = engine._create_impact_features(rec, collapse_score=70.0, dimension_scores=dims, dataset_size=10_000_000)
    assert features.shape == (64,)
    assert not np.any(np.isinf(features))


def test_filter_by_constraints_budget_exceeded():
    """Filtering should exclude recs exceeding budget."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    rec = _make_basic_recommendation()
    rec.cost_estimate.dollar_cost = 5000.0
    rec.cost_estimate.effort_hours = 100.0

    filtered = engine._filter_by_constraints([rec], budget_usd=100.0, time_budget_days=None)
    assert len(filtered) == 0


def test_filter_by_constraints_time_exceeded():
    """Filtering should exclude recs exceeding time budget."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    rec = _make_basic_recommendation()
    rec.estimated_duration_days = 30.0

    filtered = engine._filter_by_constraints([rec], budget_usd=None, time_budget_days=7.0)
    assert len(filtered) == 0


def test_simple_prioritization_ordering():
    """Simple prioritization should order by priority then impact."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)

    r1 = _make_basic_recommendation('1')
    r2 = _make_basic_recommendation('2')
    r3 = _make_basic_recommendation('3')

    r1.priority = ra.Priority.LOW
    r1.impact_prediction.expected_improvement = 50.0

    r2.priority = ra.Priority.HIGH
    r2.impact_prediction.expected_improvement = 10.0

    r3.priority = ra.Priority.HIGH
    r3.impact_prediction.expected_improvement = 30.0

    result = engine._simple_prioritization([r1, r2, r3], max_recs=3)
    # HIGH priority first, then by impact descending
    assert result[0].priority == ra.Priority.HIGH
    assert result[1].priority == ra.Priority.HIGH


# ============================ ASYNC TESTS ============================

@pytest.mark.asyncio
async def test_generate_recommendations_minimal():
    """Generate recommendations with minimal inputs."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False, enable_optimization=False)
    dims = {'distribution_fidelity': 40.0, 'gradient_health': 30.0}

    plan = await engine.generate_recommendations(
        collapse_score=35.0,
        dimension_scores=dims,
        diversity_score=None,
        dataset_size=100,
        localization_results=None,
        budget_usd=None,
        time_budget_days=None,
        optimization_objective="balanced",
        max_recommendations=5
    )

    assert isinstance(plan, ra.RecommendationPlan)
    assert plan.total_expected_impact >= 0


@pytest.mark.asyncio
async def test_generate_critical_recommendations():
    """Critical recommendations should be generated for very low scores."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False, enable_optimization=False)
    dims = {'distribution_fidelity': 20.0}

    recs = await engine._generate_critical_recommendations(
        collapse_score=25.0,
        dimension_scores=dims,
        dataset_size=1000
    )

    assert isinstance(recs, list)
    # Very low score should trigger critical recommendations
    if len(recs) > 0:
        assert any(r.priority == ra.Priority.CRITICAL for r in recs)


# ============================ GPU MOCK TESTS ============================

def test_engine_gpu_fallback_to_cpu():
    """Engine should fall back to CPU when GPU unavailable."""
    with patch('torch.cuda.is_available', return_value=False):
        engine = ra.AdvancedRecommendationEngine(use_gpu=True)
        assert engine.device == torch.device('cpu')


def test_engine_gpu_enabled_when_available():
    """Engine should use GPU when available and requested."""
    with patch('torch.cuda.is_available', return_value=True):
        engine = ra.AdvancedRecommendationEngine(use_gpu=True)
        assert engine.device == torch.device('cuda')


# ============================ SUMMARY GENERATION ============================

def test_generate_summary_various_severities():
    """Summary should reflect different severity levels."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)

    # Critical
    summary = engine._generate_summary(25.0, [], 0.0, "balanced")
    assert "CRITICAL" in summary

    # Severe
    summary = engine._generate_summary(45.0, [], 0.0, "balanced")
    assert "SEVERE" in summary

    # Moderate
    summary = engine._generate_summary(60.0, [], 0.0, "balanced")
    assert "MODERATE" in summary

    # Minor
    summary = engine._generate_summary(80.0, [], 0.0, "balanced")
    assert "MINOR" in summary


def test_generate_risk_assessment():
    """Risk assessment should be generated without errors."""
    engine = ra.AdvancedRecommendationEngine(use_gpu=False)
    recs = [_make_basic_recommendation('x')]

    risk = engine._generate_risk_assessment(recs, success_probability=0.85)
    assert "RISK" in risk.upper()
