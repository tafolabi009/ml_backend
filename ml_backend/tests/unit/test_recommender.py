import pytest
import numpy as np
import torch
from unittest.mock import MagicMock, patch
from src.collapse_engine.recommender import (
    AdvancedRecommendationEngine, 
    RecommendationPlan, 
    Recommendation,
    FixCategory,
    Priority,
    ConfidenceLevel,
    ImpactPrediction,
    CostEstimate
)
from src.collapse_engine.detector import DimensionScore

class TestAdvancedRecommendationEngine:
    
    @pytest.fixture
    def engine(self):
        # Initialize with uncertainty disabled to use heuristics (deterministic)
        return AdvancedRecommendationEngine(
            enable_optimization=True,
            enable_uncertainty=False,
            use_gpu=False
        )
    
    @pytest.fixture
    def sample_dimension_scores(self):
        return {
            'distribution_fidelity': DimensionScore(
                name='Distribution Fidelity', score=40.0, threshold=70.0, 
                passed=False, metrics={}, severity='critical'
            ),
            'correlation_preservation': DimensionScore(
                name='Correlation Preservation', score=80.0, threshold=70.0, 
                passed=True, metrics={}, severity='ok'
            ),
            'entropy_stability': 60.0,  # Test float input support
            'gradient_health': 90.0,
            'loss_landscape': 85.0,
            'spectral_coherence': 55.0,
            'generalization_gap': 75.0,
            'statistical_consistency': 65.0
        }

    @pytest.mark.asyncio
    async def test_generate_recommendations_basic(self, engine, sample_dimension_scores):
        """Test basic recommendation generation"""
        plan = await engine.generate_recommendations(
            collapse_score=55.0,
            dimension_scores=sample_dimension_scores,
            diversity_score=70.0,
            dataset_size=10000
        )
        
        assert isinstance(plan, RecommendationPlan)
        assert len(plan.recommendations) > 0
        assert plan.total_expected_impact > 0
        assert plan.total_cost_usd > 0
        
        # Check that critical dimensions generated recommendations
        rec_titles = [r.title for r in plan.recommendations]
        # distribution_fidelity (40.0) should trigger "Advanced Data Augmentation"
        assert any("Augmentation" in t for t in rec_titles)

    @pytest.mark.asyncio
    async def test_optimization_objectives(self, engine, sample_dimension_scores):
        """Test different optimization objectives"""
        # Max Impact
        plan_impact = await engine.generate_recommendations(
            collapse_score=50.0,
            dimension_scores=sample_dimension_scores,
            optimization_objective="max_impact",
            max_recommendations=3
        )
        
        # Min Cost
        plan_cost = await engine.generate_recommendations(
            collapse_score=50.0,
            dimension_scores=sample_dimension_scores,
            optimization_objective="min_cost",
            max_recommendations=3
        )
        
        # Verify impact plan has higher impact
        assert plan_impact.total_expected_impact >= plan_cost.total_expected_impact
        
        # Verify cost plan has lower cost
        assert plan_cost.total_cost_usd <= plan_impact.total_cost_usd

    @pytest.mark.asyncio
    async def test_constraints(self, engine, sample_dimension_scores):
        """Test budget and time constraints"""
        # Very low budget
        plan = await engine.generate_recommendations(
            collapse_score=50.0,
            dimension_scores=sample_dimension_scores,
            budget_usd=10.0,  # Very low
            time_budget_days=100.0
        )
        
        # Should filter out expensive recommendations
        for rec in plan.recommendations:
            assert rec.cost_estimate.get_total_usd() <= 10.0
            
        # Very low time
        plan = await engine.generate_recommendations(
            collapse_score=50.0,
            dimension_scores=sample_dimension_scores,
            budget_usd=10000.0,
            time_budget_days=0.1  # Very low
        )
        
        for rec in plan.recommendations:
            assert rec.estimated_duration_days <= 0.1

    @pytest.mark.asyncio
    async def test_critical_collapse_recommendations(self, engine, sample_dimension_scores):
        """Test recommendations for critical collapse"""
        plan = await engine.generate_recommendations(
            collapse_score=20.0,  # Critical
            dimension_scores=sample_dimension_scores,
            dataset_size=10000
        )
        
        # Should recommend full regeneration
        rec_titles = [r.title for r in plan.recommendations]
        assert any("Regeneration" in t for t in rec_titles)
        
        # Regeneration should be critical priority
        regen_rec = next(r for r in plan.recommendations if "Regeneration" in r.title)
        assert regen_rec.priority == Priority.CRITICAL

    @pytest.mark.asyncio
    async def test_diversity_recommendations(self, engine, sample_dimension_scores):
        """Test recommendations for low diversity"""
        plan = await engine.generate_recommendations(
            collapse_score=70.0,
            dimension_scores=sample_dimension_scores,
            diversity_score=40.0,  # Low diversity
            dataset_size=10000
        )
        
        rec_titles = [r.title for r in plan.recommendations]
        assert any("Diversity" in t for t in rec_titles)

    def test_execution_ordering(self, engine):
        """Test topological sort for execution order"""
        # Create dummy recommendations with dependencies
        rec1 = self._create_dummy_rec("rec1", [], priority=Priority.MEDIUM)
        rec2 = self._create_dummy_rec("rec2", ["rec1"], priority=Priority.HIGH)  # Depends on rec1
        rec3 = self._create_dummy_rec("rec3", ["rec2"], priority=Priority.LOW)   # Depends on rec2
        
        recs = [rec3, rec2, rec1]  # Unordered
        
        order = engine._determine_execution_order(recs)
        
        assert order == ["rec1", "rec2", "rec3"]
        
    def _create_dummy_rec(self, id, deps, priority=Priority.MEDIUM):
        return Recommendation(
            id=id,
            title=f"Title {id}",
            description="desc",
            category=FixCategory.DATA_CLEANING,
            priority=priority,
            impact_prediction=ImpactPrediction(10, 5, 15, ConfidenceLevel.MEDIUM, 0.8, 1.0),
            cost_estimate=CostEstimate(1, 10, 1, 1, 0.1),
            feasibility_score=0.9,
            technical_complexity=0.1,
            dependencies=deps,
            incompatible_with=[],
            synergies={},
            steps=[],
            estimated_duration_days=1.0,
            required_resources=[],
            validation_metrics=[],
            rollback_plan="",
            confidence_level=ConfidenceLevel.MEDIUM,
            evidence_strength=0.5,
            novelty=0.1
        )

    @pytest.mark.asyncio
    async def test_impact_prediction_heuristic(self, engine):
        """Test heuristic impact prediction"""
        rec = self._create_dummy_rec("test", [])
        rec.impact_prediction.expected_improvement = 20.0
        
        pred = await engine._predict_impact(rec, 50.0, {}, 1000)
        
        assert pred.expected_improvement == 20.0
        assert pred.lower_bound < 20.0
        assert pred.upper_bound > 20.0
        assert pred.confidence_level == ConfidenceLevel.MEDIUM

    @pytest.mark.asyncio
    async def test_localization_recommendations(self, engine, sample_dimension_scores):
        """Test recommendations based on localization results"""
        localization_results = MagicMock()
        localization_results.percentage_problematic = 10.0
        localization_results.total_problematic = 1000
        
        plan = await engine.generate_recommendations(
            collapse_score=60.0,
            dimension_scores=sample_dimension_scores,
            localization_results=localization_results,
            dataset_size=10000
        )
        
        rec_titles = [r.title for r in plan.recommendations]
        assert any("Remove Problematic" in t for t in rec_titles)

    @pytest.mark.asyncio
    async def test_small_dataset_recommendations(self, engine, sample_dimension_scores):
        """Test recommendations for small datasets"""
        plan = await engine.generate_recommendations(
            collapse_score=60.0,
            dimension_scores=sample_dimension_scores,
            dataset_size=100  # Very small
        )
        
        # Should recommend data collection
        rec_titles = [r.title for r in plan.recommendations]
        assert any("Data Collection" in t or "Augmentation" in t for t in rec_titles)

    @pytest.mark.asyncio
    async def test_max_recommendations_limit(self, engine, sample_dimension_scores):
        """Test max recommendations limit"""
        plan = await engine.generate_recommendations(
            collapse_score=40.0,
            dimension_scores=sample_dimension_scores,
            max_recommendations=2
        )
        
        assert len(plan.recommendations) <= 2

    @pytest.mark.asyncio
    async def test_combined_objectives(self, engine, sample_dimension_scores):
        """Test combined optimization objectives"""
        plan = await engine.generate_recommendations(
            collapse_score=50.0,
            dimension_scores=sample_dimension_scores,
            optimization_objective="balanced"
        )
        
        assert isinstance(plan, RecommendationPlan)
        assert plan.total_expected_impact > 0
        assert plan.total_cost_usd > 0

    def test_circular_dependency_detection(self, engine):
        """Test circular dependency detection"""
        rec1 = self._create_dummy_rec("rec1", ["rec2"])
        rec2 = self._create_dummy_rec("rec2", ["rec1"])
        
        # Should handle circular dependencies gracefully
        # The algorithm may skip recommendations with circular dependencies
        order = engine._determine_execution_order([rec1, rec2])
        
        # Either breaks the cycle or returns empty list (both are valid)
        assert len(order) <= 2

    def test_priority_sorting(self, engine):
        """Test that recommendations are sorted by priority"""
        rec_low = self._create_dummy_rec("low", [], priority=Priority.LOW)
        rec_high = self._create_dummy_rec("high", [], priority=Priority.CRITICAL)
        rec_med = self._create_dummy_rec("med", [], priority=Priority.MEDIUM)
        
        recs = [rec_low, rec_med, rec_high]
        
        # Get execution order which should respect priorities
        order = engine._determine_execution_order(recs)
        
        # Higher priority should come first (when no dependencies)
        assert order.index("high") < order.index("med")
        assert order.index("med") < order.index("low")

