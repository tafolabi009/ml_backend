"""
Recommendation Generator - Actionable fixes for collapse issues
Prioritizes recommendations by impact and feasibility

Features:
- Impact estimation for each fix
- Feasibility scoring
- Priority ranking
- Cost-benefit analysis
- Automated action plans
"""

import numpy as np
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class FixCategory(Enum):
    """Categories of fixes"""
    DATA_CLEANING = "data_cleaning"
    DATA_AUGMENTATION = "data_augmentation"
    RESAMPLING = "resampling"
    FEATURE_ENGINEERING = "feature_engineering"
    HYPERPARAMETER_TUNING = "hyperparameter_tuning"
    MODEL_ARCHITECTURE = "model_architecture"
    TRAINING_STRATEGY = "training_strategy"
    DATASET_REGENERATION = "dataset_regeneration"


class Priority(Enum):
    """Priority levels"""
    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


@dataclass
class Recommendation:
    """Single recommendation"""
    title: str
    description: str
    category: FixCategory
    priority: Priority
    estimated_impact: float  # 0-100 points improvement
    effort_hours: float
    cost_usd: float
    feasibility: float  # 0-1
    dependencies: List[str]
    steps: List[str]


@dataclass
class RecommendationPlan:
    """Complete recommendation plan"""
    recommendations: List[Recommendation]
    total_estimated_impact: float
    total_effort_hours: float
    total_cost_usd: float
    execution_order: List[int]  # Indices in optimal order
    summary: str
    
    # Add compatibility properties
    @property
    def projected_improvement(self) -> float:
        """Alias for total_estimated_impact"""
        return self.total_estimated_impact
    
    @property
    def projected_score(self) -> float:
        """Estimated score after applying recommendations"""
        # This would be set by the recommender
        return getattr(self, '_projected_score', 0.0)
    
    @projected_score.setter
    def projected_score(self, value: float):
        self._projected_score = value


class RecommendationEngine:
    """
    Generates prioritized, actionable recommendations for fixing collapse issues.
    """
    
    def __init__(self):
        logger.info("RecommendationEngine initialized")
    
    async def generate_recommendations(
        self,
        collapse_score: float,
        dimension_scores: Dict[str, Any],  # Can be DimensionScore objects or floats
        diversity_score: Optional[float] = None,
        dataset_size: int = 0,
        localization_results: Optional[Any] = None,
        budget_usd: Optional[float] = None
    ) -> RecommendationPlan:
        """
        Generate prioritized recommendations based on collapse analysis.
        
        Args:
            collapse_score: Overall collapse score (0-100)
            dimension_scores: Individual dimension scores
            diversity_score: Diversity score (optional)
            dataset_size: Number of rows in dataset
            localization_results: Results from localizer (optional)
            budget_usd: Available budget for fixes (optional)
        
        Returns:
            Complete recommendation plan (RecommendationPlan object)
        """
        logger.info("Generating recommendations...")
        
        recommendations = []
        
        # Generate recommendations for each failed dimension
        for dim_name, dim_score_obj in dimension_scores.items():
            # Extract score value (handle both DimensionScore objects and floats)
            if hasattr(dim_score_obj, 'score'):
                score = dim_score_obj.score
            else:
                score = dim_score_obj
            
            if score < 65:
                dim_recommendations = self._generate_dimension_recommendations(
                    dim_name, score, dataset_size
                )
                recommendations.extend(dim_recommendations)
        
        # Add localization-based recommendations
        if localization_results:
            localization_recs = self._generate_localization_recommendations(
                localization_results, dataset_size
            )
            recommendations.extend(localization_recs)
        
        # Add general best practices if score is very low
        if collapse_score < 50:
            critical_recs = self._generate_critical_recommendations(collapse_score)
            recommendations.extend(critical_recs)
        
        # Prioritize and filter recommendations
        recommendations = self._prioritize_recommendations(recommendations, budget_usd)
        
        # Determine execution order
        execution_order = self._determine_execution_order(recommendations)
        
        # Compute totals
        total_impact = sum(rec.estimated_impact for rec in recommendations)
        total_effort = sum(rec.effort_hours for rec in recommendations)
        total_cost = sum(rec.cost_usd for rec in recommendations)
        
        # Generate summary
        summary = self._generate_summary(recommendations, collapse_score, total_impact)
        
        # Calculate projected score
        projected_score = min(100.0, collapse_score + total_impact)
        
        # Create plan
        plan = RecommendationPlan(
            recommendations=recommendations,
            total_estimated_impact=total_impact,
            total_effort_hours=total_effort,
            total_cost_usd=total_cost,
            execution_order=execution_order,
            summary=summary
        )
        
        # Set projected score
        plan.projected_score = projected_score
        
        return plan
    
    # ==================== DIMENSION-SPECIFIC RECOMMENDATIONS ====================
    
    def _generate_dimension_recommendations(
        self, dim_name: str, score: float, dataset_size: int
    ) -> List[Recommendation]:
        """Generate recommendations for specific dimension failure"""
        recommendations = []
        
        if dim_name == 'distribution_fidelity':
            recommendations.extend(self._recommend_distribution_fixes(score, dataset_size))
        
        elif dim_name == 'correlation_preservation':
            recommendations.extend(self._recommend_correlation_fixes(score, dataset_size))
        
        elif dim_name == 'entropy_stability':
            recommendations.extend(self._recommend_entropy_fixes(score, dataset_size))
        
        elif dim_name == 'gradient_health':
            recommendations.extend(self._recommend_gradient_fixes(score, dataset_size))
        
        elif dim_name == 'loss_landscape':
            recommendations.extend(self._recommend_loss_fixes(score, dataset_size))
        
        elif dim_name == 'spectral_coherence':
            recommendations.extend(self._recommend_spectral_fixes(score, dataset_size))
        
        elif dim_name == 'generalization_gap':
            recommendations.extend(self._recommend_generalization_fixes(score, dataset_size))
        
        elif dim_name == 'statistical_consistency':
            recommendations.extend(self._recommend_statistical_fixes(score, dataset_size))
        
        return recommendations
    
    def _recommend_distribution_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for distribution fidelity issues"""
        recs = []
        
        # High priority: Data augmentation
        recs.append(Recommendation(
            title="Apply Advanced Data Augmentation",
            description=(
                "Use mixup, cutmix, or domain-specific augmentation to increase distribution coverage. "
                "This helps match the original data distribution more closely."
            ),
            category=FixCategory.DATA_AUGMENTATION,
            priority=Priority.HIGH if score < 50 else Priority.MEDIUM,
            estimated_impact=15.0,
            effort_hours=8.0,
            cost_usd=200.0,
            feasibility=0.9,
            dependencies=[],
            steps=[
                "1. Identify key distribution mismatches",
                "2. Design augmentation strategy (mixup, cutmix, etc.)",
                "3. Implement augmentation pipeline",
                "4. Validate distribution match improvement",
                "5. Re-run collapse detection"
            ]
        ))
        
        # Medium priority: Resampling
        if dataset_size > 100000:
            recs.append(Recommendation(
                title="Stratified Resampling",
                description=(
                    "Apply multi-dimensional stratified sampling to ensure all distribution modes "
                    "are adequately represented in the training set."
                ),
                category=FixCategory.RESAMPLING,
                priority=Priority.MEDIUM,
                estimated_impact=10.0,
                effort_hours=4.0,
                cost_usd=100.0,
                feasibility=0.95,
                dependencies=[],
                steps=[
                    "1. Identify underrepresented distribution modes",
                    "2. Design stratification strategy",
                    "3. Resample dataset with stratification",
                    "4. Validate coverage improvement"
                ]
            ))
        
        return recs
    
    def _recommend_correlation_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for correlation preservation issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Preserve Feature Correlations",
            description=(
                "Use copula-based methods or conditional generation to maintain correlations "
                "between features during data generation."
            ),
            category=FixCategory.FEATURE_ENGINEERING,
            priority=Priority.HIGH if score < 50 else Priority.MEDIUM,
            estimated_impact=12.0,
            effort_hours=12.0,
            cost_usd=300.0,
            feasibility=0.7,
            dependencies=[],
            steps=[
                "1. Analyze correlation matrix differences",
                "2. Identify most critical correlations",
                "3. Implement conditional generation or copula methods",
                "4. Validate correlation preservation",
                "5. Re-generate dataset"
            ]
        ))
        
        return recs
    
    def _recommend_entropy_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for entropy stability issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Increase Dataset Diversity",
            description=(
                "Add more diverse samples or use entropy regularization during training "
                "to maintain information content."
            ),
            category=FixCategory.DATA_AUGMENTATION,
            priority=Priority.HIGH,
            estimated_impact=14.0,
            effort_hours=10.0,
            cost_usd=250.0,
            feasibility=0.85,
            dependencies=[],
            steps=[
                "1. Measure entropy by feature",
                "2. Identify low-entropy regions",
                "3. Generate additional diverse samples",
                "4. Apply entropy regularization in training",
                "5. Validate entropy improvement"
            ]
        ))
        
        return recs
    
    def _recommend_gradient_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for gradient health issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Adjust Learning Rate and Optimization",
            description=(
                "Tune learning rate, use gradient clipping, or switch optimizer "
                "(e.g., Adam -> AdamW with weight decay) to improve gradient health."
            ),
            category=FixCategory.HYPERPARAMETER_TUNING,
            priority=Priority.CRITICAL if score < 40 else Priority.HIGH,
            estimated_impact=18.0,
            effort_hours=6.0,
            cost_usd=150.0,
            feasibility=0.95,
            dependencies=[],
            steps=[
                "1. Analyze gradient statistics (norms, variance)",
                "2. Implement gradient clipping",
                "3. Tune learning rate (use scheduler)",
                "4. Experiment with different optimizers",
                "5. Monitor gradient health during training"
            ]
        ))
        
        return recs
    
    def _recommend_loss_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for loss landscape issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Improve Training Stability",
            description=(
                "Use learning rate warmup, cosine annealing, or curriculum learning "
                "to achieve smoother loss curves and better convergence."
            ),
            category=FixCategory.TRAINING_STRATEGY,
            priority=Priority.HIGH,
            estimated_impact=16.0,
            effort_hours=8.0,
            cost_usd=200.0,
            feasibility=0.9,
            dependencies=[],
            steps=[
                "1. Analyze loss curve patterns",
                "2. Implement learning rate warmup",
                "3. Add cosine annealing scheduler",
                "4. Optionally implement curriculum learning",
                "5. Monitor loss smoothness"
            ]
        ))
        
        return recs
    
    def _recommend_spectral_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for spectral coherence issues (FFT-specific)"""
        recs = []
        
        recs.append(Recommendation(
            title="FFT-Based Data Filtering",
            description=(
                "Apply frequency-domain filtering to remove noise and improve spectral coherence. "
                "Align with Resonance NN's FFT-based architecture."
            ),
            category=FixCategory.DATA_CLEANING,
            priority=Priority.HIGH,
            estimated_impact=13.0,
            effort_hours=10.0,
            cost_usd=250.0,
            feasibility=0.8,
            dependencies=[],
            steps=[
                "1. Perform FFT analysis on data",
                "2. Identify noisy frequency components",
                "3. Design frequency-domain filters",
                "4. Apply filtering to dataset",
                "5. Validate spectral coherence improvement"
            ]
        ))
        
        return recs
    
    def _recommend_generalization_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for generalization gap issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Regularization and Dropout",
            description=(
                "Add dropout layers, weight decay, or other regularization techniques "
                "to prevent overfitting and reduce generalization gap."
            ),
            category=FixCategory.MODEL_ARCHITECTURE,
            priority=Priority.HIGH,
            estimated_impact=14.0,
            effort_hours=5.0,
            cost_usd=125.0,
            feasibility=0.95,
            dependencies=[],
            steps=[
                "1. Add dropout layers (0.1-0.3 rate)",
                "2. Implement weight decay in optimizer",
                "3. Consider spectral normalization",
                "4. Tune regularization strength",
                "5. Monitor train/val gap"
            ]
        ))
        
        return recs
    
    def _recommend_statistical_fixes(self, score: float, dataset_size: int) -> List[Recommendation]:
        """Recommendations for statistical consistency issues"""
        recs = []
        
        recs.append(Recommendation(
            title="Statistical Validation and Cleaning",
            description=(
                "Apply rigorous statistical tests and remove statistically inconsistent samples. "
                "Use robust statistics for outlier detection."
            ),
            category=FixCategory.DATA_CLEANING,
            priority=Priority.MEDIUM,
            estimated_impact=10.0,
            effort_hours=6.0,
            cost_usd=150.0,
            feasibility=0.9,
            dependencies=[],
            steps=[
                "1. Run comprehensive statistical tests",
                "2. Identify inconsistent samples",
                "3. Apply robust outlier detection",
                "4. Remove or transform problematic data",
                "5. Re-validate statistical consistency"
            ]
        ))
        
        return recs
    
    # ==================== LOCALIZATION-BASED RECOMMENDATIONS ====================
    
    def _generate_localization_recommendations(
        self, localization_results: Any, dataset_size: int
    ) -> List[Recommendation]:
        """Generate recommendations based on localization results"""
        recs = []
        
        problematic_pct = localization_results.percentage_problematic
        
        if problematic_pct > 10:
            # Critical: Remove problematic rows
            recs.append(Recommendation(
                title="Remove Problematic Data Rows",
                description=(
                    f"Remove {localization_results.total_problematic:,} identified problematic rows "
                    f"({problematic_pct:.1f}% of dataset). These rows are causing collapse."
                ),
                category=FixCategory.DATA_CLEANING,
                priority=Priority.CRITICAL,
                estimated_impact=25.0,
                effort_hours=2.0,
                cost_usd=50.0,
                feasibility=1.0,
                dependencies=[],
                steps=[
                    "1. Export problematic row indices",
                    "2. Filter dataset to remove these rows",
                    "3. Validate remaining dataset quality",
                    "4. Re-run collapse detection"
                ]
            ))
        
        return recs
    
    # ==================== CRITICAL RECOMMENDATIONS ====================
    
    def _generate_critical_recommendations(self, collapse_score: float) -> List[Recommendation]:
        """Generate critical recommendations for severe collapse"""
        recs = []
        
        if collapse_score < 30:
            # Severe collapse: Regenerate dataset
            recs.append(Recommendation(
                title="CRITICAL: Regenerate Entire Dataset",
                description=(
                    "Collapse score is extremely low (<30). The dataset is not usable. "
                    "Regenerate with different parameters or use a different generation method."
                ),
                category=FixCategory.DATASET_REGENERATION,
                priority=Priority.CRITICAL,
                estimated_impact=80.0,
                effort_hours=40.0,
                cost_usd=2000.0,
                feasibility=0.8,
                dependencies=[],
                steps=[
                    "1. Analyze root cause of collapse",
                    "2. Adjust generation parameters",
                    "3. Consider alternative generation methods",
                    "4. Regenerate dataset from scratch",
                    "5. Run full validation pipeline"
                ]
            ))
        
        return recs
    
    # ==================== PRIORITIZATION ====================
    
    def _prioritize_recommendations(
        self, recommendations: List[Recommendation], budget_usd: Optional[float]
    ) -> List[Recommendation]:
        """Prioritize and filter recommendations"""
        # Sort by priority first, then by impact/cost ratio
        def sort_key(rec):
            priority_score = rec.priority.value  # Lower is better
            impact_per_dollar = rec.estimated_impact / (rec.cost_usd + 1)
            return (priority_score, -impact_per_dollar)
        
        recommendations.sort(key=sort_key)
        
        # Filter by budget if provided
        if budget_usd is not None:
            cumulative_cost = 0
            filtered_recs = []
            for rec in recommendations:
                if cumulative_cost + rec.cost_usd <= budget_usd:
                    filtered_recs.append(rec)
                    cumulative_cost += rec.cost_usd
            return filtered_recs
        
        return recommendations
    
    def _determine_execution_order(self, recommendations: List[Recommendation]) -> List[int]:
        """Determine optimal execution order considering dependencies"""
        # Simple topological sort based on dependencies
        # For now, just return order by priority
        return list(range(len(recommendations)))
    
    # ==================== SUMMARY ====================
    
    def _generate_summary(
        self, recommendations: List[Recommendation], collapse_score: float, total_impact: float
    ) -> str:
        """Generate executive summary"""
        if collapse_score < 30:
            severity = "CRITICAL"
            action = "IMMEDIATE ACTION REQUIRED"
        elif collapse_score < 50:
            severity = "SEVERE"
            action = "URGENT ACTION NEEDED"
        elif collapse_score < 65:
            severity = "MODERATE"
            action = "Action Recommended"
        else:
            severity = "MINOR"
            action = "Optional Improvements"
        
        summary = f"""
COLLAPSE ANALYSIS SUMMARY
=========================

Current Score: {collapse_score:.1f}/100
Severity: {severity}
Status: {action}

Generated {len(recommendations)} recommendations with estimated {total_impact:.1f} points improvement.

Priority Breakdown:
- Critical: {sum(1 for r in recommendations if r.priority == Priority.CRITICAL)}
- High: {sum(1 for r in recommendations if r.priority == Priority.HIGH)}
- Medium: {sum(1 for r in recommendations if r.priority == Priority.MEDIUM)}
- Low: {sum(1 for r in recommendations if r.priority == Priority.LOW)}

Projected Score After Fixes: {min(100, collapse_score + total_impact):.1f}/100

Next Steps:
1. Review prioritized recommendations below
2. Execute critical and high-priority fixes first
3. Re-run validation after each major fix
4. Monitor improvements in real-time
"""
        
        return summary
