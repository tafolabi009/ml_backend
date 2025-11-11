"""Collapse Engine - Phase 5-6: Collapse detection and localization"""

from .detector import CollapseDetector, CollapseScore, DimensionScore, CollapseConfig
from .signature_library import (
    AdvancedSignatureLibrary as SignatureLibrary,  # Use advanced version as default
    AdvancedSignatureLibrary,  # Export both names
    CollapseSignature, 
    MatchResult,
    SearchMetrics,
    SignatureAutoencoder
)
from .localizer import CollapseLocalizer, LocalizationResult, LocalizationConfig
from .recommender import (
    AdvancedRecommendationEngine as RecommendationEngine,  # Use advanced version as default
    AdvancedRecommendationEngine,  # Export both names
    Recommendation,  # This is the correct name
    RecommendationPlan,
    FixCategory, 
    Priority,
    ImpactPrediction,
    CostEstimate,
    ConfidenceLevel,
    ImpactPredictor
)

__all__ = [
    'CollapseDetector',
    'CollapseScore',
    'DimensionScore',
    'CollapseConfig',
    'SignatureLibrary',
    'AdvancedSignatureLibrary',
    'CollapseSignature',
    'MatchResult',
    'SearchMetrics',
    'SignatureAutoencoder',
    'CollapseLocalizer',
    'LocalizationResult',
    'LocalizationConfig',
    'RecommendationEngine',
    'AdvancedRecommendationEngine',
    'Recommendation',
    'RecommendationPlan',
    'FixCategory',
    'Priority',
    'ImpactPrediction',
    'CostEstimate',
    'ConfidenceLevel',
    'ImpactPredictor'
]
