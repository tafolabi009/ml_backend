"""Collapse Engine - Phase 5-6: Collapse detection and localization"""

from .signature_library import (
    AdvancedSignatureLibrary as SignatureLibrary,  # Use advanced version as default
    AdvancedSignatureLibrary,  # Export both names
    CollapseSignature, 
    MatchResult,
    SearchMetrics,
    SignatureAutoencoder
)

try:
    from .detector import CollapseDetector, CollapseScore, DimensionScore, CollapseConfig
except ImportError:  # Optional dependency path for tests that only use signatures/recommenders
    CollapseDetector = None
    CollapseScore = None
    DimensionScore = None
    CollapseConfig = None

try:
    from .localizer import CollapseLocalizer, LocalizationResult, LocalizationConfig
except ImportError:
    CollapseLocalizer = None
    LocalizationResult = None
    LocalizationConfig = None

try:
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
except ImportError:
    RecommendationEngine = None
    AdvancedRecommendationEngine = None
    Recommendation = None
    RecommendationPlan = None
    FixCategory = None
    Priority = None
    ImpactPrediction = None
    CostEstimate = None
    ConfidenceLevel = None
    ImpactPredictor = None

__all__ = [
    'SignatureLibrary',
    'AdvancedSignatureLibrary',
    'CollapseSignature',
    'MatchResult',
    'SearchMetrics',
    'SignatureAutoencoder',
    'CollapseDetector',
    'CollapseScore',
    'DimensionScore',
    'CollapseConfig',
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
