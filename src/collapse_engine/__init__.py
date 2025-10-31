"""Collapse Engine - Phase 5-6: Collapse detection and localization"""

from .detector import CollapseDetector, CollapseScore, DimensionScore, CollapseConfig
from .signature_library import SignatureLibrary, CollapseSignature, MatchResult
from .localizer import CollapseLocalizer, LocalizationResult, LocalizationConfig
from .recommender import RecommendationEngine, Recommendation, RecommendationPlan, FixCategory, Priority

__all__ = [
    'CollapseDetector',
    'CollapseScore',
    'DimensionScore',
    'CollapseConfig',
    'SignatureLibrary',
    'CollapseSignature',
    'MatchResult',
    'CollapseLocalizer',
    'LocalizationResult',
    'LocalizationConfig',
    'RecommendationEngine',
    'Recommendation',
    'RecommendationPlan',
    'FixCategory',
    'Priority',
]
