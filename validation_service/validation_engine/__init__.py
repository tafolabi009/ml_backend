"""Validation Engine - Phase 2-5 of Synthos validation pipeline"""

from .diversity_analyzer import DiversityAnalyzer, DiversityScore, StratificationConfig

# CascadeTrainer has heavy dependencies (resonance_nn) - import lazily
def get_cascade_trainer():
    """Lazy import of CascadeTrainer to avoid import errors when dependencies missing."""
    from .cascade_trainer import CascadeTrainer
    return CascadeTrainer

__all__ = [
    'DiversityAnalyzer',
    'DiversityScore',
    'StratificationConfig',
    'get_cascade_trainer',
]
