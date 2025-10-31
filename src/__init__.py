"""
Synthos ML Validation Engine
Main package initialization

Primary Interface:
    SynthosOrchestrator - Main entry point that links all modules together
    
Example:
    from src import SynthosOrchestrator
    
    orchestrator = SynthosOrchestrator()
    result = await orchestrator.validate("data.parquet", "parquet")
    
    if result.approved_for_training:
        print("✅ Ready for training!")
"""

__version__ = "1.0.0"

# PRIMARY INTERFACE (use this for unified pipeline!)
from src.orchestrator import SynthosOrchestrator, ValidationResult

# Individual modules (for advanced use only)
from src.data_processors.dataset_loader import DatasetLoader
from src.validation_engine.diversity_analyzer import DiversityAnalyzer
from src.validation_engine.cascade_trainer import CascadeTrainer
from src.collapse_engine.collapse_detector import CollapseDetector
from src.collapse_engine.signature_library import SignatureLibrary
from src.collapse_engine.gradient_localizer import GradientLocalizer
from src.collapse_engine.recommendation_engine import RecommendationEngine
from src.utils.gpu_optimizer import GPUOptimizer

__all__ = [
    # PRIMARY INTERFACE (use this!)
    'SynthosOrchestrator',
    'ValidationResult',
    
    # Individual modules (advanced)
    'DatasetLoader',
    'DiversityAnalyzer',
    'CascadeTrainer',
    'CollapseDetector',
    'SignatureLibrary',
    'GradientLocalizer',
    'RecommendationEngine',
    'GPUOptimizer',
]
