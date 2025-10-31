"""
Synthos Validation Engine Orchestrator
======================================

This is the main entry point that links all modules together into a unified pipeline.
Data flows automatically through all validation stages without manual coordination.

Pipeline Flow:
1. Data Loading (DatasetLoader)
2. Diversity Analysis (DiversityAnalyzer) 
3. Cascade Training (CascadeTrainer)
4. Collapse Detection (CollapseDetector)
5. Problem Localization (GradientLocalizer)
6. Recommendations (RecommendationEngine)

Author: ML Engineering Team
Date: October 31, 2025
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
import json

import torch
import numpy as np
import pandas as pd

from src.data_processors.dataset_loader import DatasetLoader
from src.validation_engine.diversity_analyzer import DiversityAnalyzer
from src.validation_engine.cascade_trainer import CascadeTrainer
from src.collapse_engine.collapse_detector import CollapseDetector
from src.collapse_engine.signature_library import SignatureLibrary
from src.collapse_engine.gradient_localizer import GradientLocalizer
from src.collapse_engine.recommendation_engine import RecommendationEngine
from src.utils.gpu_optimizer import GPUOptimizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Complete validation result with all stages."""
    
    # Metadata
    validation_id: str
    timestamp: datetime
    dataset_path: str
    dataset_format: str
    total_rows: int
    total_time_seconds: float
    
    # Stage 1: Data Loading
    data_loaded: bool
    load_time_seconds: float
    
    # Stage 2: Diversity Analysis
    diversity_score: float
    diversity_metrics: Dict[str, Any]
    diversity_time_seconds: float
    
    # Stage 3: Cascade Training
    cascade_trained: bool
    cascade_models: int
    cascade_time_seconds: float
    
    # Stage 4: Collapse Detection
    collapse_detected: bool
    collapse_score: float
    dimension_scores: Dict[str, float]
    collapse_time_seconds: float
    
    # Stage 5: Localization
    problematic_rows: List[int]
    localization_time_seconds: float
    
    # Stage 6: Recommendations
    recommendations: List[Dict[str, Any]]
    projected_improvement: float
    recommendation_time_seconds: float
    
    # Final Decision
    approved_for_training: bool
    confidence: float
    reason: str
    
    # GPU Metrics
    gpu_utilization_avg: float
    gpu_memory_used_gb: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            'validation_id': self.validation_id,
            'timestamp': self.timestamp.isoformat(),
            'dataset_path': self.dataset_path,
            'dataset_format': self.dataset_format,
            'total_rows': self.total_rows,
            'total_time_seconds': self.total_time_seconds,
            'stages': {
                'data_loading': {
                    'loaded': self.data_loaded,
                    'time_seconds': self.load_time_seconds
                },
                'diversity_analysis': {
                    'score': self.diversity_score,
                    'metrics': self.diversity_metrics,
                    'time_seconds': self.diversity_time_seconds
                },
                'cascade_training': {
                    'trained': self.cascade_trained,
                    'num_models': self.cascade_models,
                    'time_seconds': self.cascade_time_seconds
                },
                'collapse_detection': {
                    'detected': self.collapse_detected,
                    'score': self.collapse_score,
                    'dimensions': self.dimension_scores,
                    'time_seconds': self.collapse_time_seconds
                },
                'localization': {
                    'problematic_rows': len(self.problematic_rows),
                    'row_indices': self.problematic_rows[:100],  # First 100
                    'time_seconds': self.localization_time_seconds
                },
                'recommendations': {
                    'count': len(self.recommendations),
                    'items': self.recommendations,
                    'projected_improvement': self.projected_improvement,
                    'time_seconds': self.recommendation_time_seconds
                }
            },
            'final_decision': {
                'approved_for_training': self.approved_for_training,
                'confidence': self.confidence,
                'reason': self.reason
            },
            'gpu_metrics': {
                'utilization_avg': self.gpu_utilization_avg,
                'memory_used_gb': self.gpu_memory_used_gb
            }
        }
    
    def save_report(self, output_path: str):
        """Save validation report to JSON file."""
        with open(output_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"📄 Validation report saved to: {output_path}")


class SynthosOrchestrator:
    """
    Main orchestrator that coordinates all validation modules.
    
    This class provides a single entry point for the entire validation pipeline.
    Simply call validate() with your dataset path and it handles everything else.
    
    Example:
        orchestrator = SynthosOrchestrator()
        result = await orchestrator.validate("data.parquet", "parquet")
        
        if result.approved_for_training:
            print("✅ Dataset approved!")
        else:
            print(f"❌ Issues found: {result.reason}")
    """
    
    def __init__(
        self,
        gpu_memory_fraction: float = 0.9,
        enable_mixed_precision: bool = True,
        collapse_threshold: float = 65.0,
        diversity_threshold: float = 50.0,
        use_cache: bool = True
    ):
        """
        Initialize orchestrator and all sub-modules.
        
        Args:
            gpu_memory_fraction: Fraction of GPU memory to use (0.0-1.0)
            enable_mixed_precision: Use BF16/FP16 for H100 efficiency
            collapse_threshold: Score below this triggers rejection (0-100)
            diversity_threshold: Minimum diversity score required
            use_cache: Cache intermediate results for faster re-runs
        """
        logger.info("🚀 Initializing Synthos Validation Engine...")
        
        self.collapse_threshold = collapse_threshold
        self.diversity_threshold = diversity_threshold
        self.use_cache = use_cache
        
        # Initialize GPU optimizer first
        self.gpu_optimizer = GPUOptimizer(
            memory_fraction=gpu_memory_fraction,
            enable_mixed_precision=enable_mixed_precision
        )
        
        # Initialize all modules
        logger.info("📦 Loading modules...")
        self.dataset_loader = DatasetLoader()
        self.diversity_analyzer = DiversityAnalyzer()
        self.cascade_trainer = CascadeTrainer()
        self.collapse_detector = CollapseDetector()
        self.signature_library = SignatureLibrary()
        self.gradient_localizer = GradientLocalizer()
        self.recommendation_engine = RecommendationEngine()
        
        # Metrics tracking
        self.gpu_metrics = []
        
        logger.info("✅ All modules initialized successfully!")
    
    async def validate(
        self,
        dataset_path: str,
        dataset_format: str,
        reference_dataset_path: Optional[str] = None,
        output_report_path: Optional[str] = None,
        stream_progress: bool = True
    ) -> ValidationResult:
        """
        Main validation pipeline - orchestrates all modules automatically.
        
        This is the primary entry point. It handles the entire workflow:
        1. Loads data
        2. Analyzes diversity
        3. Trains cascade models
        4. Detects collapse
        5. Localizes problems
        6. Generates recommendations
        7. Makes final decision
        
        Args:
            dataset_path: Path to dataset to validate
            dataset_format: Format (csv, json, parquet, hdf5, etc.)
            reference_dataset_path: Optional reference dataset for comparison
            output_report_path: Where to save JSON report (optional)
            stream_progress: Print progress updates
            
        Returns:
            ValidationResult with complete analysis and decision
        """
        start_time = asyncio.get_event_loop().time()
        validation_id = f"val_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"🔍 Starting validation: {validation_id}")
        logger.info(f"📂 Dataset: {dataset_path}")
        logger.info(f"📊 Format: {dataset_format}")
        
        # ============================================================
        # STAGE 1: DATA LOADING
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 1/6: LOADING DATA")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        try:
            dataset = await self.dataset_loader.load_dataset(
                dataset_path,
                dataset_format
            )
            total_rows = len(dataset)
            
            # Load reference dataset if provided
            reference_data = None
            if reference_dataset_path:
                reference_data = await self.dataset_loader.load_dataset(
                    reference_dataset_path,
                    dataset_format
                )
            
            load_time = asyncio.get_event_loop().time() - stage_start
            
            if stream_progress:
                print(f"✅ Loaded {total_rows:,} rows in {load_time:.2f}s")
            
            data_loaded = True
            
        except Exception as e:
            logger.error(f"❌ Data loading failed: {e}")
            raise
        
        # ============================================================
        # STAGE 2: DIVERSITY ANALYSIS
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 2/6: ANALYZING DIVERSITY")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        diversity_result = await self.diversity_analyzer.analyze_diversity(
            dataset_path,
            dataset_format
        )
        
        diversity_time = asyncio.get_event_loop().time() - stage_start
        
        if stream_progress:
            print(f"📊 Diversity Score: {diversity_result['overall_score']:.2f}/100")
            print(f"   - Semantic: {diversity_result['semantic_diversity']:.2f}")
            print(f"   - Statistical: {diversity_result['statistical_diversity']:.2f}")
            print(f"   - Structural: {diversity_result['structural_diversity']:.2f}")
            print(f"⏱️  Completed in {diversity_time:.2f}s")
        
        # Check diversity threshold
        diversity_score = diversity_result['overall_score']
        if diversity_score < self.diversity_threshold:
            logger.warning(f"⚠️ Low diversity: {diversity_score:.1f} < {self.diversity_threshold}")
        
        # ============================================================
        # STAGE 3: CASCADE TRAINING
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 3/6: TRAINING CASCADE MODELS")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        # Convert dataset to tensor format
        if isinstance(dataset, pd.DataFrame):
            # Extract numeric columns
            numeric_cols = dataset.select_dtypes(include=[np.number]).columns
            data_tensor = torch.tensor(dataset[numeric_cols].values, dtype=torch.float32)
        else:
            data_tensor = torch.tensor(dataset, dtype=torch.float32)
        
        # Train cascade
        cascade_result = await self.cascade_trainer.train_cascade(
            data_tensor,
            num_tiers=3,
            models_per_tier=[10, 5, 3]
        )
        
        cascade_time = asyncio.get_event_loop().time() - stage_start
        num_models = sum(cascade_result['models_per_tier'].values())
        
        if stream_progress:
            print(f"🎯 Trained {num_models} models across 3 tiers")
            print(f"   - Tier 1 (tiny): {cascade_result['models_per_tier']['tier_1']} models")
            print(f"   - Tier 2 (small): {cascade_result['models_per_tier']['tier_2']} models")
            print(f"   - Tier 3 (base): {cascade_result['models_per_tier']['tier_3']} models")
            print(f"⏱️  Completed in {cascade_time:.2f}s")
        
        # ============================================================
        # STAGE 4: COLLAPSE DETECTION
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 4/6: DETECTING COLLAPSE")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        # Use reference data if available, otherwise use synthetic predictions
        if reference_data is not None:
            if isinstance(reference_data, pd.DataFrame):
                ref_numeric = reference_data.select_dtypes(include=[np.number]).columns
                reference_tensor = torch.tensor(reference_data[ref_numeric].values, dtype=torch.float32)
            else:
                reference_tensor = torch.tensor(reference_data, dtype=torch.float32)
        else:
            # Use cascade predictions as synthetic data
            reference_tensor = data_tensor
        
        collapse_result = await self.collapse_detector.detect_collapse(
            synthetic_data=data_tensor,
            original_data=reference_tensor
        )
        
        collapse_time = asyncio.get_event_loop().time() - stage_start
        
        if stream_progress:
            print(f"📈 Collapse Score: {collapse_result['overall_score']:.2f}/100")
            print(f"   {'❌ COLLAPSE DETECTED!' if collapse_result['collapse_detected'] else '✅ No collapse detected'}")
            print(f"\n   Dimension Breakdown:")
            for dim, score in collapse_result['dimensions'].items():
                status = "✅" if score >= 70 else "⚠️" if score >= 50 else "❌"
                print(f"   {status} {dim}: {score:.2f}")
            print(f"⏱️  Completed in {collapse_time:.2f}s")
        
        # ============================================================
        # STAGE 5: PROBLEM LOCALIZATION
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 5/6: LOCALIZING PROBLEMS")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        # Only localize if we found issues
        if collapse_result['overall_score'] < self.collapse_threshold:
            localization_result = await self.gradient_localizer.localize_collapse(
                dataset=data_tensor,
                collapse_dimensions=collapse_result['dimensions']
            )
            
            problematic_rows = localization_result['problematic_indices']
            
            if stream_progress:
                print(f"🎯 Identified {len(problematic_rows):,} problematic rows")
                print(f"   - Top issue: {localization_result['top_contributors'][0]}")
                print(f"   - Severity: {localization_result['severity']}")
        else:
            problematic_rows = []
            if stream_progress:
                print(f"✅ No localization needed - quality is good!")
        
        localization_time = asyncio.get_event_loop().time() - stage_start
        if stream_progress:
            print(f"⏱️  Completed in {localization_time:.2f}s")
        
        # ============================================================
        # STAGE 6: RECOMMENDATIONS
        # ============================================================
        if stream_progress:
            print("\n" + "="*60)
            print("STAGE 6/6: GENERATING RECOMMENDATIONS")
            print("="*60)
        
        stage_start = asyncio.get_event_loop().time()
        
        recommendation_result = await self.recommendation_engine.generate_recommendations(
            collapse_score=collapse_result['overall_score'],
            dimension_scores=collapse_result['dimensions'],
            diversity_score=diversity_score
        )
        
        recommendation_time = asyncio.get_event_loop().time() - stage_start
        
        recommendations = recommendation_result['recommendations']
        projected_improvement = recommendation_result['projected_score'] - collapse_result['overall_score']
        
        if stream_progress:
            print(f"💡 Generated {len(recommendations)} recommendations")
            if recommendations:
                print(f"\n   Top 3 Recommendations:")
                for i, rec in enumerate(recommendations[:3], 1):
                    print(f"   {i}. {rec['title']}")
                    print(f"      Impact: +{rec['estimated_impact']:.1f} points | Cost: ${rec['cost_usd']:,.0f}")
            print(f"\n   Projected Improvement: +{projected_improvement:.1f} points")
            print(f"   Projected Score: {recommendation_result['projected_score']:.1f}/100")
            print(f"⏱️  Completed in {recommendation_time:.2f}s")
        
        # ============================================================
        # FINAL DECISION
        # ============================================================
        total_time = asyncio.get_event_loop().time() - start_time
        
        # Decide if dataset is approved for training
        approved = self._make_final_decision(
            collapse_score=collapse_result['overall_score'],
            diversity_score=diversity_score,
            dimension_scores=collapse_result['dimensions']
        )
        
        # Get GPU metrics
        gpu_util = np.mean(self.gpu_metrics) if self.gpu_metrics else 0.0
        gpu_memory = torch.cuda.max_memory_allocated() / 1e9 if torch.cuda.is_available() else 0.0
        
        # Create result object
        result = ValidationResult(
            validation_id=validation_id,
            timestamp=datetime.now(),
            dataset_path=dataset_path,
            dataset_format=dataset_format,
            total_rows=total_rows,
            total_time_seconds=total_time,
            data_loaded=data_loaded,
            load_time_seconds=load_time,
            diversity_score=diversity_score,
            diversity_metrics=diversity_result,
            diversity_time_seconds=diversity_time,
            cascade_trained=True,
            cascade_models=num_models,
            cascade_time_seconds=cascade_time,
            collapse_detected=collapse_result['collapse_detected'],
            collapse_score=collapse_result['overall_score'],
            dimension_scores=collapse_result['dimensions'],
            collapse_time_seconds=collapse_time,
            problematic_rows=problematic_rows,
            localization_time_seconds=localization_time,
            recommendations=recommendations,
            projected_improvement=projected_improvement,
            recommendation_time_seconds=recommendation_time,
            approved_for_training=approved['approved'],
            confidence=approved['confidence'],
            reason=approved['reason'],
            gpu_utilization_avg=gpu_util,
            gpu_memory_used_gb=gpu_memory
        )
        
        # Print final summary
        if stream_progress:
            self._print_final_summary(result)
        
        # Save report if requested
        if output_report_path:
            result.save_report(output_report_path)
        
        return result
    
    def _make_final_decision(
        self,
        collapse_score: float,
        diversity_score: float,
        dimension_scores: Dict[str, float]
    ) -> Dict[str, Any]:
        """
        Make final approval decision based on all metrics.
        
        Decision Logic:
        - APPROVED: collapse_score >= threshold AND diversity >= threshold
        - REJECTED: Any critical dimension < 40 OR overall score < threshold
        - WARNING: Score near threshold (within 10 points)
        """
        reasons = []
        
        # Check collapse score
        if collapse_score < self.collapse_threshold:
            reasons.append(f"Collapse score ({collapse_score:.1f}) below threshold ({self.collapse_threshold})")
        
        # Check diversity
        if diversity_score < self.diversity_threshold:
            reasons.append(f"Diversity score ({diversity_score:.1f}) below threshold ({self.diversity_threshold})")
        
        # Check critical dimensions
        critical_dims = [
            'mode_collapse',
            'spectral_degradation',
            'gradient_pathology'
        ]
        
        for dim in critical_dims:
            score = dimension_scores.get(dim, 100)
            if score < 40:
                reasons.append(f"Critical dimension '{dim}' severely degraded ({score:.1f}/100)")
        
        # Make decision
        approved = len(reasons) == 0
        
        # Calculate confidence
        if approved:
            # High confidence if well above thresholds
            margin = min(
                collapse_score - self.collapse_threshold,
                diversity_score - self.diversity_threshold
            )
            confidence = min(95, 75 + margin)
            reason = "All quality metrics passed thresholds"
        else:
            # Confidence based on how far below threshold
            worst_margin = max(
                self.collapse_threshold - collapse_score,
                self.diversity_threshold - diversity_score,
                0
            )
            confidence = max(50, 90 - worst_margin)
            reason = "; ".join(reasons)
        
        return {
            'approved': approved,
            'confidence': confidence,
            'reason': reason
        }
    
    def _print_final_summary(self, result: ValidationResult):
        """Print beautiful final summary."""
        print("\n" + "="*60)
        print("VALIDATION COMPLETE")
        print("="*60)
        
        # Decision banner
        if result.approved_for_training:
            print("✅ DATASET APPROVED FOR TRAINING")
        else:
            print("❌ DATASET REJECTED - ISSUES FOUND")
        
        print(f"\nConfidence: {result.confidence:.1f}%")
        print(f"Reason: {result.reason}")
        
        # Key metrics
        print(f"\n📊 Key Metrics:")
        print(f"   • Collapse Score: {result.collapse_score:.1f}/100")
        print(f"   • Diversity Score: {result.diversity_score:.1f}/100")
        print(f"   • Problematic Rows: {len(result.problematic_rows):,}")
        print(f"   • Recommendations: {len(result.recommendations)}")
        
        # Performance
        print(f"\n⚡ Performance:")
        print(f"   • Total Time: {result.total_time_seconds:.1f}s")
        print(f"   • Throughput: {result.total_rows/result.total_time_seconds:,.0f} rows/sec")
        print(f"   • GPU Utilization: {result.gpu_utilization_avg:.1f}%")
        print(f"   • GPU Memory: {result.gpu_memory_used_gb:.2f} GB")
        
        # Next steps
        if result.approved_for_training:
            print(f"\n🚀 Next Steps:")
            print(f"   1. Proceed with model training")
            print(f"   2. Monitor training metrics")
            print(f"   3. Validate final model")
        else:
            print(f"\n🔧 Next Steps:")
            print(f"   1. Review recommendations (top 3):")
            for i, rec in enumerate(result.recommendations[:3], 1):
                print(f"      {i}. {rec['title']} (+{rec['estimated_impact']:.1f} pts)")
            print(f"   2. Fix identified issues")
            print(f"   3. Re-run validation")
        
        print("\n" + "="*60)


async def main():
    """Example usage of the orchestrator."""
    
    # Initialize orchestrator
    orchestrator = SynthosOrchestrator(
        gpu_memory_fraction=0.9,
        enable_mixed_precision=True,
        collapse_threshold=65.0,
        diversity_threshold=50.0
    )
    
    # Run validation
    result = await orchestrator.validate(
        dataset_path="data/synthetic_dataset.parquet",
        dataset_format="parquet",
        output_report_path="validation_report.json",
        stream_progress=True
    )
    
    # Check result
    if result.approved_for_training:
        print("\n✅ Dataset approved! Proceeding with training...")
    else:
        print(f"\n❌ Dataset needs work: {result.reason}")
        print(f"💡 {len(result.recommendations)} recommendations available")


if __name__ == "__main__":
    asyncio.run(main())
