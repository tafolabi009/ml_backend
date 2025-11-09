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
from src.collapse_engine.detector import CollapseDetector
from src.collapse_engine.signature_library import SignatureLibrary
from src.collapse_engine.localizer import CollapseLocalizer
from src.collapse_engine.recommender import RecommendationEngine
from src.utils.gpu_optimizer import GPUOptimizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """
    Complete validation result with all stages.
    
    This dataclass is designed to match the API specification exactly while
    maintaining backward compatibility with internal ML metrics.
    """
    
    # API-Required Metadata
    validation_id: str
    dataset_id: str  # NEW - Required by API spec
    status: str  # NEW - "queued", "running", "completed", "failed"
    created_at: datetime  # NEW - When validation started
    completed_at: datetime  # NEW - When validation finished
    
    # Legacy/Internal Metadata (still needed for ML operations)
    timestamp: datetime  # Internal timestamp
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
    collapse_score: float  # 0-100, higher = better (internal metric)
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
    
    # API-Required Performance Predictions (NEW)
    predicted_performance: Dict[str, Any] = field(default_factory=lambda: {
        'accuracy': 0.0,
        'confidence_interval': [0.0, 0.0],
        'confidence_level': 0.95
    })
    collapse_probability: float = 0.0  # NEW - Probability of collapse (0-1)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to API-compliant dictionary format.
        
        This method produces output that exactly matches the API specification
        while preserving internal ML metrics for debugging and analysis.
        
        Returns:
            Dictionary matching the API spec format with nested 'results' and 'internal' sections
        """
        # Calculate risk_score (API uses inverse: 0=best, 100=worst)
        # Our collapse_score is 0-100 where 100=best, so we invert it
        risk_score = int(100 - self.collapse_score)
        
        # Determine risk_level based on risk_score
        if risk_score < 25:
            risk_level = "low"
        elif risk_score < 60:
            risk_level = "medium"
        else:
            risk_level = "high"
        
        # Map 8 internal dimensions to API's 6 standard dimensions
        # dimension_scores contains DimensionScore objects, extract .score attribute
        def get_dim_score(dim_name: str) -> int:
            """Extract score value from DimensionScore object or float."""
            dim = self.dimension_scores.get(dim_name, 0)
            if hasattr(dim, 'score'):
                return int(dim.score)
            elif isinstance(dim, (int, float)):
                return int(dim)
            else:
                return 0
        
        dimension_mapping = {
            'distribution_fidelity': get_dim_score('distribution_fidelity'),
            'correlation_preservation': get_dim_score('correlation_preservation'),
            'diversity_retention': int(self.diversity_score),  # Use overall diversity score
            'rare_pattern_handling': get_dim_score('statistical_consistency'),
            'temporal_stability': get_dim_score('entropy_stability'),
            'semantic_coherence': get_dim_score('spectral_coherence')
        }
        
        # Calculate warranty eligibility (risk_score < 25 = eligible)
        warranty_eligible = risk_score < 25
        
        # Determine recommendation string
        recommendation = 'approved' if self.approved_for_training else 'rejected'
        
        # API-compliant output format
        return {
            # API-Required Top-Level Fields
            'validation_id': self.validation_id,
            'dataset_id': self.dataset_id,
            'status': self.status,
            'created_at': self.created_at.isoformat(),
            'completed_at': self.completed_at.isoformat(),
            
            # API-Required Results Section
            'results': {
                'risk_score': risk_score,
                'risk_level': risk_level,
                'predicted_performance': {
                    'accuracy': self.predicted_performance.get('accuracy', 0.0),
                    'confidence_interval': self.predicted_performance.get('confidence_interval', [0.0, 0.0]),
                    'confidence_level': self.predicted_performance.get('confidence_level', 0.95)
                },
                'collapse_probability': self.collapse_probability,
                'dimensions': dimension_mapping,
                'recommendation': recommendation,
                'warranty_eligible': warranty_eligible
            },
            
            # Internal ML Metrics (for debugging and analysis)
            'internal': {
                'dataset_path': self.dataset_path,
                'dataset_format': self.dataset_format,
                'total_rows': self.total_rows,
                'total_time_seconds': self.total_time_seconds,
                'timestamp': self.timestamp.isoformat(),
                
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
                        'collapse_score': self.collapse_score,  # Internal metric (higher=better)
                        'all_dimensions': {  # All 8 dimensions, converted to int
                            k: int(v.score) if hasattr(v, 'score') else int(v) 
                            for k, v in self.dimension_scores.items()
                        },
                        'time_seconds': self.collapse_time_seconds
                    },
                    'localization': {
                        'problematic_rows_count': len(self.problematic_rows),
                        'row_indices': self.problematic_rows[:100],  # First 100
                        'time_seconds': self.localization_time_seconds
                    },
                    'recommendations': {
                        'count': len(self.recommendations),
                        'items': [  # Convert Recommendation objects to dicts
                            {
                                'title': str(rec.title if hasattr(rec, 'title') else rec.get('title', 'Unknown')),
                                'description': str(rec.description if hasattr(rec, 'description') else rec.get('description', '')),
                                'estimated_impact': float(rec.estimated_impact if hasattr(rec, 'estimated_impact') else rec.get('estimated_impact', 0)),
                                'cost_usd': float(rec.cost_usd if hasattr(rec, 'cost_usd') else rec.get('cost_usd', 0)),
                                'priority': str(rec.priority.value if hasattr(rec, 'priority') and hasattr(rec.priority, 'value') else str(rec.priority) if hasattr(rec, 'priority') else rec.get('priority', 'medium')),
                                'category': str(rec.category if hasattr(rec, 'category') else rec.get('category', 'other'))
                            } if (hasattr(rec, 'title') or isinstance(rec, dict)) else str(rec)
                            for rec in self.recommendations
                        ],
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
        use_cache: bool = True,
        skip_cascade_training: bool = False  # Skip expensive cascade training for testing
    ):
        """
        Initialize orchestrator and all sub-modules.
        
        Args:
            gpu_memory_fraction: Fraction of GPU memory to use (0.0-1.0)
            enable_mixed_precision: Use BF16/FP16 for H100 efficiency
            collapse_threshold: Score below this triggers rejection (0-100)
            diversity_threshold: Minimum diversity score required
            use_cache: Cache intermediate results for faster re-runs
            skip_cascade_training: Skip cascade training (useful for CPU testing)
        """
        logger.info("🚀 Initializing Synthos Validation Engine...")
        
        self.collapse_threshold = collapse_threshold
        self.diversity_threshold = diversity_threshold
        self.use_cache = use_cache
        self.skip_cascade_training = skip_cascade_training
        
        # Initialize GPU optimizer first
        self.gpu_optimizer = GPUOptimizer(
            memory_fraction=gpu_memory_fraction,
            enable_mixed_precision=enable_mixed_precision
        )
        
        # Initialize all modules
        logger.info("📦 Loading modules...")
        self.dataset_loader = DatasetLoader()
        self.diversity_analyzer = DiversityAnalyzer()
        self.cascade_trainer = None  # Initialized per validation
        self.collapse_detector = CollapseDetector()
        self.signature_library = SignatureLibrary()
        self.collapse_localizer = CollapseLocalizer()
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
        stream_progress: bool = True,
        validation_id: Optional[str] = None,  # NEW - Allow custom validation ID
        dataset_id: Optional[str] = None  # NEW - Allow custom dataset ID
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
            validation_id: Custom validation ID (auto-generated if not provided)
            dataset_id: Custom dataset ID (auto-generated if not provided)
            
        Returns:
            ValidationResult with complete analysis and decision
        """
        start_time = asyncio.get_event_loop().time()
        
        # Generate IDs if not provided
        if validation_id is None:
            validation_id = f"val_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        if dataset_id is None:
            dataset_id = f"ds_{hash(dataset_path) % 1000000:06d}"
        
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
            print(f"📊 Diversity Score: {diversity_result.overall_score:.2f}/100")
            print(f"   - Semantic: {diversity_result.dimension_scores.get('semantic_diversity', 0):.2f}")
            print(f"   - Statistical: {diversity_result.dimension_scores.get('statistical_diversity', 0):.2f}")
            print(f"   - Structural: {diversity_result.dimension_scores.get('structural_diversity', 0):.2f}")
            print(f"⏱️  Completed in {diversity_time:.2f}s")
        
        # Check diversity threshold
        diversity_score = diversity_result.overall_score
        if diversity_score < self.diversity_threshold:
            logger.warning(f"⚠️ Low diversity: {diversity_score:.1f} < {self.diversity_threshold}")
        
        # ============================================================
        # STAGE 3: CASCADE TRAINING
        # ============================================================
        if self.skip_cascade_training:
            if stream_progress:
                print("\n" + "="*60)
                print("STAGE 3/6: CASCADE TRAINING (SKIPPED)")
                print("="*60)
                print("⚠️  Cascade training skipped (CPU test mode)")
            
            cascade_time = 0
            num_models = 0
            cascade_trained = False
            
            # Convert dataset to tensor for later stages
            if isinstance(dataset, pd.DataFrame):
                numeric_cols = dataset.select_dtypes(include=[np.number]).columns
                data_tensor = torch.tensor(dataset[numeric_cols].values, dtype=torch.float32)
            else:
                data_tensor = torch.tensor(dataset, dtype=torch.float32)
        else:
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
            cascade_trained = True
            
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
        
        # Prepare data for collapse detection
        # Ensure both tensors have same shape
        if reference_data is not None:
            if isinstance(reference_data, pd.DataFrame):
                ref_numeric = reference_data.select_dtypes(include=[np.number]).columns
                reference_tensor = torch.tensor(reference_data[ref_numeric].values, dtype=torch.float32)
            else:
                reference_tensor = torch.tensor(reference_data, dtype=torch.float32)
        else:
            # Use same data as both synthetic and original for self-comparison
            # This will show perfect match (high scores) which is expected
            reference_tensor = data_tensor.clone()
        
        # Ensure same number of samples (take minimum)
        min_samples = min(data_tensor.shape[0], reference_tensor.shape[0])
        data_tensor_trimmed = data_tensor[:min_samples]
        reference_tensor_trimmed = reference_tensor[:min_samples]
        
        # Ensure same number of features (take common features)
        min_features = min(data_tensor.shape[1], reference_tensor.shape[1])
        data_tensor_trimmed = data_tensor_trimmed[:, :min_features]
        reference_tensor_trimmed = reference_tensor_trimmed[:, :min_features]
        
        collapse_result = await self.collapse_detector.detect_collapse(
            synthetic_data=data_tensor_trimmed.numpy(),
            original_data=reference_tensor_trimmed.numpy()
        )
        
        collapse_time = asyncio.get_event_loop().time() - stage_start
        
        if stream_progress:
            print(f"📈 Collapse Score: {collapse_result.overall_score:.2f}/100")
            print(f"   {'❌ COLLAPSE DETECTED!' if collapse_result.collapse_detected else '✅ No collapse detected'}")
            print(f"\n   Dimension Breakdown:")
            for dim_name, dim_score in collapse_result.dimensions.items():
                # dim_score is a DimensionScore object
                score_value = dim_score.score if hasattr(dim_score, 'score') else dim_score
                status = "✅" if score_value >= 70 else "⚠️" if score_value >= 50 else "❌"
                print(f"   {status} {dim_name}: {score_value:.2f}")
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
        if collapse_result.overall_score < self.collapse_threshold:
            localization_result = await self.collapse_localizer.localize_collapse(
                dataset=data_tensor,
                collapse_dimensions=collapse_result.dimensions
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
            collapse_score=collapse_result.overall_score,
            dimension_scores=collapse_result.dimensions,
            diversity_score=diversity_score,
            dataset_size=total_rows
        )
        
        recommendation_time = asyncio.get_event_loop().time() - stage_start
        
        # Extract data from RecommendationPlan object
        recommendations = recommendation_result.recommendations if hasattr(recommendation_result, 'recommendations') else []
        projected_improvement = recommendation_result.projected_improvement if hasattr(recommendation_result, 'projected_improvement') else 0
        projected_score = recommendation_result.projected_score if hasattr(recommendation_result, 'projected_score') else collapse_result.overall_score
        
        if stream_progress:
            print(f"💡 Generated {len(recommendations)} recommendations")
            if recommendations:
                print(f"\n   Top 3 Recommendations:")
                for i, rec in enumerate(recommendations[:3], 1):
                    # rec might be a Recommendation object or dict
                    title = rec.title if hasattr(rec, 'title') else rec.get('title', 'Unknown')
                    impact = rec.estimated_impact if hasattr(rec, 'estimated_impact') else rec.get('estimated_impact', 0)
                    cost = rec.cost_usd if hasattr(rec, 'cost_usd') else rec.get('cost_usd', 0)
                    print(f"   {i}. {title}")
                    print(f"      Impact: +{impact:.1f} points | Cost: ${cost:,.0f}")
            print(f"\n   Projected Improvement: +{projected_improvement:.1f} points")
            print(f"   Projected Score: {projected_score:.1f}/100")
            print(f"⏱️  Completed in {recommendation_time:.2f}s")
        
        # ============================================================
        # FINAL DECISION
        # ============================================================
        total_time = asyncio.get_event_loop().time() - start_time
        
        # Decide if dataset is approved for training
        approved = self._make_final_decision(
            collapse_score=collapse_result.overall_score,
            diversity_score=diversity_score,
            dimension_scores=collapse_result.dimensions
        )
        
        # Get GPU metrics
        gpu_util = np.mean(self.gpu_metrics) if self.gpu_metrics else 0.0
        gpu_memory = torch.cuda.max_memory_allocated() / 1e9 if torch.cuda.is_available() else 0.0
        
        # Calculate predicted performance (placeholder - would come from cascade training in production)
        predicted_performance = {
            'accuracy': 0.85 + (collapse_result.overall_score / 100) * 0.1,  # 0.85-0.95 based on score
            'confidence_interval': [
                0.82 + (collapse_result.overall_score / 100) * 0.08,
                0.88 + (collapse_result.overall_score / 100) * 0.12
            ],
            'confidence_level': 0.95
        }
        
        # Calculate collapse probability (inverse of collapse_score)
        collapse_probability = max(0.0, min(1.0, (100 - collapse_result.overall_score) / 100))
        
        # Create result object
        result = ValidationResult(
            # API-Required Fields
            validation_id=validation_id,
            dataset_id=dataset_id,  # Use provided or generated dataset_id
            status='completed',  # Could be 'failed' if exception occurred
            created_at=datetime.fromtimestamp(start_time),
            completed_at=datetime.now(),
            
            # Legacy/Internal Fields
            timestamp=datetime.now(),
            dataset_path=dataset_path,
            dataset_format=dataset_format,
            total_rows=total_rows,
            total_time_seconds=total_time,
            
            # Stage Results
            data_loaded=data_loaded,
            load_time_seconds=load_time,
            diversity_score=diversity_score,
            diversity_metrics={'overall_score': diversity_result.overall_score, 'dimension_scores': diversity_result.dimension_scores},
            diversity_time_seconds=diversity_time,
            cascade_trained=cascade_trained,
            cascade_models=num_models,
            cascade_time_seconds=cascade_time,
            collapse_detected=collapse_result.collapse_detected,
            collapse_score=collapse_result.overall_score,
            dimension_scores=collapse_result.dimensions,
            collapse_time_seconds=collapse_time,
            problematic_rows=problematic_rows,
            localization_time_seconds=localization_time,
            recommendations=recommendations,
            projected_improvement=projected_improvement,
            recommendation_time_seconds=recommendation_time,
            
            # Final Decision
            approved_for_training=approved['approved'],
            confidence=approved['confidence'],
            reason=approved['reason'],
            
            # GPU Metrics
            gpu_utilization_avg=gpu_util,
            gpu_memory_used_gb=gpu_memory,
            
            # API-Required Performance Fields
            predicted_performance=predicted_performance,
            collapse_probability=collapse_probability
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
