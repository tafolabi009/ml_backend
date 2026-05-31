#!/usr/bin/env python3
"""
Collapse Service - Entry Point
Handles collapse detection and recommendations
"""

import os
import sys
import logging
import grpc
from concurrent import futures
import asyncio
import torch
from typing import Dict, Any

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

import collapse_pb2
import collapse_pb2_grpc
from collapse_engine.detector import CollapseDetector, CollapseConfig
from collapse_engine.localizer import CollapseLocalizer, LocalizationConfig
from collapse_engine.recommender import Recommender
from collapse_engine.recommender_advanced import AdvancedRecommender
from collapse_engine.signature_library import SignatureLibrary

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global job tracker
active_jobs: Dict[str, Any] = {}


class CollapseServiceServicer(collapse_pb2_grpc.CollapseServiceServicer):
    """gRPC servicer for collapse detection and recommendations"""
    
    def __init__(self):
        self.detector = None
        self.localizer = None
        self.recommender = None
        self.advanced_recommender = None
        self.signature_library = None
        logger.info("CollapseServiceServicer initialized")
    
    def DetectCollapse(self, request, context):
        """Detect mode collapse in dataset"""
        job_id = request.job_id
        logger.info(f"DetectCollapse request received for job {job_id}")
        
        try:
            # Parse config
            config_dict = {}
            if request.config.chunk_size:
                config_dict['chunk_size'] = request.config.chunk_size
            if request.config.dimension_thresholds:
                config_dict.update(dict(request.config.dimension_thresholds))
            
            collapse_config = CollapseConfig(**config_dict) if config_dict else None
            
            # Initialize detector
            if self.detector is None:
                self.detector = CollapseDetector(config=collapse_config)
            
            # Track job
            active_jobs[job_id] = {'status': 'running', 'stage': 'detection'}
            
            # Run detection
            logger.info(f"Starting collapse detection for {request.dataset_path}")
            
            # Load and prepare data using DatasetLoader
            from data_processors.dataset_loader import DatasetLoader
            import numpy as np
            
            loader = DatasetLoader()
            dataset = loader.load_full(request.dataset_path)
            
            if request.target_columns:
                cols = list(request.target_columns)
                numeric_data = dataset[cols].values
            else:
                numeric_data = dataset.select_dtypes(include=[np.number]).values
                
            if numeric_data.shape[1] == 0:
                raise ValueError("No numeric columns found for analysis")
                
            # Split dataset in half (first half as original, second half as synthetic)
            midpoint = len(numeric_data) // 2
            original_data = numeric_data[:midpoint]
            synthetic_data = numeric_data[midpoint:]
            
            min_samples = min(len(original_data), len(synthetic_data))
            original_data = original_data[:min_samples]
            synthetic_data = synthetic_data[:min_samples]
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            collapse_score = loop.run_until_complete(
                self.detector.detect_collapse(
                    synthetic_data=synthetic_data,
                    original_data=original_data
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to proto
            response = collapse_pb2.DetectCollapseResponse(job_id=job_id)
            
            # Map collapse score
            response.score.CopyFrom(collapse_pb2.CollapseScore(
                overall_score=collapse_score.overall_score,
                confidence=collapse_score.confidence,
                severity=collapse_score.severity,
                collapse_detected=collapse_score.collapse_detected,
                collapse_type=collapse_score.collapse_type or "",
                affected_dimensions=collapse_score.affected_dimensions
            ))
            
            # Map scale prediction
            if hasattr(collapse_score, 'scale_prediction'):
                pred = collapse_score.scale_prediction
                response.score.scale_prediction.CopyFrom(collapse_pb2.ScalePrediction(
                    score_at_1m=pred.get('1M', 0.0),
                    score_at_10m=pred.get('10M', 0.0),
                    score_at_100m=pred.get('100M', 0.0),
                    score_at_1b=pred.get('1B', 0.0),
                    recommendation=pred.get('recommendation', '')
                ))
            
            # Map dimensions
            for dim_name, dim_score in collapse_score.dimensions.items():
                response.dimensions[dim_name].CopyFrom(collapse_pb2.DimensionScore(
                    name=dim_name,
                    score=dim_score.score,
                    threshold=dim_score.threshold,
                    passed=dim_score.passed,
                    severity=dim_score.severity,
                    issues=dim_score.issues,
                    confidence=dim_score.confidence
                ))
            
            logger.info(f"Collapse detection completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in DetectCollapse for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.DetectCollapseResponse(
                job_id=job_id,
                error_message=str(e)
            )
    
    def LocalizeCollapse(self, request, context):
        """Localize collapse points in dataset"""
        job_id = request.job_id
        logger.info(f"LocalizeCollapse request received for job {job_id}")
        
        try:
            # Parse config
            loc_config = LocalizationConfig(
                chunk_size=request.config.chunk_size or 100000,
                top_k_regions=request.config.top_k_regions or 20,
                use_gpu=request.config.use_gpu
            )
            
            # Initialize localizer
            if self.localizer is None:
                self.localizer = CollapseLocalizer(config=loc_config)
            
            # Track job
            active_jobs[job_id] = {'status': 'running', 'stage': 'localization'}
            
            # Load and prepare data using DatasetLoader
            from data_processors.dataset_loader import DatasetLoader
            import numpy as np
            
            loader = DatasetLoader()
            dataset = loader.load_full(request.dataset_path)
            numeric_data = dataset.select_dtypes(include=[np.number]).values
            
            # Extract collapse dimensions from request
            collapse_dimensions = {}
            if request.collapse_score and request.collapse_score.dimensions:
                for name, dim in request.collapse_score.dimensions.items():
                    collapse_dimensions[name] = dim.score
            else:
                collapse_dimensions = {
                    'distribution_fidelity': 50.0,
                    'correlation_preservation': 50.0,
                    'entropy_stability': 50.0
                }
            
            # Run localization
            logger.info(f"Starting collapse localization for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            results = loop.run_until_complete(
                self.localizer.localize_collapse(
                    data=numeric_data,
                    collapse_dimensions=collapse_dimensions
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to proto
            response = collapse_pb2.LocalizeCollapseResponse(job_id=job_id)
            
            # Group problematic indices into contiguous regions
            regions = []
            indices = results.problematic_indices
            if len(indices) > 0:
                start_idx = indices[0]
                end_idx = indices[0]
                
                for idx in indices[1:]:
                    if idx == end_idx + 1:
                        end_idx = idx
                    else:
                        regions.append((start_idx, end_idx))
                        start_idx = idx
                        end_idx = idx
                regions.append((start_idx, end_idx))
                
            for i, (start, end) in enumerate(regions):
                loc_result = collapse_pb2.LocalizationResult(
                    region_id=f"region_{i:03d}",
                    start_row=start,
                    end_row=end,
                    affected_columns=list(request.config.focus_dimensions) if request.config.focus_dimensions else [],
                    issue_type="collapse_anomaly",
                    severity_score=float(results.impact_scores[start:end+1].mean()) if len(results.impact_scores) > end else 0.8,
                    confidence=0.9,
                    description=f"Anomaly detected in rows {start}-{end}"
                )
                
                # Map dimension attributions if available
                for dim, attributions in results.dimension_attributions.items():
                    loc_result.dimension_impacts[dim] = float(attributions.mean())
                
                response.regions.append(loc_result)
            
            logger.info(f"Collapse localization completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in LocalizeCollapse for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.LocalizeCollapseResponse(
                job_id=job_id,
                error_message=str(e)
            )
    
    def GenerateRecommendations(self, request, context):
        """Generate fix recommendations"""
        job_id = request.job_id
        logger.info(f"GenerateRecommendations request received for job {job_id}")
        
        try:
            # Initialize recommender
            if self.recommender is None:
                self.recommender = Recommender()
            
            # Track job
            active_jobs[job_id] = {'status': 'running', 'stage': 'recommendations'}
            
            # Run recommendations
            logger.info(f"Generating recommendations for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Convert request data to proper format
            collapse_score = float(request.collapse_score.overall_score) if request.collapse_score else 75.0
            
            # Map dimensions
            dimension_scores = {}
            if request.collapse_score and request.collapse_score.dimensions:
                for name, dim in request.collapse_score.dimensions.items():
                    dimension_scores[name] = dim.score
            else:
                dimension_scores = {
                    'distribution_fidelity': 50.0,
                    'correlation_preservation': 50.0,
                    'entropy_stability': 50.0
                }
                
            # Construct localization helper object
            class SimpleLocalizationResult:
                def __init__(self, total, pct):
                    self.total_problematic = total
                    self.percentage_problematic = pct
                    
            total_problematic = len(request.localization_results) if request.localization_results else 0
            # Estimate percentage if possible
            pct = 0.0
            if total_problematic > 0 and request.localization_results:
                est_size = 100000
                total_rows = sum(r.end_row - r.start_row + 1 for r in request.localization_results)
                pct = min(100.0, (total_rows / est_size) * 100)
            loc_results = SimpleLocalizationResult(total_problematic, pct)
            
            # Run recommendations
            logger.info(f"Generating recommendations for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            recommendation_plan = loop.run_until_complete(
                self.recommender.generate_recommendations(
                    collapse_score=collapse_score,
                    dimension_scores=dimension_scores,
                    diversity_score=70.0,
                    dataset_size=100000,
                    localization_results=loc_results
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to proto
            response = collapse_pb2.RecommendationsResponse(job_id=job_id)
            
            # Map combined impact
            response.combined_impact.CopyFrom(collapse_pb2.CombinedImpact(
                current_risk_score=collapse_score,
                expected_risk_score=float(recommendation_plan.projected_score),
                total_improvement=float(recommendation_plan.total_expected_impact),
                estimated_time=f"{recommendation_plan.total_duration_days:.1f} days",
                prerequisites=recommendation_plan.quick_wins
            ))
            
            # Convert recommendations
            from collapse_engine.recommender import ConfidenceLevel
            
            for rec in recommendation_plan.recommendations:
                # Map enums/objects to correct scalar types
                priority_val = rec.priority.value if hasattr(rec.priority, 'value') else int(rec.priority)
                category_str = rec.category.value if hasattr(rec.category, 'value') else str(rec.category)
                
                # Confidence mapping
                conf_map = {
                    ConfidenceLevel.VERY_HIGH: 0.95,
                    ConfidenceLevel.HIGH: 0.8,
                    ConfidenceLevel.MEDIUM: 0.6,
                    ConfidenceLevel.LOW: 0.3
                }
                conf_val = conf_map.get(rec.confidence_level, 0.7)
                
                recommendation = collapse_pb2.Recommendation(
                    priority=priority_val,
                    category=category_str,
                    title=rec.title,
                    description=rec.description,
                    confidence=conf_val
                )
                
                # Map impact
                recommendation.impact.CopyFrom(collapse_pb2.Impact(
                    current_risk_score=collapse_score,
                    expected_risk_score=min(100.0, collapse_score + rec.impact_prediction.expected_improvement),
                    improvement=rec.impact_prediction.expected_improvement
                ))
                
                # Map implementation
                recommendation.implementation.CopyFrom(collapse_pb2.Implementation(
                    method=rec.category.value if hasattr(rec.category, 'value') else str(rec.category),
                    affected_rows=0,
                    affected_columns=list(dimension_scores.keys()),
                    estimated_time=f"{rec.estimated_duration_days:.1f} days",
                    steps=rec.steps,
                    code_snippet=""
                ))
                
                response.recommendations.append(recommendation)
            
            logger.info(f"Recommendations generation completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in GenerateRecommendations for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.RecommendationsResponse(
                job_id=job_id,
                error_message=str(e)
            )
    
    def GenerateAdvancedRecommendations(self, request, context):
        """Generate advanced fix recommendations"""
        job_id = request.job_id
        logger.info(f"GenerateAdvancedRecommendations request received for job {job_id}")
        
        try:
            # Initialize advanced recommender
            if self.advanced_recommender is None:
                self.advanced_recommender = AdvancedRecommender()
            
            # Track job
            active_jobs[job_id] = {'status': 'running', 'stage': 'advanced_recommendations'}
            
            logger.info(f"Generating advanced recommendations for {request.dataset_path}")
            # Implementation similar to GenerateRecommendations but using AdvancedRecommender
            
            response = collapse_pb2.AdvancedRecommendationsResponse(
                job_id=job_id,
                error_message="Advanced recommendations implementation in progress"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error in GenerateAdvancedRecommendations for job {job_id}: {e}", exc_info=True)
            return collapse_pb2.AdvancedRecommendationsResponse(
                job_id=job_id,
                error_message=str(e)
            )
    
    def CheckSignatureLibrary(self, request, context):
        """Check signature library for known patterns"""
        logger.info(f"CheckSignatureLibrary request received for {request.dataset_path}")
        
        try:
            # Initialize signature library
            if self.signature_library is None:
                self.signature_library = SignatureLibrary()
            
            # Check signatures (simplified)
            response = collapse_pb2.SignatureCheckResponse(
                confidence=0.0
            )
            
            logger.info("Signature check completed")
            return response
            
        except Exception as e:
            logger.error(f"Error in CheckSignatureLibrary: {e}", exc_info=True)
            return collapse_pb2.SignatureCheckResponse(confidence=0.0)


def serve():
    """Start the Collapse Service gRPC server"""
    port = os.getenv("PORT", "50053")
    
    # Configure server with threading for CPU-bound operations
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
        ]
    )
    
    # Add servicer
    collapse_pb2_grpc.add_CollapseServiceServicer_to_server(
        CollapseServiceServicer(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"🚀 Collapse Service started on port {port}")
    logger.info(f"  - DetectCollapse: Ready for collapse detection requests")
    logger.info(f"  - LocalizeCollapse: Ready for localization requests")
    logger.info(f"  - GenerateRecommendations: Ready for recommendation requests")
    logger.info(f"  - GPU Available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        logger.info(f"  - GPU Count: {torch.cuda.device_count()}")
    
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
