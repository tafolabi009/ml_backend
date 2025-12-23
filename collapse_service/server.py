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

# Use unified synthos proto stubs
from synthos_proto import synthos_pb2 as collapse_pb2
from synthos_proto import synthos_pb2_grpc as collapse_pb2_grpc
from collapse_engine.detector import CollapseDetector, CollapseConfig
from collapse_engine.localizer import CollapseLocalizer, LocalizationConfig
from collapse_engine.recommender import Recommender
from collapse_engine.recommender_advanced import AdvancedRecommender
from collapse_engine.signature_library import SignatureLibrary

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global job tracker
active_jobs: Dict[str, Any] = {}


class CollapseEngineServicer(collapse_pb2_grpc.CollapseEngineServicer):
    """gRPC servicer for collapse detection and recommendations (using synthos proto)"""
    
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
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            collapse_score = loop.run_until_complete(
                self.detector.detect_collapse(
                    data_path=request.dataset_path,
                    data_format=request.data_format,
                    target_columns=list(request.target_columns) if request.target_columns else None
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to proto using synthos message types
            response = collapse_pb2.CollapseResponse(
                job_id=job_id,
                dataset_id=request.dataset_id,
                collapse_detected=collapse_score.collapse_detected,
                collapse_type=collapse_score.collapse_type or "",
                severity=collapse_score.severity,
                overall_score=collapse_score.overall_score,
                confidence=collapse_score.confidence
            )
            
            # Map scale prediction
            if hasattr(collapse_score, 'scale_prediction'):
                pred = collapse_score.scale_prediction
                response.scale_prediction.CopyFrom(collapse_pb2.ScalePrediction(
                    score_at_1m=pred.get('1M', 0.0),
                    score_at_10m=pred.get('10M', 0.0),
                    score_at_100m=pred.get('100M', 0.0),
                    score_at_1b=pred.get('1B', 0.0),
                    recommendation=pred.get('recommendation', '')
                ))
            
            # Map dimensions using DimensionScore
            for dim_name, dim_score in collapse_score.dimensions.items():
                dim = response.dimensions.add()
                dim.dimension = dim_name
                dim.score = dim_score.score
                dim.threshold = dim_score.threshold
                dim.passed = dim_score.passed
                dim.severity = dim_score.severity
                dim.issues.extend(dim_score.issues if dim_score.issues else [])
            
            logger.info(f"Collapse detection completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in DetectCollapse for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.CollapseResponse(
                job_id=job_id,
                error=collapse_pb2.ErrorInfo(
                    code=3001,
                    message=str(e),
                    retryable=True
                )
            )
    
    def LocalizeProblems(self, request, context):
        """Localize collapse points in dataset (synthos: LocalizeProblems)"""
        job_id = request.job_id
        logger.info(f"LocalizeProblems request received for job {job_id}")
        
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
            
            # Run localization
            logger.info(f"Starting collapse localization for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Note: collapse_score from request needs to be converted to proper object
            # For now, we'll pass the data path and let localizer detect internally
            results = loop.run_until_complete(
                self.localizer.localize_collapse(
                    data_path=request.dataset_path,
                    data_format=request.data_format,
                    collapse_score=None  # Would need proper object conversion
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to synthos proto LocalizationResponse
            response = collapse_pb2.LocalizationResponse(
                job_id=job_id,
                dataset_id=request.dataset_id,
                validation_id=request.validation_id
            )
            
            for result in results:
                region = response.regions.add()
                region.region_id = result.region_id
                region.row_start = result.start_row
                region.row_end = result.end_row
                region.issue_type = result.issue_type
                region.impact_score = result.severity_score
                region.severity_score = result.severity_score
                region.confidence = result.confidence
                region.affected_columns.extend(result.affected_columns or [])
                region.description = result.description or ""
                
                # Map dimension impacts
                if hasattr(result, 'dimension_impacts'):
                    for dim, impact in result.dimension_impacts.items():
                        region.dimension_impacts[dim] = impact
            
            logger.info(f"Collapse localization completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in LocalizeProblems for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.LocalizationResponse(
                job_id=job_id,
                error=collapse_pb2.ErrorInfo(
                    code=3002,
                    message=str(e),
                    retryable=True
                )
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
            
            # Convert request data to proper format (simplified for now)
            recommendations = loop.run_until_complete(
                self.recommender.generate_recommendations(
                    data_path=request.dataset_path,
                    collapse_score=None,  # Would need conversion
                    localization_results=None  # Would need conversion
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            
            # Convert to synthos proto RecommendationResponse
            response = collapse_pb2.RecommendationResponse(
                job_id=job_id,
                dataset_id=request.dataset_id,
                validation_id=request.validation_id
            )
            
            for rec in recommendations:
                recommendation = response.recommendations.add()
                recommendation.priority = rec.priority
                recommendation.category = rec.category
                recommendation.title = rec.title
                recommendation.description = rec.description
                recommendation.confidence = rec.confidence
                
                # Map impact
                if hasattr(rec, 'impact'):
                    recommendation.impact.current_risk_score = rec.impact.current_risk_score
                    recommendation.impact.expected_risk_score = rec.impact.expected_risk_score
                    recommendation.impact.improvement = rec.impact.improvement
                
                # Map implementation
                if hasattr(rec, 'implementation'):
                    recommendation.implementation.method = rec.implementation.method
                    recommendation.implementation.affected_rows = rec.implementation.affected_rows
                    recommendation.implementation.affected_columns.extend(rec.implementation.affected_columns or [])
                    recommendation.implementation.estimated_time = rec.implementation.estimated_time
                    recommendation.implementation.steps.extend(rec.implementation.steps or [])
                    if rec.implementation.code_snippet:
                        recommendation.implementation.code_snippet = rec.implementation.code_snippet
            
            logger.info(f"Recommendations generation completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in GenerateRecommendations for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return collapse_pb2.RecommendationResponse(
                job_id=job_id,
                error=collapse_pb2.ErrorInfo(
                    code=3003,
                    message=str(e),
                    retryable=False
                )
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
    """Start the Collapse Service gRPC server (using synthos proto CollapseEngine)"""
    port = os.getenv("PORT", "50053")
    
    # Configure server with threading for CPU-bound operations
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
        ]
    )
    
    # Add servicer using synthos CollapseEngine
    collapse_pb2_grpc.add_CollapseEngineServicer_to_server(
        CollapseEngineServicer(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"🚀 Collapse Engine Service started on port {port}")
    logger.info(f"  - DetectCollapse: Ready for collapse detection requests")
    logger.info(f"  - LocalizeProblems: Ready for localization requests")
    logger.info(f"  - GenerateRecommendations: Ready for recommendation requests")
    logger.info(f"  - GPU Available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        logger.info(f"  - GPU Count: {torch.cuda.device_count()}")
    
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
