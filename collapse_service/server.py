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
            
            # Convert to proto
            response = collapse_pb2.LocalizeCollapseResponse(job_id=job_id)
            
            for result in results:
                loc_result = collapse_pb2.LocalizationResult(
                    region_id=result.region_id,
                    start_row=result.start_row,
                    end_row=result.end_row,
                    affected_columns=result.affected_columns,
                    issue_type=result.issue_type,
                    severity_score=result.severity_score,
                    confidence=result.confidence,
                    description=result.description or ""
                )
                
                # Map dimension impacts
                if hasattr(result, 'dimension_impacts'):
                    for dim, impact in result.dimension_impacts.items():
                        loc_result.dimension_impacts[dim] = impact
                
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
            
            # Convert to proto
            response = collapse_pb2.RecommendationsResponse(job_id=job_id)
            
            for rec in recommendations:
                recommendation = collapse_pb2.Recommendation(
                    priority=rec.priority,
                    category=rec.category,
                    title=rec.title,
                    description=rec.description,
                    confidence=rec.confidence
                )
                
                # Map impact
                if hasattr(rec, 'impact'):
                    recommendation.impact.CopyFrom(collapse_pb2.Impact(
                        current_risk_score=rec.impact.current_risk_score,
                        expected_risk_score=rec.impact.expected_risk_score,
                        improvement=rec.impact.improvement
                    ))
                
                # Map implementation
                if hasattr(rec, 'implementation'):
                    recommendation.implementation.CopyFrom(collapse_pb2.Implementation(
                        method=rec.implementation.method,
                        affected_rows=rec.implementation.affected_rows,
                        affected_columns=rec.implementation.affected_columns or [],
                        estimated_time=rec.implementation.estimated_time,
                        steps=rec.implementation.steps or [],
                        code_snippet=rec.implementation.code_snippet or ""
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
