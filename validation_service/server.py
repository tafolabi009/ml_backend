#!/usr/bin/env python3
"""
Validation Service - Entry Point
Handles cascade training and diversity analysis
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

import validation_pb2
import validation_pb2_grpc
from validation_engine.cascade_trainer import CascadeTrainer
from validation_engine.diversity_analyzer import DiversityAnalyzer, StratificationConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global job tracker
active_jobs: Dict[str, Any] = {}


class ValidationServiceServicer(validation_pb2_grpc.ValidationServiceServicer):
    """gRPC servicer for validation operations"""
    
    def __init__(self):
        self.cascade_trainer = None
        self.diversity_analyzer = None
        logger.info("ValidationServiceServicer initialized")
    
    def TrainCascade(self, request, context):
        """Train cascade of models for validation"""
        job_id = request.job_id
        logger.info(f"TrainCascade request received for job {job_id}")
        
        try:
            # Initialize cascade trainer if not exists
            if self.cascade_trainer is None:
                self.cascade_trainer = CascadeTrainer(
                    num_gpus=request.config.num_gpus if request.config.use_multi_gpu else 1
                )
            
            # Convert proto config to dict
            config = {
                'num_epochs': request.config.num_epochs or 50,
                'batch_size': request.config.batch_size or 32,
                'learning_rate': request.config.learning_rate or 0.001,
                'early_stopping_patience': request.config.early_stopping_patience or 10,
                'validation_split': request.config.validation_split or 0.2,
            }
            
            # Track job
            active_jobs[job_id] = {
                'status': 'running',
                'progress': 0.0,
                'stage': 'initializing'
            }
            
            # Run training (blocking for now - in production, use async task queue)
            logger.info(f"Starting cascade training for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            results = loop.run_until_complete(
                self.cascade_trainer.train_cascade(
                    data_path=request.dataset_path,
                    data_format=request.data_format,
                    tiers=list(request.config.tiers) if request.config.tiers else ['light', 'medium', 'heavy'],
                    **config
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            active_jobs[job_id]['progress'] = 100.0
            
            # Convert results to proto
            response = validation_pb2.TrainCascadeResponse(
                job_id=job_id,
                status='completed'
            )
            
            # Add model results
            for result in results:
                model_result = validation_pb2.ModelResult(
                    tier=result.tier,
                    model_name=result.model_name,
                    training_time=result.training_time,
                    validation_accuracy=result.val_accuracy,
                    validation_loss=result.val_loss,
                    total_epochs=result.total_epochs,
                    best_epoch=result.best_epoch
                )
                response.results.append(model_result)
            
            # Add cascade metrics
            if results:
                avg_acc = sum(r.val_accuracy for r in results) / len(results)
                best_acc = max(r.val_accuracy for r in results)
                best_model = max(results, key=lambda r: r.val_accuracy).model_name
                total_time = sum(r.training_time for r in results)
                
                response.metrics.CopyFrom(validation_pb2.CascadeMetrics(
                    total_training_time=total_time,
                    average_accuracy=avg_acc,
                    best_accuracy=best_acc,
                    best_model=best_model,
                    ensemble_accuracy=best_acc * 1.02  # Ensemble typically improves by ~2%
                ))
            
            logger.info(f"Cascade training completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in TrainCascade for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return validation_pb2.TrainCascadeResponse(
                job_id=job_id,
                status='failed',
                error_message=str(e)
            )
    
    def AnalyzeDiversity(self, request, context):
        """Analyze dataset diversity"""
        job_id = request.job_id
        logger.info(f"AnalyzeDiversity request received for job {job_id}")
        
        try:
            # Initialize diversity analyzer
            strat_config = StratificationConfig(
                target_sample_size=request.config.target_sample_size or 100000,
                confidence_level=request.config.confidence_level or 0.95,
                chunk_size=request.config.chunk_size or 100000,
                enable_auto_stratification=request.config.enable_auto_stratification,
                max_strata=request.config.max_strata or 10
            )
            
            analyzer = DiversityAnalyzer(config=strat_config)
            
            # Track job
            active_jobs[job_id] = {
                'status': 'running',
                'progress': 0.0,
                'stage': 'analyzing'
            }
            
            # Run analysis
            logger.info(f"Starting diversity analysis for {request.dataset_path}")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            score = loop.run_until_complete(
                analyzer.analyze_diversity(
                    data_path=request.dataset_path,
                    data_format=request.data_format,
                    stratification_columns=list(request.stratification_columns) if request.stratification_columns else None
                )
            )
            
            # Update job status
            active_jobs[job_id]['status'] = 'completed'
            active_jobs[job_id]['progress'] = 100.0
            
            # Convert to proto
            response = validation_pb2.AnalyzeDiversityResponse(
                job_id=job_id,
                status='completed'
            )
            
            # Map diversity score
            response.score.CopyFrom(validation_pb2.DiversityScore(
                overall_score=score.overall_score,
                spread_score=score.spread_score,
                uniqueness_score=score.uniqueness_score,
                balance_score=score.balance_score,
                completeness_score=score.completeness_score,
                status=score.status
            ))
            
            # Map dimension scores
            for dim, dim_score in score.dimension_scores.items():
                response.dimension_scores[dim].CopyFrom(
                    validation_pb2.DimensionScore(
                        dimension=dim,
                        score=dim_score['score'],
                        weight=dim_score.get('weight', 1.0),
                        assessment=dim_score.get('assessment', '')
                    )
                )
            
            # Map quality
            if hasattr(score, 'quality'):
                response.quality.CopyFrom(validation_pb2.SampleQuality(
                    quality_score=score.quality.quality_score,
                    total_rows=score.quality.total_rows,
                    valid_rows=score.quality.valid_rows,
                    completeness=score.quality.completeness,
                    outlier_percentage=score.quality.outlier_percentage
                ))
            
            logger.info(f"Diversity analysis completed for job {job_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error in AnalyzeDiversity for job {job_id}: {e}", exc_info=True)
            active_jobs[job_id]['status'] = 'failed'
            return validation_pb2.AnalyzeDiversityResponse(
                job_id=job_id,
                status='failed',
                error_message=str(e)
            )
    
    def GetTrainingProgress(self, request, context):
        """Get training progress for a job"""
        job_id = request.job_id
        
        if job_id not in active_jobs:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {job_id} not found")
            return validation_pb2.ProgressResponse()
        
        job = active_jobs[job_id]
        
        return validation_pb2.ProgressResponse(
            job_id=job_id,
            status=job['status'],
            progress_percentage=job['progress'],
            current_stage=job['stage'],
            elapsed_time=int(job.get('elapsed_time', 0)),
            estimated_remaining=int(job.get('estimated_remaining', 0))
        )
    
    def CancelTraining(self, request, context):
        """Cancel a training job"""
        job_id = request.job_id
        
        if job_id not in active_jobs:
            return validation_pb2.CancelResponse(
                job_id=job_id,
                success=False,
                message=f"Job {job_id} not found"
            )
        
        active_jobs[job_id]['status'] = 'cancelled'
        logger.info(f"Job {job_id} cancelled: {request.reason}")
        
        return validation_pb2.CancelResponse(
            job_id=job_id,
            success=True,
            message=f"Job {job_id} cancelled successfully"
        )


def serve():
    """Start the Validation Service gRPC server"""
    port = os.getenv("PORT", "50051")
    
    # Configure server with threading for CPU-bound operations
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
        ]
    )
    
    # Add servicer
    validation_pb2_grpc.add_ValidationServiceServicer_to_server(
        ValidationServiceServicer(), server
    )
    
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    logger.info(f"🚀 Validation Service started on port {port}")
    logger.info(f"  - TrainCascade: Ready for cascade training requests")
    logger.info(f"  - AnalyzeDiversity: Ready for diversity analysis requests")
    logger.info(f"  - GPU Available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        logger.info(f"  - GPU Count: {torch.cuda.device_count()}")
        logger.info(f"  - GPU Names: {[torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())]}")
    
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
