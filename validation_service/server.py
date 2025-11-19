#!/usr/bin/env python3
"""
Enhanced Validation Service with proper error handling, logging, and persistence
"""

import os
import sys
import logging
import grpc
from concurrent import futures
import asyncio
import torch
from typing import Dict, Any
import json
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(__file__))

import validation_pb2
import validation_pb2_grpc
from validation_engine.cascade_trainer import CascadeTrainer
from validation_engine.diversity_analyzer import DiversityAnalyzer, StratificationConfig

# Structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - trace_id=%(trace_id)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('/var/log/validation-service.log')
    ]
)
logger = logging.getLogger(__name__)

# Global job tracker (in production, use Redis or PostgreSQL)
active_jobs: Dict[str, Any] = {}


class ValidationServiceServicer(validation_pb2_grpc.ValidationServiceServicer):
    """Production-ready gRPC servicer for validation operations"""
    
    def __init__(self):
        self.cascade_trainer = None
        self.diversity_analyzer = None
        self.max_concurrent_jobs = int(os.getenv('MAX_CONCURRENT_JOBS', '5'))
        self.gpu_count = torch.cuda.device_count()
        logger.info(f"ValidationServiceServicer initialized with {self.gpu_count} GPUs")
    
    def _check_capacity(self) -> bool:
        """Check if service has capacity for new jobs"""
        running_jobs = sum(1 for job in active_jobs.values() if job['status'] == 'running')
        return running_jobs < self.max_concurrent_jobs
    
    def TrainCascade(self, request, context):
        """Train cascade of models for validation"""
        job_id = request.job_id
        trace_id = dict(context.invocation_metadata()).get('x-trace-id', 'unknown')
        
        logger.info(f"TrainCascade request received", extra={'trace_id': trace_id, 'job_id': job_id})
        
        try:
            # Check capacity
            if not self._check_capacity():
                context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
                context.set_details("Service at capacity, please retry later")
                return validation_pb2.TrainCascadeResponse(
                    job_id=job_id,
                    status='failed',
                    error_message='Service at capacity'
                )
            
            # Validate request
            if not request.dataset_path:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("dataset_path is required")
                return validation_pb2.TrainCascadeResponse(
                    job_id=job_id,
                    status='failed',
                    error_message='Invalid request: missing dataset_path'
                )
            
            # Initialize cascade trainer if not exists
            if self.cascade_trainer is None:
                self.cascade_trainer = CascadeTrainer(
                    num_gpus=min(request.config.num_gpus if request.config.use_multi_gpu else 1, self.gpu_count)
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
                'stage': 'initializing',
                'trace_id': trace_id,
                'started_at': datetime.utcnow().isoformat()
            }
            
            # Run training
            logger.info(f"Starting cascade training", extra={'trace_id': trace_id, 'dataset': request.dataset_path})
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            results = loop.run_until_complete(
                self.cascade_trainer.train_cascade(
                    data_path=request.dataset_path,
                    data_format=request.data_format or 'parquet',
                    tiers=list(request.config.tiers) if request.config.tiers else ['light', 'medium', 'heavy'],
                    **config
                )
            )
