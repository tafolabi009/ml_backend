"""
Job Orchestrator Client
Communicates with the Go Job Orchestrator to report validation progress and results
"""

import grpc
import time
import logging
import socket
from typing import Optional, Dict, Any, List
from datetime import datetime
import psutil

try:
    from . import job_orchestrator_pb2
    from . import job_orchestrator_pb2_grpc
except ImportError:
    import job_orchestrator_pb2
    import job_orchestrator_pb2_grpc

logger = logging.getLogger(__name__)


class JobOrchestratorClient:
    """
    Client for communicating with the Job Orchestrator.
    
    Responsibilities:
    - Acknowledge jobs when picked up
    - Stream progress updates during validation
    - Report completion with results
    - Report failures
    - Send heartbeats to indicate worker is alive
    """
    
    def __init__(
        self,
        orchestrator_host: str = "localhost",
        orchestrator_port: int = 50052,
        worker_id: Optional[str] = None,
        use_tls: bool = False,
        cert_path: Optional[str] = None
    ):
        """
        Initialize Job Orchestrator client.
        
        Args:
            orchestrator_host: Hostname/IP of Job Orchestrator
            orchestrator_port: Port of Job Orchestrator gRPC service
            worker_id: Unique worker identifier (auto-generated if None)
            use_tls: Whether to use TLS
            cert_path: Path to TLS certificates
        """
        self.orchestrator_host = orchestrator_host
        self.orchestrator_port = orchestrator_port
        
        # Generate worker ID if not provided
        if worker_id is None:
            hostname = socket.gethostname()
            pid = os.getpid()
            self.worker_id = f"worker-{hostname}-{pid}"
        else:
            self.worker_id = worker_id
        
        # Create channel
        server_address = f"{orchestrator_host}:{orchestrator_port}"
        
        if use_tls and cert_path:
            with open(cert_path, 'rb') as f:
                credentials = grpc.ssl_channel_credentials(f.read())
            self.channel = grpc.secure_channel(server_address, credentials)
        else:
            self.channel = grpc.insecure_channel(server_address)
        
        self.stub = job_orchestrator_pb2_grpc.JobOrchestratorStub(self.channel)
        
        logger.info(f"Job Orchestrator client initialized: {self.worker_id} -> {server_address}")
    
    def acknowledge_job(
        self,
        job_id: str,
        validation_id: str
    ) -> bool:
        """
        Acknowledge that a job has been picked up for processing.
        
        Args:
            job_id: Job identifier from orchestrator
            validation_id: Validation identifier
            
        Returns:
            True if acknowledgment was successful
        """
        try:
            request = job_orchestrator_pb2.JobAck(
                job_id=job_id,
                worker_id=self.worker_id,
                validation_id=validation_id,
                timestamp=int(time.time())
            )
            
            response = self.stub.AcknowledgeJob(request, timeout=10)
            
            if response.success:
                logger.info(f"Job {job_id} acknowledged successfully")
            else:
                logger.error(f"Job acknowledgment failed: {response.message}")
            
            return response.success
            
        except grpc.RpcError as e:
            logger.error(f"Failed to acknowledge job {job_id}: {e}")
            return False
    
    def update_progress(
        self,
        job_id: str,
        validation_id: str,
        stage: str,
        progress_percent: int,
        current_activity: str,
        gpu_utilization: Optional[Dict[int, float]] = None,
        memory_usage_gb: Optional[float] = None,
        estimated_completion_timestamp: Optional[int] = None,
        metrics: Optional[Dict[str, float]] = None
    ) -> bool:
        """
        Send progress update to orchestrator.
        
        Args:
            job_id: Job identifier
            validation_id: Validation identifier
            stage: Current stage ("diversity", "cascade", "detection", etc.)
            progress_percent: Overall progress (0-100)
            current_activity: Human-readable description of current activity
            gpu_utilization: GPU utilization per GPU (ID -> %)
            memory_usage_gb: Memory usage in GB
            estimated_completion_timestamp: Unix timestamp for estimated completion
            metrics: Real-time metrics (loss, accuracy, etc.)
            
        Returns:
            True if update was successful
        """
        try:
            request = job_orchestrator_pb2.JobProgress(
                job_id=job_id,
                worker_id=self.worker_id,
                validation_id=validation_id,
                stage=stage,
                progress_percent=progress_percent,
                current_activity=current_activity,
                timestamp=int(time.time())
            )
            
            if gpu_utilization:
                request.gpu_utilization.update(gpu_utilization)
            
            if memory_usage_gb:
                request.memory_usage_gb = memory_usage_gb
            
            if estimated_completion_timestamp:
                request.estimated_completion_timestamp = estimated_completion_timestamp
            
            if metrics:
                request.metrics.update(metrics)
            
            response = self.stub.UpdateJobProgress(request, timeout=10)
            
            if response.should_cancel:
                logger.warning(f"Orchestrator requested cancellation of job {job_id}")
            
            return response.success
            
        except grpc.RpcError as e:
            logger.error(f"Failed to update progress for job {job_id}: {e}")
            return False
    
    def complete_job(
        self,
        job_id: str,
        validation_id: str,
        dataset_id: str,
        result: Dict[str, Any],
        result_storage_path: str,
        model_checkpoint_path: Optional[str] = None,
        processing_time_seconds: Optional[int] = None,
        gpu_hours_used: Optional[float] = None
    ) -> bool:
        """
        Report job completion with results.
        
        Args:
            job_id: Job identifier
            validation_id: Validation identifier
            dataset_id: Dataset identifier
            result: Validation result dictionary (API-compliant format)
            result_storage_path: Where full results are stored (GCS/S3 path)
            model_checkpoint_path: Where model checkpoints are stored
            processing_time_seconds: Total processing time
            gpu_hours_used: Total GPU hours consumed
            
        Returns:
            True if completion was accepted
        """
        try:
            # Convert result dict to protobuf
            validation_result = self._dict_to_validation_result(result)
            
            request = job_orchestrator_pb2.JobCompletion(
                job_id=job_id,
                worker_id=self.worker_id,
                validation_id=validation_id,
                dataset_id=dataset_id,
                result=validation_result,
                result_storage_path=result_storage_path,
                model_checkpoint_path=model_checkpoint_path or "",
                processing_time_seconds=processing_time_seconds or 0,
                gpu_hours_used=gpu_hours_used or 0.0,
                timestamp=int(time.time())
            )
            
            response = self.stub.CompleteJob(request, timeout=30)
            
            if response.success and response.result_accepted:
                logger.info(f"Job {job_id} completed and accepted")
            else:
                logger.error(f"Job completion not accepted: {response.message}")
            
            return response.success and response.result_accepted
            
        except grpc.RpcError as e:
            logger.error(f"Failed to complete job {job_id}: {e}")
            return False
    
    def fail_job(
        self,
        job_id: str,
        validation_id: str,
        error_code: str,
        error_message: str,
        error_stack_trace: str,
        retryable: bool = True,
        partial_results_path: Optional[str] = None
    ) -> bool:
        """
        Report job failure.
        
        Args:
            job_id: Job identifier
            validation_id: Validation identifier
            error_code: Error code (e.g., "CUDA_OOM", "INVALID_DATA")
            error_message: Human-readable error message
            error_stack_trace: Full stack trace
            retryable: Whether the job can be retried
            partial_results_path: Path to any partial results
            
        Returns:
            True if failure was reported successfully
        """
        try:
            request = job_orchestrator_pb2.JobFailure(
                job_id=job_id,
                worker_id=self.worker_id,
                validation_id=validation_id,
                error_code=error_code,
                error_message=error_message,
                error_stack_trace=error_stack_trace,
                retryable=retryable,
                partial_results_path=partial_results_path or "",
                timestamp=int(time.time())
            )
            
            response = self.stub.FailJob(request, timeout=10)
            
            if response.should_retry:
                logger.info(f"Orchestrator requested retry for job {job_id} after {response.retry_after_seconds}s")
            
            return response.success
            
        except grpc.RpcError as e:
            logger.error(f"Failed to report job failure for {job_id}: {e}")
            return False
    
    def heartbeat(
        self,
        current_job_id: Optional[str] = None,
        status: str = "idle",
        available_gpus: Optional[List[int]] = None,
        available_memory_gb: Optional[float] = None,
        active_job_ids: Optional[List[str]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Send heartbeat to indicate worker is alive.
        
        Args:
            current_job_id: Currently processing job (None if idle)
            status: Worker status ("idle", "busy", "error")
            available_gpus: List of available GPU IDs
            available_memory_gb: Available memory in GB
            active_job_ids: List of all active job IDs
            
        Returns:
            Response dict with orchestrator commands, or None if failed
        """
        try:
            # Get system info
            if available_memory_gb is None:
                memory = psutil.virtual_memory()
                available_memory_gb = memory.available / (1024**3)
            
            request = job_orchestrator_pb2.HeartbeatRequest(
                worker_id=self.worker_id,
                current_job_id=current_job_id or "",
                status=status,
                available_memory_gb=available_memory_gb,
                timestamp=int(time.time())
            )
            
            if available_gpus:
                request.available_gpus.extend(available_gpus)
            
            if active_job_ids:
                request.active_job_ids.extend(active_job_ids)
            
            response = self.stub.Heartbeat(request, timeout=5)
            
            return {
                'success': response.success,
                'jobs_to_cancel': list(response.jobs_to_cancel),
                'should_shutdown': response.should_shutdown,
                'heartbeat_interval': response.heartbeat_interval_seconds
            }
            
        except grpc.RpcError as e:
            logger.error(f"Heartbeat failed: {e}")
            return None
    
    def close(self):
        """Close the gRPC channel"""
        self.channel.close()
        logger.info(f"Job Orchestrator client closed: {self.worker_id}")
    
    def _dict_to_validation_result(self, result: Dict[str, Any]) -> job_orchestrator_pb2.ValidationResult:
        """Convert validation result dict to protobuf message"""
        
        # Extract results section
        results_section = result.get('results', {})
        
        # Convert predicted performance
        pred_perf_dict = results_section.get('predicted_performance', {})
        pred_perf = job_orchestrator_pb2.PredictedPerformance(
            accuracy=pred_perf_dict.get('accuracy', 0.0),
            confidence_interval=pred_perf_dict.get('confidence_interval', [0.0, 0.0]),
            confidence_level=pred_perf_dict.get('confidence_level', 0.95)
        )
        
        # Convert recommendations list
        recommendations = []
        for rec in results_section.get('recommendations', []):
            recommendations.append(
                job_orchestrator_pb2.Recommendation(
                    priority=rec.get('priority', 3),
                    category=rec.get('category', ''),
                    title=rec.get('title', ''),
                    description=rec.get('description', ''),
                    estimated_improvement=rec.get('estimated_impact', 0)
                )
            )
        
        # Create protobuf message
        validation_result = job_orchestrator_pb2.ValidationResult(
            validation_id=result.get('validation_id', ''),
            dataset_id=result.get('dataset_id', ''),
            status=result.get('status', 'completed'),
            created_at=result.get('created_at', ''),
            completed_at=result.get('completed_at', ''),
            risk_score=results_section.get('risk_score', 0),
            risk_level=results_section.get('risk_level', 'low'),
            collapse_probability=results_section.get('collapse_probability', 0.0),
            predicted_performance=pred_perf,
            recommendation=results_section.get('recommendation', 'approved'),
            warranty_eligible=results_section.get('warranty_eligible', False)
        )
        
        # Add dimensions
        dimensions = results_section.get('dimensions', {})
        validation_result.dimensions.update(dimensions)
        
        # Add recommendations
        validation_result.recommendations_list.extend(recommendations)
        
        return validation_result


# Import os for worker ID generation
import os
