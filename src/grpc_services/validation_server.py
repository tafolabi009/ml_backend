"""
gRPC Validation Service with mTLS and Comprehensive Error Handling
Implements the ValidationEngine and CollapseEngine services
"""

import grpc
from grpc import aio
import asyncio
import logging
from concurrent import futures
from typing import AsyncIterator, Optional
from pathlib import Path
import ssl
import traceback
from functools import wraps

# Import generated protobuf code (will be generated from .proto file)
# import validation_pb2
# import validation_pb2_grpc

from ..validation_engine.cascade_trainer import CascadeTrainer, CascadeProgress
from ..collapse_engine.detector import CollapseDetector
from ..data_processors.dataset_loader import DatasetLoader

logger = logging.getLogger(__name__)


# ============================================================================
# Error Handling Decorator
# ============================================================================

class ValidationError(Exception):
    """Base exception for validation errors"""
    def __init__(self, message: str, code: int = 1, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class DataError(ValidationError):
    """Data loading or processing errors"""
    def __init__(self, message: str):
        super().__init__(message, code=1001, retryable=False)


class ModelError(ValidationError):
    """Model training or inference errors"""
    def __init__(self, message: str, retryable: bool = True):
        super().__init__(message, code=2001, retryable=retryable)


class ResourceError(ValidationError):
    """GPU or memory resource errors"""
    def __init__(self, message: str):
        super().__init__(message, code=3001, retryable=True)


class TimeoutError(ValidationError):
    """Operation timeout errors"""
    def __init__(self, message: str):
        super().__init__(message, code=4001, retryable=True)


def handle_errors(func):
    """
    Decorator for comprehensive error handling in gRPC methods.
    Catches all exceptions and converts them to proper gRPC errors.
    """
    @wraps(func)
    async def wrapper(self, request, context):
        try:
            # Set timeout
            context.set_deadline(context.time_remaining() or 3600)  # 1 hour default
            
            # Execute the actual method
            return await func(self, request, context)
            
        except ValidationError as e:
            # Known validation error
            logger.error(f"Validation error in {func.__name__}: {e}")
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            
            # Return error response (if response type supports it)
            error_info = self._create_error_info(
                code=e.code,
                message=str(e),
                details=traceback.format_exc(),
                retryable=e.retryable
            )
            return self._create_error_response(error_info)
            
        except grpc.aio.AioRpcError as e:
            # gRPC-specific error
            logger.error(f"gRPC error in {func.__name__}: {e.details()}")
            context.set_code(e.code())
            context.set_details(e.details())
            raise
            
        except asyncio.TimeoutError:
            # Timeout
            logger.error(f"Timeout in {func.__name__}")
            context.set_code(grpc.StatusCode.DEADLINE_EXCEEDED)
            context.set_details("Operation timed out")
            
            error_info = self._create_error_info(
                code=4001,
                message="Operation timed out",
                details="",
                retryable=True,
                retry_after_seconds=60
            )
            return self._create_error_response(error_info)
            
        except MemoryError:
            # Out of memory
            logger.error(f"OOM error in {func.__name__}")
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details("Out of GPU memory")
            
            error_info = self._create_error_info(
                code=3002,
                message="Out of GPU memory",
                details="Try reducing batch size or using smaller model",
                retryable=True,
                retry_after_seconds=300
            )
            return self._create_error_response(error_info)
            
        except Exception as e:
            # Unexpected error
            logger.error(f"Unexpected error in {func.__name__}: {e}")
            logger.error(traceback.format_exc())
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal server error: {str(e)}")
            
            error_info = self._create_error_info(
                code=5000,
                message="Internal server error",
                details=traceback.format_exc(),
                retryable=False
            )
            return self._create_error_response(error_info)
    
    return wrapper


# ============================================================================
# Validation Engine Service Implementation
# ============================================================================

class ValidationEngineServicer:  # Will inherit from validation_pb2_grpc.ValidationEngineServicer
    """
    Implementation of ValidationEngine gRPC service.
    Handles Phase 2-5 of the validation pipeline.
    """
    
    def __init__(self, config: dict, hardware_config: dict):
        self.config = config
        self.hardware_config = hardware_config
        self.dataset_loader = DatasetLoader()
        logger.info("ValidationEngine service initialized")
    
    @handle_errors
    async def AnalyzeDiversity(self, request, context):
        """
        Phase 2: Analyze dataset diversity and create stratified sample.
        """
        logger.info(f"AnalyzeDiversity called for dataset {request.dataset_id}")
        
        # TODO: Implement diversity analysis
        # 1. Load dataset from S3
        # 2. Perform clustering and diversity analysis
        # 3. Create stratified sample (20M rows)
        # 4. Save sample to S3
        # 5. Return metrics and sample path
        
        # Placeholder response
        return {
            'dataset_id': request.dataset_id,
            'sample_s3_path': f"s3://samples/{request.dataset_id}_sample.parquet",
            'metrics': {
                'entropy': 0.87,
                'gini_coefficient': 0.32,
                'cluster_count': 15
            },
            'sampling_confidence': 94
        }
    
    @handle_errors
    async def PreScreenRisk(self, request, context):
        """
        Phase 3: Pre-screen against collapse signature library.
        """
        logger.info(f"PreScreenRisk called for dataset {request.dataset_id}")
        
        # TODO: Implement pre-screening
        # 1. Extract dataset fingerprint
        # 2. Match against signature library
        # 3. Calculate pre-risk score
        # 4. Determine if should proceed
        
        return {
            'dataset_id': request.dataset_id,
            'pre_risk_score': 18,
            'should_proceed': True,
            'recommendation': 'Low risk detected, safe to proceed'
        }
    
    @handle_errors
    async def TrainCascade(
        self, 
        request, 
        context
    ) -> AsyncIterator:
        """
        Phase 4: Multi-scale cascade training with STREAMING progress.
        Yields progress updates every 10 seconds.
        """
        logger.info(
            f"TrainCascade called for dataset {request.dataset_id}, "
            f"validation {request.validation_id}"
        )
        
        # Progress callback that yields updates
        async def progress_callback(progress: CascadeProgress):
            # Convert to protobuf and yield
            yield {
                'dataset_id': progress.dataset_id,
                'validation_id': progress.validation_id,
                'current_tier': progress.current_tier,
                'current_variant': progress.current_variant,
                'models_completed': progress.models_completed,
                'models_total': progress.models_total,
                'progress_percent': progress.progress_percent,
                'current_loss': progress.current_loss,
                'gpu_utilization': progress.gpu_utilization,
                'estimated_completion': progress.estimated_completion,
                'result': progress.current_model_result if progress.current_model_result else None
            }
        
        # Initialize trainer
        trainer = CascadeTrainer(
            dataset_id=request.dataset_id,
            validation_id=request.validation_id,
            config=self.config,
            hardware_config=self.hardware_config,
            progress_callback=progress_callback
        )
        
        # Load data
        # TODO: Load actual data from S3
        import torch
        train_data = torch.randint(0, 50257, (20_000_000,))
        val_data = torch.randint(0, 50257, (1_000_000,))
        
        # Train cascade (will stream progress every 10s)
        try:
            results = await trainer.train_cascade(
                train_data=train_data,
                val_data=val_data,
                vocab_size=request.config.vocab_size
            )
            
            # Send final completion message
            yield {
                'dataset_id': request.dataset_id,
                'validation_id': request.validation_id,
                'models_completed': trainer.total_models,
                'models_total': trainer.total_models,
                'progress_percent': 100.0
            }
            
        except Exception as e:
            logger.error(f"Cascade training failed: {e}")
            # Yield error progress
            yield {
                'dataset_id': request.dataset_id,
                'validation_id': request.validation_id,
                'error': {
                    'code': 2001,
                    'message': str(e),
                    'details': traceback.format_exc(),
                    'retryable': True
                }
            }
            raise ModelError(f"Cascade training failed: {e}")
    
    @handle_errors
    async def GetPredictions(self, request, context):
        """
        Phase 5: Get final predictions with confidence intervals.
        """
        logger.info(f"GetPredictions called for validation {request.validation_id}")
        
        # TODO: Implement scaling law extrapolation
        # 1. Fit power law to cascade results
        # 2. Extrapolate to target model size
        # 3. Calculate confidence intervals
        # 4. Compute final risk score
        
        return {
            'dataset_id': request.dataset_id,
            'validation_id': request.validation_id,
            'predicted_accuracy': 0.87,
            'confidence': {
                'lower_bound': 0.84,
                'upper_bound': 0.90,
                'confidence_level': 0.95
            },
            'final_risk_score': 23
        }
    
    def _create_error_info(
        self,
        code: int,
        message: str,
        details: str,
        retryable: bool,
        retry_after_seconds: int = 60
    ):
        """Create error info protobuf"""
        return {
            'code': code,
            'message': message,
            'details': details,
            'retryable': retryable,
            'retry_after_seconds': retry_after_seconds if retryable else 0
        }
    
    def _create_error_response(self, error_info):
        """Create error response with error info"""
        return {'error': error_info}


# ============================================================================
# Collapse Engine Service Implementation
# ============================================================================

class CollapseEngineServicer:  # Will inherit from validation_pb2_grpc.CollapseEngineServicer
    """
    Implementation of CollapseEngine gRPC service.
    Handles Phase 5-6 of the validation pipeline.
    """
    
    def __init__(self, config: dict):
        self.config = config
        self.detector = CollapseDetector(config)
        logger.info("CollapseEngine service initialized")
    
    @handle_errors
    async def DetectCollapse(self, request, context):
        """
        Phase 5: Detect collapse in cascade results.
        """
        logger.info(f"DetectCollapse called for validation {request.validation_id}")
        
        # TODO: Implement collapse detection
        # 1. Analyze cascade results
        # 2. Calculate multi-dimensional scores
        # 3. Identify collapse type
        # 4. Determine severity
        
        return {
            'dataset_id': request.dataset_id,
            'validation_id': request.validation_id,
            'collapse_detected': False,
            'severity': 'low',
            'dimensions': [
                {'dimension': 'distribution_fidelity', 'score': 92, 'threshold': 70, 'passed': True},
                {'dimension': 'correlation_preservation', 'score': 88, 'threshold': 70, 'passed': True},
            ]
        }
    
    @handle_errors
    async def LocalizeProblems(self, request, context):
        """
        Phase 6: Pinpoint problematic data regions.
        """
        logger.info(f"LocalizeProblems called for validation {request.validation_id}")
        
        # TODO: Implement gradient-based localization
        # 1. Analyze gradient attributions
        # 2. Identify problematic row ranges
        # 3. Run ablation experiments
        # 4. Confirm hypotheses
        
        return {
            'dataset_id': request.dataset_id,
            'validation_id': request.validation_id,
            'regions': []
        }
    
    @handle_errors
    async def GenerateRecommendations(self, request, context):
        """
        Phase 6: Generate actionable recommendations.
        """
        logger.info(f"GenerateRecommendations called for validation {request.validation_id}")
        
        # TODO: Implement recommendation generation
        # 1. Analyze problematic regions
        # 2. Generate fix strategies
        # 3. Estimate impact
        # 4. Prioritize recommendations
        
        return {
            'dataset_id': request.dataset_id,
            'validation_id': request.validation_id,
            'recommendations': []
        }


# ============================================================================
# mTLS Server Configuration
# ============================================================================

def load_mtls_credentials(
    server_cert_path: Path,
    server_key_path: Path,
    ca_cert_path: Path
) -> grpc.ServerCredentials:
    """
    Load mTLS credentials for secure service-to-service communication.
    
    Args:
        server_cert_path: Path to server certificate
        server_key_path: Path to server private key
        ca_cert_path: Path to CA certificate for client verification
    
    Returns:
        gRPC server credentials with mTLS enabled
    """
    # Read certificates
    with open(server_cert_path, 'rb') as f:
        server_cert = f.read()
    
    with open(server_key_path, 'rb') as f:
        server_key = f.read()
    
    with open(ca_cert_path, 'rb') as f:
        ca_cert = f.read()
    
    # Create server credentials with client verification
    server_credentials = grpc.ssl_server_credentials(
        [(server_key, server_cert)],
        root_certificates=ca_cert,
        require_client_auth=True  # Enforce mTLS
    )
    
    logger.info("mTLS credentials loaded successfully")
    return server_credentials


async def serve(
    port: int = 50051,
    use_mtls: bool = True,
    cert_dir: Path = Path("/etc/synthos/certs"),
    config: dict = None,
    hardware_config: dict = None
):
    """
    Start the gRPC server with mTLS support.
    
    Args:
        port: Port to listen on
        use_mtls: Whether to use mTLS (required in production)
        cert_dir: Directory containing certificates
        config: ML configuration
        hardware_config: Hardware configuration
    """
    server = aio.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.max_receive_message_length', 100 * 1024 * 1024),  # 100MB
            ('grpc.keepalive_time_ms', 30000),  # 30 seconds
            ('grpc.keepalive_timeout_ms', 10000),  # 10 seconds
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.http2.min_time_between_pings_ms', 10000),
            ('grpc.http2.min_ping_interval_without_data_ms', 5000),
        ]
    )
    
    # Add servicers
    validation_servicer = ValidationEngineServicer(config, hardware_config)
    collapse_servicer = CollapseEngineServicer(config)
    
    # TODO: Register servicers with generated protobuf code
    # validation_pb2_grpc.add_ValidationEngineServicer_to_server(
    #     validation_servicer, server
    # )
    # validation_pb2_grpc.add_CollapseEngineServicer_to_server(
    #     collapse_servicer, server
    # )
    
    # Configure server with mTLS
    if use_mtls:
        credentials = load_mtls_credentials(
            server_cert_path=cert_dir / "server.crt",
            server_key_path=cert_dir / "server.key",
            ca_cert_path=cert_dir / "ca.crt"
        )
        server.add_secure_port(f'[::]:{port}', credentials)
        logger.info(f"gRPC server starting with mTLS on port {port}")
    else:
        server.add_insecure_port(f'[::]:{port}')
        logger.warning(f"gRPC server starting WITHOUT mTLS on port {port}")
    
    await server.start()
    logger.info(f"gRPC server started successfully on port {port}")
    
    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info("Shutting down gRPC server...")
        await server.stop(grace=10)


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point for gRPC server"""
    import yaml
    
    # Load configurations
    with open('/workspaces/ml_backend/config/ml_config.yaml') as f:
        config = yaml.safe_load(f)
    
    with open('/workspaces/ml_backend/config/hardware_config.yaml') as f:
        hardware_config = yaml.safe_load(f)
    
    # Start server
    await serve(
        port=50051,
        use_mtls=True,  # Always use mTLS in production
        config=config,
        hardware_config=hardware_config
    )


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(main())
