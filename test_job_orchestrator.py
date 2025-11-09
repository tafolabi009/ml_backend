"""
Mock Job Orchestrator Server + Client Test
Creates a mock orchestrator to test client communication
"""

import sys
import os
import grpc
from grpc import aio
import asyncio
import logging
import time
from concurrent import futures

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.grpc_services import job_orchestrator_pb2
from src.grpc_services import job_orchestrator_pb2_grpc
from src.grpc_services.job_orchestrator_client import JobOrchestratorClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# Mock Job Orchestrator Server
# ============================================================================

class MockJobOrchestratorServicer(job_orchestrator_pb2_grpc.JobOrchestratorServicer):
    """Mock Job Orchestrator for testing"""
    
    def __init__(self):
        self.acknowledged_jobs = set()
        self.progress_updates = []
        self.completed_jobs = []
        self.failed_jobs = []
        self.heartbeats = []
    
    def AcknowledgeJob(self, request, context):
        """Handle job acknowledgment"""
        logger.info(f"📨 Received job acknowledgment: {request.job_id} from {request.worker_id}")
        self.acknowledged_jobs.add(request.job_id)
        
        return job_orchestrator_pb2.JobAckResponse(
            success=True,
            message=f"Job {request.job_id} acknowledged"
        )
    
    def UpdateJobProgress(self, request, context):
        """Handle progress update"""
        logger.info(
            f"📊 Progress update for {request.job_id}: "
            f"{request.stage} - {request.progress_percent}% - {request.current_activity}"
        )
        
        self.progress_updates.append({
            'job_id': request.job_id,
            'stage': request.stage,
            'progress': request.progress_percent,
            'activity': request.current_activity
        })
        
        # Simulate cancellation request if progress > 80%
        should_cancel = request.progress_percent > 80
        
        return job_orchestrator_pb2.JobProgressResponse(
            success=True,
            message="Progress recorded",
            should_cancel=should_cancel
        )
    
    def CompleteJob(self, request, context):
        """Handle job completion"""
        logger.info(f"✅ Job completed: {request.job_id}")
        logger.info(f"   Validation ID: {request.validation_id}")
        logger.info(f"   Dataset ID: {request.dataset_id}")
        logger.info(f"   Risk Score: {request.result.risk_score}")
        logger.info(f"   Risk Level: {request.result.risk_level}")
        logger.info(f"   Recommendation: {request.result.recommendation}")
        logger.info(f"   Warranty Eligible: {request.result.warranty_eligible}")
        logger.info(f"   Processing Time: {request.processing_time_seconds}s")
        logger.info(f"   GPU Hours: {request.gpu_hours_used:.2f}h")
        
        self.completed_jobs.append({
            'job_id': request.job_id,
            'validation_id': request.validation_id,
            'risk_score': request.result.risk_score
        })
        
        return job_orchestrator_pb2.JobCompletionResponse(
            success=True,
            message="Job completion accepted",
            result_accepted=True
        )
    
    def FailJob(self, request, context):
        """Handle job failure"""
        logger.error(f"❌ Job failed: {request.job_id}")
        logger.error(f"   Error: {request.error_code} - {request.error_message}")
        logger.error(f"   Retryable: {request.retryable}")
        
        self.failed_jobs.append({
            'job_id': request.job_id,
            'error_code': request.error_code
        })
        
        return job_orchestrator_pb2.JobFailureResponse(
            success=True,
            message="Failure recorded",
            should_retry=request.retryable,
            retry_after_seconds=60 if request.retryable else 0
        )
    
    def Heartbeat(self, request, context):
        """Handle heartbeat"""
        logger.info(f"💓 Heartbeat from {request.worker_id}: {request.status}")
        
        self.heartbeats.append({
            'worker_id': request.worker_id,
            'status': request.status,
            'timestamp': request.timestamp
        })
        
        return job_orchestrator_pb2.HeartbeatResponse(
            success=True,
            should_shutdown=False,
            heartbeat_interval_seconds=30
        )


def run_mock_server_sync(port=50052):
    """Start mock Job Orchestrator server (synchronous)"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = MockJobOrchestratorServicer()
    job_orchestrator_pb2_grpc.add_JobOrchestratorServicer_to_server(servicer, server)
    
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logger.info(f"🚀 Mock Job Orchestrator server started on port {port}")
    
    return server, servicer


# ============================================================================
# Client Tests
# ============================================================================

def test_job_orchestrator_client(port=50052):
    """Test Job Orchestrator client"""
    print("\n" + "=" * 70)
    print("TESTING JOB ORCHESTRATOR CLIENT")
    print("=" * 70)
    
    # Create client
    client = JobOrchestratorClient(
        orchestrator_host="localhost",
        orchestrator_port=port,
        worker_id="test-worker-001"
    )
    print(f"\n✅ Client created: {client.worker_id}")
    
    # Test 1: Acknowledge job
    print("\n📨 Test 1: Acknowledge Job")
    job_id = "job_12345"
    validation_id = "val_test_001"
    
    success = client.acknowledge_job(job_id, validation_id)
    assert success, "Job acknowledgment should succeed"
    print("   ✅ Job acknowledged successfully")
    
    # Test 2: Send progress updates
    print("\n📊 Test 2: Progress Updates")
    stages = [
        ("diversity", 10, "Analyzing dataset diversity"),
        ("prescreening", 25, "Checking signature library"),
        ("cascade", 40, "Training tier 1 models"),
        ("cascade", 60, "Training tier 2 models"),
        ("detection", 75, "Detecting collapse patterns"),
        ("localization", 85, "Localizing problematic regions"),
        ("recommendations", 95, "Generating recommendations")
    ]
    
    for stage, progress, activity in stages:
        success = client.update_progress(
            job_id=job_id,
            validation_id=validation_id,
            stage=stage,
            progress_percent=progress,
            current_activity=activity,
            gpu_utilization={0: 85.5, 1: 72.3},
            memory_usage_gb=24.5,
            metrics={'loss': 0.123, 'accuracy': 0.956}
        )
        assert success, f"Progress update should succeed for {stage}"
        time.sleep(0.1)  # Small delay between updates
    
    print("   ✅ All progress updates sent successfully")
    
    # Test 3: Complete job
    print("\n✅ Test 3: Complete Job")
    
    # Mock validation result (API-compliant format)
    result = {
        'validation_id': validation_id,
        'dataset_id': 'ds_test_001',
        'status': 'completed',
        'created_at': '2025-11-09T12:00:00Z',
        'completed_at': '2025-11-09T12:15:00Z',
        'results': {
            'risk_score': 18,
            'risk_level': 'low',
            'collapse_probability': 0.18,
            'predicted_performance': {
                'accuracy': 0.9234,
                'confidence_interval': [0.9015, 0.9453],
                'confidence_level': 0.95
            },
            'dimensions': {
                'distribution_fidelity': 92,
                'correlation_preservation': 88,
                'diversity_retention': 85,
                'rare_pattern_handling': 78,
                'temporal_stability': 91,
                'semantic_coherence': 89
            },
            'recommendation': 'approved',
            'warranty_eligible': True,
            'recommendations': [
                {
                    'priority': 1,
                    'category': 'data_cleaning',
                    'title': 'Remove duplicate rows',
                    'description': 'Found 12 duplicate rows that should be removed',
                    'estimated_impact': 3
                }
            ]
        }
    }
    
    success = client.complete_job(
        job_id=job_id,
        validation_id=validation_id,
        dataset_id='ds_test_001',
        result=result,
        result_storage_path='gs://bucket/results/val_test_001.json',
        model_checkpoint_path='gs://bucket/checkpoints/val_test_001/',
        processing_time_seconds=900,  # 15 minutes
        gpu_hours_used=0.5
    )
    assert success, "Job completion should succeed"
    print("   ✅ Job completed successfully")
    
    # Test 4: Fail a job
    print("\n❌ Test 4: Fail Job")
    failed_job_id = "job_67890"
    
    success = client.fail_job(
        job_id=failed_job_id,
        validation_id="val_test_002",
        error_code="CUDA_OOM",
        error_message="CUDA out of memory: tried to allocate 2.5 GB",
        error_stack_trace="Traceback (most recent call last):\n  File...",
        retryable=True,
        partial_results_path='gs://bucket/partial/val_test_002.json'
    )
    assert success, "Job failure should be reported successfully"
    print("   ✅ Job failure reported successfully")
    
    # Test 5: Send heartbeat
    print("\n💓 Test 5: Heartbeat")
    
    heartbeat_response = client.heartbeat(
        current_job_id=job_id,
        status="busy",
        available_gpus=[0, 1],
        available_memory_gb=48.0,
        active_job_ids=[job_id]
    )
    
    assert heartbeat_response is not None, "Heartbeat should succeed"
    assert heartbeat_response['success'], "Heartbeat should be successful"
    print(f"   Heartbeat interval: {heartbeat_response['heartbeat_interval']}s")
    print(f"   Should shutdown: {heartbeat_response['should_shutdown']}")
    print("   ✅ Heartbeat successful")
    
    # Close client
    client.close()
    print("\n✅ Client closed")


def main():
    """Run tests"""
    print("\n🧪 Job Orchestrator Integration Test")
    print("=" * 70)
    
    # Start mock server
    port = 50052
    server, servicer = run_mock_server_sync(port)
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Run client tests
        test_job_orchestrator_client(port)
        
        # Print server statistics
        print("\n" + "=" * 70)
        print("SERVER STATISTICS")
        print("=" * 70)
        print(f"   Acknowledged jobs: {len(servicer.acknowledged_jobs)}")
        print(f"   Progress updates: {len(servicer.progress_updates)}")
        print(f"   Completed jobs: {len(servicer.completed_jobs)}")
        print(f"   Failed jobs: {len(servicer.failed_jobs)}")
        print(f"   Heartbeats: {len(servicer.heartbeats)}")
        
        # Show some details
        if servicer.progress_updates:
            print(f"\n   Sample progress updates:")
            for update in servicer.progress_updates[:3]:
                print(f"      - {update['stage']}: {update['progress']}% - {update['activity']}")
        
        if servicer.completed_jobs:
            print(f"\n   Completed jobs:")
            for job in servicer.completed_jobs:
                print(f"      - {job['validation_id']}: risk_score={job['risk_score']}")
        
        print("\n" + "=" * 70)
        print("✅ ALL TESTS PASSED")
        print("=" * 70)
        print("\n📝 Integration test successful!")
        print("   The Job Orchestrator client can now communicate with")
        print("   the Go Job Orchestrator using the same protocol.")
        
    finally:
        # Stop server
        server.stop(grace=5)
        print("\n🛑 Mock server stopped")


if __name__ == '__main__':
    main()
