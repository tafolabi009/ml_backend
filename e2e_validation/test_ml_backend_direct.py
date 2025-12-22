#!/usr/bin/env python3
"""
Direct ML Backend gRPC Test
Tests the ML backend services directly without going through job orchestrator.
This verifies that the ML backend is properly processing requests.
"""

import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ml_backend', 'src', 'grpc_services'))

import grpc

# Import the generated proto stubs from ML backend
from ml_backend.src.grpc_services import validation_pb2
from ml_backend.src.grpc_services import validation_pb2_grpc

def test_diversity_analysis():
    """Test AnalyzeDiversity RPC call"""
    print("\n" + "="*60)
    print("Testing ML Backend - AnalyzeDiversity")
    print("="*60)
    
    channel = grpc.insecure_channel('localhost:50051')
    
    # Wait for channel to be ready
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        print("✅ Connected to validation service on port 50051")
    except grpc.FutureTimeoutError:
        print("❌ Failed to connect to validation service")
        return False
    
    stub = validation_pb2_grpc.ValidationEngineStub(channel)
    
    # Create request matching the ML backend's proto definition
    request = validation_pb2.DiversityRequest(
        dataset_id="test-dataset-001",
        s3_path="s3://synthos-storage/test-data",
        row_count=10000,
        format=validation_pb2.DataFormat.CSV
    )
    
    print(f"📤 Sending DiversityRequest: dataset_id={request.dataset_id}")
    
    try:
        response = stub.AnalyzeDiversity(request, timeout=30)
        print(f"✅ Got response!")
        print(f"   Dataset ID: {response.dataset_id}")
        print(f"   Sample S3 Path: {response.sample_s3_path}")
        if response.metrics:
            print(f"   Diversity Score: {response.metrics.diversity_score:.3f}")
            print(f"   Coverage Score: {response.metrics.coverage_score:.3f}")
            print(f"   Balance Score: {response.metrics.balance_score:.3f}")
        if response.error and response.error.message:
            print(f"   ⚠️ Error: {response.error.message}")
        return True
    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()} - {e.details()}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_collapse_detection():
    """Test DetectCollapse RPC call"""
    print("\n" + "="*60)
    print("Testing ML Backend - DetectCollapse")
    print("="*60)
    
    channel = grpc.insecure_channel('localhost:50052')
    
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        print("✅ Connected to collapse service on port 50052")
    except grpc.FutureTimeoutError:
        print("❌ Failed to connect to collapse service")
        return False
    
    stub = validation_pb2_grpc.CollapseEngineStub(channel)
    
    # Create request matching the ML backend's proto definition
    # cascade_results is a repeated field of ModelResult
    # tier and variant are int32 fields (0=LIGHT, 1=MEDIUM, 2=HEAVY)
    request = validation_pb2.CollapseRequest(
        dataset_id="test-dataset-001",
        validation_id="validation-001",
        cascade_results=[
            validation_pb2.ModelResult(
                tier=0,  # LIGHT = 0
                variant=0,  # First variant
                model_size=1000000,
                training_loss=0.15,
                validation_loss=0.18,
                training_time_seconds=5.2,
                convergence_epoch=10,
                collapse_detected=False
            )
        ],
        original_diversity=validation_pb2.DiversityMetrics(
            entropy=0.75,
            gini_coefficient=0.30,
            cluster_count=5,
            rare_pattern_percentage=0.05,
            outlier_percentage=0.02
        )
    )
    
    print(f"📤 Sending CollapseRequest: dataset_id={request.dataset_id}")
    
    try:
        response = stub.DetectCollapse(request, timeout=30)
        print(f"✅ Got response!")
        print(f"   Is Collapsed: {response.is_collapsed}")
        print(f"   Severity: {response.severity}")
        print(f"   Collapse Type: {response.collapse_type}")
        if response.dimension_scores:
            for dim in response.dimension_scores:
                print(f"   Dimension {dim.dimension}: score={dim.score:.3f}")
        if response.error and response.error.message:
            print(f"   ⚠️ Error: {response.error.message}")
        return True
    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()} - {e.details()}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_cascade_training():
    """Test TrainCascade RPC call (streaming)"""
    print("\n" + "="*60)
    print("Testing ML Backend - TrainCascade (streaming)")
    print("="*60)
    
    channel = grpc.insecure_channel('localhost:50051')
    
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        print("✅ Connected to validation service on port 50051")
    except grpc.FutureTimeoutError:
        print("❌ Failed to connect to validation service")
        return False
    
    stub = validation_pb2_grpc.ValidationEngineStub(channel)
    
    # Create request matching the ML backend's proto definition
    request = validation_pb2.CascadeRequest(
        dataset_id="test-dataset-001",
        sample_s3_path="s3://synthos-storage/test-data/sample.csv",
        metrics=validation_pb2.DiversityMetrics(
            diversity_score=0.75,
            coverage_score=0.80,
            balance_score=0.70
        ),
        config=validation_pb2.CascadeConfig(
            enabled_tiers=["light"],
            models_per_tier=1,
            max_epochs=5,
            batch_size=32,
            learning_rate=0.001
        )
    )
    
    print(f"📤 Sending CascadeRequest: dataset_id={request.dataset_id}")
    print("📥 Receiving streaming progress updates...")
    
    try:
        progress_count = 0
        for progress in stub.TrainCascade(request, timeout=60):
            progress_count += 1
            print(f"   [{progress_count}] {progress.current_tier}/{progress.current_model}: "
                  f"Epoch {progress.current_epoch}, Loss: {progress.current_loss:.4f}, "
                  f"Accuracy: {progress.current_accuracy:.4f}")
            if progress.is_complete:
                print(f"   ✅ Training complete!")
                break
            if progress_count > 20:  # Safety limit
                print("   ... (truncated)")
                break
        print(f"✅ Received {progress_count} progress updates")
        return True
    except grpc.RpcError as e:
        print(f"❌ RPC Error: {e.code()} - {e.details()}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    print("="*70)
    print("  ML BACKEND DIRECT gRPC TEST")
    print("="*70)
    print("This test validates ML backend gRPC services directly")
    print("bypassing the job orchestrator.\n")
    
    results = []
    
    # Test 1: Diversity Analysis
    results.append(("AnalyzeDiversity", test_diversity_analysis()))
    
    # Test 2: Collapse Detection
    results.append(("DetectCollapse", test_collapse_detection()))
    
    # Test 3: Cascade Training (streaming)
    results.append(("TrainCascade", test_cascade_training()))
    
    # Summary
    print("\n" + "="*70)
    print("  SUMMARY")
    print("="*70)
    
    passed = sum(1 for _, r in results if r)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"  {name}: {status}")
    
    print(f"\n  Total: {passed}/{total} tests passed")
    print("="*70)
    
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
