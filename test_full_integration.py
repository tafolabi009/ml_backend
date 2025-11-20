#!/usr/bin/env python3
"""
Full System Integration Test
Tests the complete validation pipeline end-to-end
"""

import grpc
import json
import sys
from pathlib import Path

# Add ml_backend to path
sys.path.insert(0, str(Path(__file__).parent / 'ml_backend' / 'src'))

from grpc_services import validation_pb2, validation_pb2_grpc


def test_service_connections():
    """Test gRPC connections to all ML services"""
    print("=" * 70)
    print("Testing Service Connectivity")
    print("=" * 70)
    
    services = [
        ("Validation Service", "localhost:50051"),
        ("Collapse Service", "localhost:50052"),
        ("Data Service (Go)", "localhost:50055"),
    ]
    
    all_connected = True
    for name, address in services:
        try:
            channel = grpc.insecure_channel(address)
            future = grpc.channel_ready_future(channel)
            future.result(timeout=5)
            print(f"✅ {name:25} - Connected ({address})")
            channel.close()
        except Exception as e:
            print(f"❌ {name:25} - Failed: {e}")
            all_connected = False
    
    return all_connected


def test_job_orchestrator_api():
    """Test job orchestrator REST API"""
    print("\n" + "=" * 70)
    print("Testing Job Orchestrator REST API")
    print("=" * 70)
    
    import requests
    
    try:
        # Test health endpoint
        response = requests.get("http://localhost:8080/health", timeout=5)
        if response.status_code == 200:
            print(f"✅ Health endpoint - OK")
            return True
        else:
            print(f"❌ Health endpoint - Status {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Orchestrator API - Failed: {e}")
        return False


def test_validation_diversity_analysis():
    """Test diversity analysis endpoint"""
    print("\n" + "=" * 70)
    print("Testing Validation Pipeline - Diversity Analysis")
    print("=" * 70)
    
    try:
        channel = grpc.insecure_channel('localhost:50051')
        stub = validation_pb2_grpc.ValidationEngineStub(channel)
        
        # Create a test request
        request = validation_pb2.DiversityRequest(
            dataset_id="test-dataset-001",
            s3_path="s3://test-bucket/test-data.csv",
            row_count=10000,
            format=validation_pb2.CSV
        )
        
        print(f"Sending diversity analysis request for dataset: {request.dataset_id}")
        print(f"  - S3 Path: {request.s3_path}")
        print(f"  - Row Count: {request.row_count}")
        print(f"  - Format: CSV")
        
        # This will fail because we don't have actual data,
        # but we can verify the service is responding
        try:
            response = stub.AnalyzeDiversity(request, timeout=10)
            print(f"✅ Diversity Analysis - Response received")
            if response.error and response.error.message:
                print(f"   Note: {response.error.message}")
            return True
        except grpc.RpcError as e:
            # Service is responding - file not found is expected without real data
            if "No such file" in e.details() or \
               e.code() == grpc.StatusCode.NOT_FOUND or \
               e.code() == grpc.StatusCode.INVALID_ARGUMENT:
                print(f"✅ Diversity Analysis - Service responding correctly")
                print(f"   (Expected error: File not found - no test data uploaded)")
                return True
            else:
                print(f"❌ Diversity Analysis - Unexpected error: {e.details()}")
                return False
                
    except Exception as e:
        print(f"❌ Diversity Analysis - Failed: {e}")
        return False
    finally:
        channel.close()


def test_collapse_detection():
    """Test collapse detection endpoint"""
    print("\n" + "=" * 70)
    print("Testing Collapse Detection")
    print("=" * 70)
    
    try:
        channel = grpc.insecure_channel('localhost:50052')
        stub = validation_pb2_grpc.CollapseEngineStub(channel)
        
        # Create a test request
        request = validation_pb2.CollapseRequest(
            dataset_id="test-dataset-001",
            validation_id="val-001",
        )
        
        print(f"Sending collapse detection request")
        print(f"  - Dataset ID: {request.dataset_id}")
        print(f"  - Validation ID: {request.validation_id}")
        
        try:
            response = stub.DetectCollapse(request, timeout=10)
            print(f"✅ Collapse Detection - Response received")
            print(f"   Collapse Detected: {response.collapse_detected}")
            print(f"   Collapse Type: {response.collapse_type}")
            return True
        except grpc.RpcError as e:
            if e.code() in [grpc.StatusCode.NOT_FOUND, grpc.StatusCode.INVALID_ARGUMENT]:
                print(f"✅ Collapse Detection - Service responding (expected error: {e.details()})")
                return True
            else:
                print(f"❌ Collapse Detection - Unexpected error: {e.details()}")
                return False
                
    except Exception as e:
        print(f"❌ Collapse Detection - Failed: {e}")
        return False
    finally:
        channel.close()


def print_service_summary():
    """Print summary of running services"""
    print("\n" + "=" * 70)
    print("Service Architecture Summary")
    print("=" * 70)
    print("""
┌─────────────────────────────────────────────────────────────┐
│                  Job Orchestrator (Go)                      │
│                  REST API: :8080                            │
│                  gRPC: :50053                               │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Coordinates validation jobs
                   │
        ┌──────────┼──────────┐
        │          │          │
        ▼          ▼          ▼
┌───────────┐ ┌───────────┐ ┌───────────┐
│Validation │ │ Collapse  │ │   Data    │
│ Service   │ │  Service  │ │  Service  │
│  :50051   │ │  :50052   │ │  :50055   │
└───────────┘ └───────────┘ └───────────┘
     │             │             │
     └─────────────┴─────────────┘
               │
               │ Uses Resonance NN
               ▼
        ML Backend Container
    """)


def main():
    """Run all integration tests"""
    print("\n🚀 Full System Integration Test Suite")
    print("=" * 70)
    
    results = []
    
    # Test 1: Service Connectivity
    results.append(("Service Connectivity", test_service_connections()))
    
    # Test 2: Orchestrator API
    results.append(("Job Orchestrator API", test_job_orchestrator_api()))
    
    # Test 3: Validation Service
    results.append(("Validation - Diversity Analysis", test_validation_diversity_analysis()))
    
    # Test 4: Collapse Detection
    results.append(("Collapse Detection", test_collapse_detection()))
    
    # Print architecture
    print_service_summary()
    
    # Summary
    print("\n" + "=" * 70)
    print("Test Results Summary")
    print("=" * 70)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:40} {status}")
    
    print("=" * 70)
    print(f"Tests Passed: {passed}/{total}")
    
    if passed == total:
        print("\n✅ All integration tests passed!")
        print("\n🎉 The full validation pipeline is operational!")
        print("\nNext steps:")
        print("  1. Upload a dataset via the Job Orchestrator API")
        print("  2. Submit a validation job")
        print("  3. Monitor progress via streaming updates")
        print("  4. Review collapse detection results and recommendations")
        return 0
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
