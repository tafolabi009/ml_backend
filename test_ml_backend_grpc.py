#!/usr/bin/env python3
"""
Test script to verify ml-backend gRPC services are working
"""

import grpc
import sys

def test_service_connection(service_name, port):
    """Test if a gRPC service is accessible"""
    try:
        channel = grpc.insecure_channel(f'localhost:{port}')
        future = grpc.channel_ready_future(channel)
        future.result(timeout=5)
        print(f"✅ {service_name} (port {port}) is ready and accepting connections")
        channel.close()
        return True
    except Exception as e:
        print(f"❌ {service_name} (port {port}) failed: {e}")
        return False

def main():
    """Test all ML backend services"""
    print("=" * 60)
    print("Testing ML Backend gRPC Services")
    print("=" * 60)
    
    services = [
        ("Validation Service", 50051),
        ("Collapse Service", 50052),
    ]
    
    all_passed = True
    for service_name, port in services:
        if not test_service_connection(service_name, port):
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("✅ All services are running successfully!")
        print("\nYou can now:")
        print("  1. Send gRPC requests to the services")
        print("  2. Start the Go backend services (api-gateway, job-orchestrator)")
        print("  3. Test the full validation pipeline")
        return 0
    else:
        print("❌ Some services failed to start")
        return 1

if __name__ == "__main__":
    sys.exit(main())
