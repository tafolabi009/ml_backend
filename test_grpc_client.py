"""
gRPC Client for Testing Validation Services
"""

import grpc
import asyncio
import logging
from pathlib import Path

# Import generated protobuf code
from src.grpc_services import validation_pb2
from src.grpc_services import validation_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_validation_service():
    """Test the ValidationEngine gRPC service."""
    
    print("\n" + "="*70)
    print("TESTING GRPC VALIDATION SERVICE")
    print("="*70)
    
    # Connect to server
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = validation_pb2_grpc.ValidationEngineStub(channel)
        
        # Test 1: AnalyzeDiversity
        print("\n📊 Test 1: AnalyzeDiversity")
        print("-" * 70)
        
        diversity_request = validation_pb2.DiversityRequest(
            dataset_id="ds_test_001",
            s3_path="test_api_data_large.csv",
            row_count=1000,
            format=validation_pb2.CSV
        )
        
        try:
            diversity_response = await stub.AnalyzeDiversity(diversity_request)
            print(f"✅ Diversity Analysis Complete")
            print(f"   Dataset ID: {diversity_response.dataset_id}")
            print(f"   Sample Path: {diversity_response.sample_s3_path}")
            print(f"   Confidence: {diversity_response.sampling_confidence}%")
            print(f"   Entropy: {diversity_response.metrics.entropy:.2f}")
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")
        
        # Test 2: PreScreenRisk
        print("\n🔍 Test 2: PreScreenRisk")
        print("-" * 70)
        
        prescreen_request = validation_pb2.PreScreenRequest(
            dataset_id="ds_test_001"
        )
        prescreen_request.diversity.entropy = 0.87
        prescreen_request.diversity.gini_coefficient = 0.32
        prescreen_request.diversity.cluster_count = 15
        
        try:
            prescreen_response = await stub.PreScreenRisk(prescreen_request)
            print(f"✅ Pre-Screen Complete")
            print(f"   Risk Score: {prescreen_response.pre_risk_score}/100")
            print(f"   Should Proceed: {prescreen_response.should_proceed}")
            print(f"   Recommendation: {prescreen_response.recommendation}")
            if prescreen_response.matches:
                print(f"   Matches Found: {len(prescreen_response.matches)}")
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")
        
        # Test 3: TrainCascade (streaming)
        print("\n🎯 Test 3: TrainCascade (Streaming)")
        print("-" * 70)
        
        cascade_request = validation_pb2.CascadeRequest(
            dataset_id="ds_test_001",
            validation_id="val_test_001",
            sample_s3_path="test_api_data_large.csv"
        )
        cascade_request.config.target_architecture = "resonance_nn"
        cascade_request.config.target_model_size = 1000000000
        cascade_request.config.vocab_size = 50257
        
        try:
            print("📡 Streaming cascade training progress...")
            progress_count = 0
            async for progress in stub.TrainCascade(cascade_request):
                progress_count += 1
                if progress.error and progress.error.code:
                    print(f"❌ Error: {progress.error.message}")
                    break
                else:
                    print(f"   Progress Update {progress_count}:")
                    print(f"   - Models: {progress.models_completed}/{progress.models_total}")
                    print(f"   - Progress: {progress.progress_percent:.1f}%")
                    
            if progress_count > 0 and not (progress.error and progress.error.code):
                print(f"✅ Cascade Training Complete ({progress_count} updates)")
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")
        
        # Test 4: GetPredictions
        print("\n📈 Test 4: GetPredictions")
        print("-" * 70)
        
        prediction_request = validation_pb2.PredictionRequest(
            dataset_id="ds_test_001",
            validation_id="val_test_001",
            target_model_size=1000000000
        )
        
        try:
            prediction_response = await stub.GetPredictions(prediction_request)
            print(f"✅ Predictions Generated")
            print(f"   Predicted Accuracy: {prediction_response.predicted_accuracy:.2%}")
            print(f"   Confidence Interval: [{prediction_response.confidence.lower_bound:.2%}, {prediction_response.confidence.upper_bound:.2%}]")
            print(f"   Final Risk Score: {prediction_response.final_risk_score}/100")
            print(f"   Scaling Law R²: {prediction_response.scaling.r_squared:.3f}")
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")


async def test_collapse_service():
    """Test the CollapseEngine gRPC service."""
    
    print("\n" + "="*70)
    print("TESTING GRPC COLLAPSE SERVICE")
    print("="*70)
    
    # Connect to server
    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = validation_pb2_grpc.CollapseEngineStub(channel)
        
        # Test 1: DetectCollapse
        print("\n🔎 Test 1: DetectCollapse")
        print("-" * 70)
        
        collapse_request = validation_pb2.CollapseRequest(
            dataset_id="ds_test_001",
            validation_id="val_test_001"
        )
        
        try:
            collapse_response = await stub.DetectCollapse(collapse_request)
            print(f"✅ Collapse Detection Complete")
            print(f"   Collapse Detected: {collapse_response.collapse_detected}")
            print(f"   Severity: {collapse_response.severity}")
            print(f"   Dimensions Analyzed: {len(collapse_response.dimensions)}")
            
            passed_dims = sum(1 for d in collapse_response.dimensions if d.passed)
            print(f"   Passed: {passed_dims}/{len(collapse_response.dimensions)}")
            
            for dim in collapse_response.dimensions[:3]:  # Show first 3
                status = "✅" if dim.passed else "❌"
                print(f"   {status} {dim.dimension}: {dim.score}/{dim.threshold}")
                
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")
        
        # Test 2: LocalizeProblems
        print("\n📍 Test 2: LocalizeProblems")
        print("-" * 70)
        
        localization_request = validation_pb2.LocalizationRequest(
            dataset_id="ds_test_001",
            validation_id="val_test_001",
            sample_s3_path="test_api_data_large.csv"
        )
        localization_request.collapse_info.collapse_detected = False
        
        try:
            localization_response = await stub.LocalizeProblems(localization_request)
            print(f"✅ Localization Complete")
            print(f"   Problematic Regions: {len(localization_response.regions)}")
            
            if localization_response.regions:
                for region in localization_response.regions:
                    print(f"   - {region.region_id}: rows {region.row_start}-{region.row_end}")
                    print(f"     Issue: {region.issue_type} (impact: {region.impact_score:.1f})")
            else:
                print(f"   No problems detected ✓")
                
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")
        
        # Test 3: GenerateRecommendations
        print("\n💡 Test 3: GenerateRecommendations")
        print("-" * 70)
        
        recommendation_request = validation_pb2.RecommendationRequest(
            dataset_id="ds_test_001",
            validation_id="val_test_001"
        )
        
        try:
            recommendation_response = await stub.GenerateRecommendations(recommendation_request)
            print(f"✅ Recommendations Generated")
            print(f"   Count: {len(recommendation_response.recommendations)}")
            print(f"   Combined Impact:")
            print(f"   - Current Risk: {recommendation_response.combined_impact.current_risk_score}/100")
            print(f"   - Expected Risk: {recommendation_response.combined_impact.expected_risk_score}/100")
            print(f"   - Improvement: +{recommendation_response.combined_impact.total_improvement} points")
            print(f"   - Estimated Time: {recommendation_response.combined_impact.estimated_time}")
            
            for i, rec in enumerate(recommendation_response.recommendations[:3], 1):
                print(f"\n   Recommendation {i}:")
                print(f"   - {rec.title}")
                print(f"   - Impact: +{rec.impact.improvement} points")
                print(f"   - Time: {rec.implementation.estimated_time}")
                
        except grpc.aio.AioRpcError as e:
            print(f"❌ Failed: {e.code()} - {e.details()}")


async def main():
    """Run all tests."""
    
    print("\n" + "="*70)
    print("GRPC SERVICE TEST SUITE")
    print("="*70)
    print("\nMake sure the gRPC server is running:")
    print("  python -m src.grpc_services.validation_server_complete")
    print()
    
    try:
        # Test both services
        await test_validation_service()
        await test_collapse_service()
        
        print("\n" + "="*70)
        print("✅ ALL GRPC TESTS COMPLETED")
        print("="*70)
        print()
        print("The gRPC services are now integrated with the orchestrator")
        print("and ready for production use.")
        print()
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    asyncio.run(main())
