"""
Quick Test Script - Verify Unified Pipeline
============================================

This script verifies that the orchestrator can be imported and used.
It creates sample data and runs a quick validation.

Author: ML Engineering Team
Date: October 31, 2025
"""

import sys
import asyncio
from pathlib import Path

print("=" * 70)
print("🧪 TESTING SYNTHOS UNIFIED PIPELINE")
print("=" * 70)

# Step 1: Check imports
print("\n📦 Step 1: Checking imports...")
try:
    from src import SynthosOrchestrator, ValidationResult
    print("✅ SynthosOrchestrator imported successfully")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print("\n💡 Fix: Install dependencies:")
    print("   pip install -r requirements.txt")
    sys.exit(1)

# Step 2: Create sample data
print("\n📊 Step 2: Creating sample dataset...")
try:
    import numpy as np
    import pandas as pd
    
    # Create simple test data
    np.random.seed(42)
    data = {
        'feature_1': np.random.randn(1000),
        'feature_2': np.random.randn(1000),
        'feature_3': np.random.randn(1000),
        'label': np.random.randint(0, 2, 1000)
    }
    df = pd.DataFrame(data)
    
    # Save to CSV
    test_path = Path("test_data_sample.csv")
    df.to_csv(test_path, index=False)
    print(f"✅ Created test dataset: {test_path} (1000 rows)")
    
except Exception as e:
    print(f"❌ Failed to create test data: {e}")
    sys.exit(1)

# Step 3: Initialize orchestrator
print("\n🚀 Step 3: Initializing orchestrator...")
try:
    orchestrator = SynthosOrchestrator(
        collapse_threshold=65.0,
        diversity_threshold=50.0,
        enable_mixed_precision=False,  # Disable for testing
        use_cache=True
    )
    print("✅ Orchestrator initialized")
    print("   ├─ Collapse threshold: 65.0")
    print("   ├─ Diversity threshold: 50.0")
    print("   └─ Mixed precision: disabled (testing mode)")
    
except Exception as e:
    print(f"❌ Initialization failed: {e}")
    print(f"\n💡 Error details: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Step 4: Run validation
print("\n🔍 Step 4: Running validation pipeline...")
print("-" * 70)

async def run_validation():
    try:
        result = await orchestrator.validate(
            dataset_path=str(test_path),
            dataset_format="csv",
            output_report_path="test_validation_report.json",
            stream_progress=True
        )
        return result
    except Exception as e:
        print(f"\n❌ Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return None

try:
    result = asyncio.run(run_validation())
except Exception as e:
    print(f"\n❌ Async execution failed: {e}")
    import traceback
    traceback.print_exc()
    result = None

# Step 5: Verify results
print("\n" + "=" * 70)
print("📋 VALIDATION RESULTS")
print("=" * 70)

if result:
    print("\n✅ PIPELINE COMPLETED SUCCESSFULLY!")
    print(f"\n📊 Results:")
    print(f"   • Approved: {result.approved_for_training}")
    print(f"   • Quality Score: {result.collapse_score:.1f}/100")
    print(f"   • Diversity Score: {result.diversity_score:.1f}/100")
    print(f"   • Confidence: {result.confidence:.1f}%")
    print(f"   • Total Time: {result.total_time_seconds:.2f}s")
    print(f"   • Rows Processed: {result.total_rows:,}")
    print(f"   • Recommendations: {len(result.recommendations)}")
    
    if result.approved_for_training:
        print(f"\n✅ Dataset approved for training!")
    else:
        print(f"\n⚠️  Dataset needs improvement:")
        print(f"   Reason: {result.reason}")
    
    print(f"\n📄 Full report saved to: test_validation_report.json")
    print("\n" + "=" * 70)
    print("🎉 ALL TESTS PASSED!")
    print("=" * 70)
    
    # Cleanup
    print("\n🧹 Cleaning up test files...")
    test_path.unlink(missing_ok=True)
    Path("test_validation_report.json").unlink(missing_ok=True)
    print("✅ Cleanup complete")
    
    sys.exit(0)
    
else:
    print("\n❌ PIPELINE FAILED")
    print("\n💡 Troubleshooting:")
    print("   1. Check that all dependencies are installed")
    print("   2. Verify GPU is available (or disable mixed precision)")
    print("   3. Check the error messages above")
    print("   4. Review logs for more details")
    
    # Cleanup
    test_path.unlink(missing_ok=True)
    
    sys.exit(1)
