"""
Simple Unified Pipeline Example
================================

This demonstrates how all modules work together automatically.
Just load data and the orchestrator handles everything!

Author: ML Engineering Team
Date: October 31, 2025
"""

import asyncio
from src import SynthosOrchestrator

async def main():
    """
    Simple 3-step validation:
    1. Create orchestrator
    2. Run validation
    3. Check result
    """
    
    print("🚀 Synthos Unified Validation Pipeline")
    print("=" * 60)
    
    # STEP 1: Create orchestrator (initializes all modules automatically)
    print("\n📦 Initializing all modules...")
    orchestrator = SynthosOrchestrator(
        collapse_threshold=65.0,      # Minimum quality score
        diversity_threshold=50.0,     # Minimum diversity
        enable_mixed_precision=True   # Use BF16 on H100
    )
    
    # STEP 2: Run validation (automatically flows through all 6 stages)
    print("\n🔍 Starting validation pipeline...\n")
    result = await orchestrator.validate(
        dataset_path="data/sample_data.csv",
        dataset_format="csv",
        output_report_path="validation_report.json",
        stream_progress=True  # Show real-time progress
    )
    
    # STEP 3: Check result and take action
    print("\n" + "=" * 60)
    print("📋 FINAL RESULT")
    print("=" * 60)
    
    if result.approved_for_training:
        print("✅ APPROVED FOR TRAINING")
        print(f"   • Quality Score: {result.collapse_score:.1f}/100")
        print(f"   • Diversity Score: {result.diversity_score:.1f}/100")
        print(f"   • Confidence: {result.confidence:.1f}%")
        print(f"\n🚀 You can now proceed with model training!")
        
    else:
        print("❌ NOT APPROVED - Issues Found")
        print(f"   • Quality Score: {result.collapse_score:.1f}/100 (need ≥65)")
        print(f"   • Diversity Score: {result.diversity_score:.1f}/100 (need ≥50)")
        print(f"   • Problematic Rows: {len(result.problematic_rows):,}")
        print(f"\n💡 Top Recommendations:")
        
        for i, rec in enumerate(result.recommendations[:3], 1):
            print(f"   {i}. {rec['title']}")
            print(f"      • Expected Impact: +{rec['estimated_impact']:.1f} points")
            print(f"      • Cost: ${rec['cost_usd']:,.0f}")
            print(f"      • Priority: {rec['priority']}")
        
        print(f"\n📈 After fixes, expected score: {result.collapse_score + result.projected_improvement:.1f}/100")
    
    # Performance stats
    print(f"\n⚡ Performance:")
    print(f"   • Total Time: {result.total_time_seconds:.1f}s")
    print(f"   • Rows Processed: {result.total_rows:,}")
    print(f"   • Throughput: {result.total_rows/result.total_time_seconds:,.0f} rows/sec")
    print(f"   • GPU Utilization: {result.gpu_utilization_avg:.1f}%")
    
    print(f"\n📄 Full report saved to: validation_report.json")
    print("=" * 60)
    
    return result


if __name__ == "__main__":
    # Run the unified pipeline
    result = asyncio.run(main())
    
    # Exit code for CI/CD integration
    exit(0 if result.approved_for_training else 1)
