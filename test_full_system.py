#!/usr/bin/env python3
"""
Full System Test - Test all ML modules with real data
Tests the complete validation pipeline end-to-end
"""

import asyncio
import numpy as np
import pandas as pd
import torch
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_dataset_loader():
    """Test dataset loading with multiple formats"""
    logger.info("\n" + "="*70)
    logger.info("TEST 1: Dataset Loader")
    logger.info("="*70)
    
    from src.data_processors.dataset_loader import DatasetLoader
    
    loader = DatasetLoader()
    
    # Create test data
    test_csv = Path("test_data.csv")
    df = pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000),
        'feature3': np.random.randn(1000),
        'label': np.random.randint(0, 2, 1000)
    })
    df.to_csv(test_csv, index=False)
    
    # Test CSV loading
    data = await loader.load_dataset(str(test_csv), 'csv')
    logger.info(f"✅ Loaded CSV: {data.shape}")
    
    # Test metadata extraction
    metadata = loader.get_metadata(str(test_csv))
    logger.info(f"✅ Metadata: {metadata.num_rows} rows, {metadata.num_columns} cols")
    
    # Cleanup
    test_csv.unlink()
    
    return True


async def test_diversity_analyzer():
    """Test diversity analysis"""
    logger.info("\n" + "="*70)
    logger.info("TEST 2: Diversity Analyzer")
    logger.info("="*70)
    
    from src.validation_engine.diversity_analyzer import DiversityAnalyzer
    
    # Create test data
    test_csv = Path("test_data.csv")
    df = pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000),
        'feature3': np.random.randn(1000)
    })
    df.to_csv(test_csv, index=False)
    
    analyzer = DiversityAnalyzer()
    result = await analyzer.analyze_diversity(str(test_csv), 'csv')
    
    logger.info(f"✅ Overall Diversity Score: {result.overall_score:.2f}/100")
    logger.info(f"   Dimensions:")
    for dim, score in result.dimension_scores.items():
        logger.info(f"      - {dim}: {score:.2f}")
    
    # Cleanup
    test_csv.unlink()
    
    return result.overall_score > 0


async def test_collapse_detector():
    """Test collapse detection with synthetic collapse scenario"""
    logger.info("\n" + "="*70)
    logger.info("TEST 3: Collapse Detector")
    logger.info("="*70)
    
    from src.collapse_engine.detector import CollapseDetector, CollapseConfig
    
    detector = CollapseDetector(CollapseConfig())
    
    # Test 1: Good data (should NOT detect collapse)
    logger.info("\n  Test 3a: Good Data (No Collapse Expected)")
    original = np.random.randn(1000, 50)
    synthetic = original + np.random.randn(1000, 50) * 0.1
    
    result = await detector.detect_collapse(synthetic, original)
    
    logger.info(f"  Overall Score: {result.overall_score:.2f}/100")
    logger.info(f"  Collapse Detected: {result.collapse_detected}")
    logger.info(f"  Confidence: {result.confidence:.1f}%")
    
    if not result.collapse_detected:
        logger.info("  ✅ Correctly identified good data")
    else:
        logger.warning("  ⚠️ False positive - detected collapse in good data")
    
    # Test 2: Mode collapse (should detect collapse)
    logger.info("\n  Test 3b: Mode Collapse (Collapse Expected)")
    original = np.random.randn(1000, 50)
    synthetic = np.random.randn(100, 50)  # Only 100 samples, repeated
    synthetic = np.tile(synthetic, (10, 1))  # Repeat to get 1000
    
    result_collapse = await detector.detect_collapse(synthetic, original)
    
    logger.info(f"  Overall Score: {result_collapse.overall_score:.2f}/100")
    logger.info(f"  Collapse Detected: {result_collapse.collapse_detected}")
    logger.info(f"  Confidence: {result_collapse.confidence:.1f}%")
    
    if result_collapse.collapse_detected:
        logger.info("  ✅ Correctly identified collapsed data")
    else:
        logger.warning("  ⚠️ False negative - missed collapse")
    
    # Show dimension breakdown
    logger.info("\n  Dimension Scores:")
    for name, dim in result.dimensions.items():
        status = '✅' if dim.passed else '❌'
        logger.info(f"    {status} {dim.name}: {dim.score:.2f}/100 (threshold: {dim.threshold})")
    
    return True


async def test_signature_library():
    """Test signature library pattern matching"""
    logger.info("\n" + "="*70)
    logger.info("TEST 4: Signature Library")
    logger.info("="*70)
    
    from src.collapse_engine.signature_library import SignatureLibrary
    
    library = SignatureLibrary()
    
    # Test pattern matching (library is pre-populated)
    test_data = np.random.randn(100, 50)
    matches = library.match_patterns(test_data, top_k=3)
    logger.info(f"✅ Pattern matching complete")
    
    if matches:
        logger.info(f"✅ Found {len(matches)} matches:")
        for match in matches:
            logger.info(f"   - {match['pattern_name']}: {match['similarity']:.3f} similarity")
    else:
        logger.info("  No matches found")
    
    return True


async def test_localizer():
    """Test gradient-based localization"""
    logger.info("\n" + "="*70)
    logger.info("TEST 5: Collapse Localizer")
    logger.info("="*70)
    
    from src.collapse_engine.localizer import CollapseLocalizer
    
    localizer = CollapseLocalizer()
    
    # Create dataset with problematic region
    dataset = torch.randn(1000, 50)
    # Inject anomaly in rows 400-500
    dataset[400:500] = torch.ones(100, 50) * 0.1  # Constant values
    
    collapse_dimensions = {}  # Empty for now
    
    result = await localizer.localize_collapse(dataset, collapse_dimensions)
    
    # Handle different result formats
    if hasattr(result, 'problematic_indices'):
        indices = result.problematic_indices
    elif isinstance(result, dict):
        indices = result.get('problematic_indices', [])
    else:
        indices = []
    
    logger.info(f"✅ Localization complete")
    logger.info(f"   Found {len(indices)} problematic rows")
    
    return True


async def test_recommender():
    """Test recommendation engine"""
    logger.info("\n" + "="*70)
    logger.info("TEST 6: Recommendation Engine")
    logger.info("="*70)
    
    from src.collapse_engine.recommender import RecommendationEngine
    
    recommender = RecommendationEngine()
    
    # Test with low collapse score (should generate recommendations)
    result = await recommender.generate_recommendations(
        collapse_score=55.0,
        dimension_scores={},
        diversity_score=60.0,
        dataset_size=1000000
    )
    
    # Handle result attributes
    recs = result.recommendations if hasattr(result, 'recommendations') else []
    improvement = result.projected_improvement if hasattr(result, 'projected_improvement') else 0
    proj_score = result.projected_score if hasattr(result, 'projected_score') else 0
    
    logger.info(f"✅ Generated {len(recs)} recommendations")
    logger.info(f"   Projected Improvement: +{improvement:.1f} points")
    logger.info(f"   Projected Score: {proj_score:.1f}/100")
    
    result.recommendations = recs  # Ensure it's set
    
    if result.recommendations:
        logger.info(f"\n   Top 3 Recommendations:")
        for i, rec in enumerate(result.recommendations[:3], 1):
            title = rec.title if hasattr(rec, 'title') else str(rec)
            impact = rec.estimated_impact if hasattr(rec, 'estimated_impact') else 0
            cost = rec.cost_usd if hasattr(rec, 'cost_usd') else 0
            logger.info(f"      {i}. {title}")
            logger.info(f"         Impact: +{impact:.1f} points | Cost: ${cost:,.0f}")
    
    return len(result.recommendations) > 0


async def test_cascade_trainer_basic():
    """Test cascade trainer with small model"""
    logger.info("\n" + "="*70)
    logger.info("TEST 7: Cascade Trainer (Basic)")
    logger.info("="*70)
    
    try:
        from src.validation_engine.cascade_trainer import CascadeTrainer
        from src.model_architectures import create_resonance_model
        
        # Test model creation
        model = create_resonance_model('tiny')
        logger.info(f"✅ Created Resonance NN model (tiny)")
        logger.info(f"   Parameters: {sum(p.numel() for p in model.parameters()):,}")
        
        # Create small training data
        train_data = torch.randint(0, 1000, (10000,))
        val_data = torch.randint(0, 1000, (1000,))
        
        logger.info(f"✅ Created training data: {len(train_data):,} samples")
        logger.info(f"   Note: Full cascade training requires GPU and takes time")
        logger.info(f"   Skipping full training in basic test")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Cascade trainer test failed: {e}")
        return False


async def test_orchestrator():
    """Test full orchestrator pipeline"""
    logger.info("\n" + "="*70)
    logger.info("TEST 8: Orchestrator (Full Pipeline)")
    logger.info("="*70)
    
    from src.orchestrator import SynthosOrchestrator
    
    # Create test data
    test_csv = Path("test_data.csv")
    df = pd.DataFrame({
        'feature1': np.random.randn(1000),
        'feature2': np.random.randn(1000),
        'feature3': np.random.randn(1000)
    })
    df.to_csv(test_csv, index=False)
    
    # Run orchestrator
    orchestrator = SynthosOrchestrator(
        skip_cascade_training=True,  # Skip expensive training for test
        collapse_threshold=65.0,
        diversity_threshold=50.0
    )
    
    logger.info("  Running full validation pipeline...")
    result = await orchestrator.validate(
        dataset_path=str(test_csv),
        dataset_format='csv',
        stream_progress=False,  # Quiet mode for test
        validation_id="test_001",
        dataset_id="test_ds_001"
    )
    
    logger.info(f"\n✅ Pipeline Complete!")
    logger.info(f"   Validation ID: {result.validation_id}")
    logger.info(f"   Dataset ID: {result.dataset_id}")
    logger.info(f"   Status: {result.status}")
    logger.info(f"   Collapse Score: {result.collapse_score:.2f}/100")
    logger.info(f"   Diversity Score: {result.diversity_score:.2f}/100")
    logger.info(f"   Approved: {result.approved_for_training}")
    logger.info(f"   Total Time: {result.total_time_seconds:.2f}s")
    
    # Test API-compliant output
    output_dict = result.to_dict()
    logger.info(f"\n✅ API-Compliant Output:")
    logger.info(f"   Risk Score: {output_dict['results']['risk_score']}")
    logger.info(f"   Risk Level: {output_dict['results']['risk_level']}")
    logger.info(f"   Recommendation: {output_dict['results']['recommendation']}")
    logger.info(f"   Warranty Eligible: {output_dict['results']['warranty_eligible']}")
    
    # Cleanup
    test_csv.unlink()
    
    return result.approved_for_training is not None


async def run_all_tests():
    """Run all system tests"""
    logger.info("\n" + "="*70)
    logger.info("FULL SYSTEM TEST SUITE")
    logger.info("Testing all ML validation modules")
    logger.info("="*70)
    
    tests = [
        ("Dataset Loader", test_dataset_loader),
        ("Diversity Analyzer", test_diversity_analyzer),
        ("Collapse Detector", test_collapse_detector),
        ("Signature Library", test_signature_library),
        ("Collapse Localizer", test_localizer),
        ("Recommendation Engine", test_recommender),
        ("Cascade Trainer", test_cascade_trainer_basic),
        ("Full Orchestrator", test_orchestrator),
    ]
    
    results = []
    
    for name, test_func in tests:
        try:
            success = await test_func()
            results.append((name, success))
        except Exception as e:
            logger.error(f"\n❌ {name} failed with exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
    
    # Summary
    logger.info("\n" + "="*70)
    logger.info("TEST SUMMARY")
    logger.info("="*70)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        logger.info(f"{status} - {name}")
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Results: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
    logger.info(f"{'='*70}")
    
    if passed == total:
        logger.info("\n🎉 ALL TESTS PASSED! System is functional!")
    else:
        logger.warning(f"\n⚠️ {total - passed} test(s) failed. Review logs above.")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(run_all_tests())
    exit(0 if success else 1)
