#!/usr/bin/env python3
"""
Comprehensive Implementation Test
==================================

Tests all major components to verify they work correctly:
1. Model architectures (Resonance NN + Temporal Eigenstate)
2. Data loading (all formats)
3. Diversity analyzer
4. Collapse detector
5. Recommender
6. Full orchestrator integration
"""

import sys
import os
import asyncio
import numpy as np
import pandas as pd
import torch
from pathlib import Path

print("="*80)
print("COMPREHENSIVE IMPLEMENTATION TEST")
print("="*80)

# Test 1: Model Architectures
print("\n[1/7] Testing Model Architectures...")
try:
    from src.model_architectures import (
        create_resonance_model,
        create_temporal_eigenstate_model,
        get_model_info
    )
    
    # Create models
    resonance_tiny = create_resonance_model('tiny', vocab_size=1000)
    resonance_small = create_resonance_model('small', vocab_size=1000)
    temporal_model = create_temporal_eigenstate_model()
    
    # Get info
    info_tiny = get_model_info(resonance_tiny)
    info_small = get_model_info(resonance_small)
    info_temporal = get_model_info(temporal_model)
    
    print(f"  ✅ Resonance NN (tiny): {info_tiny['total_params']:,} params")
    print(f"  ✅ Resonance NN (small): {info_small['total_params']:,} params")
    print(f"  ✅ Temporal Eigenstate: {info_temporal['total_params']:,} params")
    
    # Clean up
    del resonance_tiny, resonance_small, temporal_model
    torch.cuda.empty_cache() if torch.cuda.is_available() else None
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Data Loader
print("\n[2/7] Testing Data Loader...")
try:
    from src.data_processors.dataset_loader import DatasetLoader
    
    loader = DatasetLoader()
    
    # Create test data in multiple formats
    test_data = pd.DataFrame({
        'feature_1': np.random.randn(5000),
        'feature_2': np.random.randn(5000),
        'feature_3': np.random.randint(0, 10, 5000),
        'label': np.random.randint(0, 2, 5000)
    })
    
    # Test CSV
    test_data.to_csv('test_data.csv', index=False)
    metadata_csv = loader.get_metadata('test_data.csv')
    data_csv = loader.load_full('test_data.csv')
    print(f"  ✅ CSV: {len(data_csv)} rows loaded")
    os.remove('test_data.csv')
    
    # Test Parquet
    test_data.to_parquet('test_data.parquet', index=False)
    metadata_parquet = loader.get_metadata('test_data.parquet')
    data_parquet = loader.load_full('test_data.parquet')
    print(f"  ✅ Parquet: {len(data_parquet)} rows loaded")
    os.remove('test_data.parquet')
    
    # Test JSON
    test_data.to_json('test_data.json', orient='records')
    data_json = loader.load_full('test_data.json')
    print(f"  ✅ JSON: {len(data_json)} rows loaded")
    os.remove('test_data.json')
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Diversity Analyzer
print("\n[3/7] Testing Diversity Analyzer...")
try:
    from src.validation_engine.diversity_analyzer import DiversityAnalyzer
    
    analyzer = DiversityAnalyzer()
    
    # Create test data
    test_data = pd.DataFrame({
        'feature_1': np.random.randn(10000),
        'feature_2': np.random.randn(10000),
        'feature_3': np.random.randint(0, 100, 10000)
    })
    test_data.to_csv('test_diversity.csv', index=False)
    
    # Analyze diversity
    diversity_result = asyncio.run(analyzer.analyze_diversity('test_diversity.csv', 'csv'))
    
    print(f"  ✅ Overall Score: {diversity_result['overall_score']:.2f}/100")
    print(f"  ✅ Semantic Diversity: {diversity_result['semantic_diversity']:.2f}")
    print(f"  ✅ Statistical Diversity: {diversity_result['statistical_diversity']:.2f}")
    
    os.remove('test_diversity.csv')
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Collapse Detector
print("\n[4/7] Testing Collapse Detector...")
try:
    from src.collapse_engine.detector import CollapseDetector
    
    detector = CollapseDetector()
    
    # Create synthetic vs original data
    original_data = np.random.randn(5000, 10).astype(np.float32)
    synthetic_data = original_data + np.random.randn(5000, 10).astype(np.float32) * 0.1
    
    # Detect collapse
    result = asyncio.run(detector.detect_collapse(
        synthetic_data=synthetic_data,
        original_data=original_data
    ))
    
    print(f"  ✅ Overall Score: {result['overall_score']:.2f}/100")
    print(f"  ✅ Collapse Detected: {result['collapse_detected']}")
    print(f"  ✅ Dimensions analyzed: {len(result['dimensions'])}")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Signature Library
print("\n[5/7] Testing Signature Library...")
try:
    from src.collapse_engine.signature_library import SignatureLibrary
    
    library = SignatureLibrary()
    
    # Add some test signatures
    for i in range(10):
        signature = np.random.randn(512).astype(np.float32)
        pattern_type = f"pattern_{i % 3}"
        library.add_signature(
            signature=signature,
            pattern_type=pattern_type,
            severity=0.5 + i * 0.05,
            metadata={'test_id': i}
        )
    
    # Search for similar patterns
    query = np.random.randn(512).astype(np.float32)
    results = asyncio.run(library.find_similar_patterns(query, top_k=5))
    
    print(f"  ✅ Added 10 signatures")
    print(f"  ✅ Found {len(results)} similar patterns")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 6: Recommendation Engine
print("\n[6/7] Testing Recommendation Engine...")
try:
    from src.collapse_engine.recommender import RecommendationEngine
    
    recommender = RecommendationEngine()
    
    # Generate recommendations
    dimension_scores = {
        'distribution_fidelity': 45.0,
        'correlation_preservation': 60.0,
        'entropy_stability': 55.0,
        'gradient_health': 70.0,
        'loss_landscape': 65.0,
        'spectral_coherence': 50.0,
        'generalization_gap': 55.0,
        'statistical_consistency': 60.0
    }
    
    result = asyncio.run(recommender.generate_recommendations(
        collapse_score=57.5,
        dimension_scores=dimension_scores,
        diversity_score=45.0
    ))
    
    print(f"  ✅ Generated {len(result['recommendations'])} recommendations")
    print(f"  ✅ Projected improvement: +{result['projected_score'] - 57.5:.1f} points")
    if result['recommendations']:
        top_rec = result['recommendations'][0]
        print(f"  ✅ Top recommendation: {top_rec['title']}")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 7: Full Orchestrator
print("\n[7/7] Testing Full Orchestrator...")
try:
    from src.orchestrator import SynthosOrchestrator
    
    orchestrator = SynthosOrchestrator(
        collapse_threshold=60.0,
        diversity_threshold=45.0,
        enable_mixed_precision=False  # CPU mode
    )
    
    # Create test dataset
    test_data = pd.DataFrame({
        'feature_1': np.random.randn(1000),
        'feature_2': np.random.randn(1000),
        'feature_3': np.random.randint(0, 10, 1000)
    })
    test_data.to_parquet('test_orchestrator.parquet', index=False)
    
    # Run full validation (this will take a bit)
    print("  ⏳ Running full validation pipeline...")
    result = asyncio.run(orchestrator.validate(
        dataset_path='test_orchestrator.parquet',
        dataset_format='parquet',
        stream_progress=False  # Suppress detailed progress
    ))
    
    print(f"  ✅ Validation completed in {result.total_time_seconds:.2f}s")
    print(f"  ✅ Collapse Score: {result.collapse_score:.1f}/100")
    print(f"  ✅ Diversity Score: {result.diversity_score:.1f}/100")
    print(f"  ✅ Approved: {result.approved_for_training}")
    print(f"  ✅ Recommendations: {len(result.recommendations)}")
    
    os.remove('test_orchestrator.parquet')
    if Path('test_orchestrator_report.json').exists():
        os.remove('test_orchestrator_report.json')
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Summary
print("\n" + "="*80)
print("✅ ALL TESTS PASSED!")
print("="*80)
print("\nImplementation Status:")
print("  ✅ Model Architectures: Working")
print("  ✅ Data Loader: Working")
print("  ✅ Diversity Analyzer: Working")
print("  ✅ Collapse Detector: Working")
print("  ✅ Signature Library: Working")
print("  ✅ Recommendation Engine: Working")
print("  ✅ Full Orchestrator: Working")
print("\n🚀 System is ready for GPU instance testing!")
print("="*80)
