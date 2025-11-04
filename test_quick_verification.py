#!/usr/bin/env python3
"""
Quick Implementation Verification Test
=======================================

Fast test to verify all imports and basic functionality work.
"""

import sys
print("="*80)
print("QUICK IMPLEMENTATION VERIFICATION")
print("="*80)

# Test 1: Imports
print("\n[1/5] Testing Imports...")
try:
    from src.model_architectures import create_resonance_model, create_temporal_eigenstate_model
    print("  ✅ Model architectures")
    
    from src.data_processors.dataset_loader import DatasetLoader
    print("  ✅ Data loader")
    
    from src.validation_engine.diversity_analyzer import DiversityAnalyzer
    print("  ✅ Diversity analyzer")
    
    from src.validation_engine.cascade_trainer import CascadeTrainer
    print("  ✅ Cascade trainer")
    
    from src.collapse_engine.detector import CollapseDetector
    print("  ✅ Collapse detector")
    
    from src.collapse_engine.signature_library import SignatureLibrary
    print("  ✅ Signature library")
    
    from src.collapse_engine.localizer import GradientLocalizer
    print("  ✅ Gradient localizer")
    
    from src.collapse_engine.recommender import RecommendationEngine
    print("  ✅ Recommendation engine")
    
    from src.utils.gpu_optimizer import GPUOptimizer
    print("  ✅ GPU optimizer")
    
    from src.orchestrator import SynthosOrchestrator
    print("  ✅ Orchestrator")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Data Loader
print("\n[2/5] Testing Data Loader...")
try:
    import pandas as pd
    import numpy as np
    import os
    
    loader = DatasetLoader()
    
    # Create and test CSV
    test_data = pd.DataFrame({
        'col1': np.random.randn(100),
        'col2': np.random.randn(100)
    })
    test_data.to_csv('test.csv', index=False)
    
    metadata = loader.get_metadata('test.csv')
    data = loader.load_full('test.csv')
    
    print(f"  ✅ CSV: Loaded {len(data)} rows, {len(data.columns)} columns")
    os.remove('test.csv')
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    sys.exit(1)

# Test 3: Model Architecture Access
print("\n[3/5] Testing Model Configs...")
try:
    from src.model_architectures import MODEL_CONFIGS, RESONANCE_CONFIGS
    
    print(f"  ✅ Available sizes: {list(MODEL_CONFIGS.keys())}")
    print(f"  ✅ Resonance configs: {list(RESONANCE_CONFIGS.keys())}")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    sys.exit(1)

# Test 4: Collapse Detector Logic
print("\n[4/5] Testing Collapse Detection Logic...")
try:
    import asyncio
    import torch
    
    detector = CollapseDetector()
    
    # Small test arrays
    orig = np.random.randn(100, 10).astype(np.float32)
    synth = orig + np.random.randn(100, 10).astype(np.float32) * 0.1
    
    result = asyncio.run(detector.detect_collapse(synth, orig))
    
    print(f"  ✅ Score: {result['overall_score']:.1f}/100")
    print(f"  ✅ Dimensions: {len(result['dimensions'])}")
    print(f"  ✅ Collapse detected: {result['collapse_detected']}")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Orchestrator Initialization
print("\n[5/5] Testing Orchestrator Initialization...")
try:
    orchestrator = SynthosOrchestrator(
        enable_mixed_precision=False,
        collapse_threshold=65.0
    )
    
    print(f"  ✅ Orchestrator initialized")
    print(f"  ✅ Collapse threshold: {orchestrator.collapse_threshold}")
    print(f"  ✅ Diversity threshold: {orchestrator.diversity_threshold}")
    
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Summary
print("\n" + "="*80)
print("✅ ALL QUICK TESTS PASSED!")
print("="*80)
print("\nVerified Components:")
print("  ✅ All imports successful")
print("  ✅ Data loading works")
print("  ✅ Model configs accessible")
print("  ✅ Collapse detection logic works")
print("  ✅ Orchestrator can initialize")
print("\n🚀 Ready for full testing on GPU instance!")
print("="*80)
