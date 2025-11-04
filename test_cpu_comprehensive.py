#!/usr/bin/env python3
"""
Comprehensive CPU test - Run before GPU deployment
Tests all major components on CPU to catch errors early
"""

import sys
import asyncio
import numpy as np
import pandas as pd
import torch
from pathlib import Path

print("="*70)
print("🧪 COMPREHENSIVE CPU TEST SUITE")
print("="*70)

# Test 1: Model Architecture Imports
print("\n1️⃣  Testing Model Architectures...")
try:
    from src.model_architectures import (
        create_resonance_model,
        create_temporal_eigenstate_model,
        get_model_info
    )
    
    # Create tiny models for testing
    model1 = create_resonance_model('tiny', vocab_size=1000)
    model2 = create_temporal_eigenstate_model(d_model=128, n_layers=2)
    
    info1 = get_model_info(model1)
    info2 = get_model_info(model2)
    
    print(f"   ✅ Resonance NN: {info1['total_params']:,} params")
    print(f"   ✅ Temporal Eigenstate: {info2['total_params']:,} params")
    
except Exception as e:
    print(f"   ❌ Model architecture test failed: {e}")
    sys.exit(1)

# Test 2: Dataset Loader
print("\n2️⃣  Testing Dataset Loader...")
try:
    from src.data_processors.dataset_loader import DatasetLoader
    
    # Create test data
    test_data = pd.DataFrame({
        'feature_1': np.random.randn(1000),
        'feature_2': np.random.randn(1000),
        'feature_3': np.random.randn(1000),
        'target': np.random.randint(0, 2, 1000)
    })
    test_path = Path("test_data.csv")
    test_data.to_csv(test_path, index=False)
    
    loader = DatasetLoader()
    metadata = loader.get_metadata(test_path)
    loaded_data = loader.load_full(test_path)
    
    print(f"   ✅ Loaded {len(loaded_data):,} rows, {len(loaded_data.columns)} columns")
    print(f"   ✅ Metadata: {metadata.estimated_rows} rows estimated")
    
    test_path.unlink()  # Cleanup
    
except Exception as e:
    print(f"   ❌ Dataset loader test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Collapse Detector (basic instantiation)
print("\n3️⃣  Testing Collapse Detector...")
try:
    from src.collapse_engine.detector import CollapseDetector, CollapseConfig
    
    config = CollapseConfig(use_gpu=False)  # Force CPU
    detector = CollapseDetector(config)
    
    print(f"   ✅ Collapse Detector initialized on {detector.device}")
    
except Exception as e:
    print(f"   ❌ Collapse detector test failed: {e}")
    import traceback
    traceback.print_exc()
    # Don't exit, continue with other tests

# Test 4: GPU Optimizer (CPU fallback)
print("\n4️⃣  Testing GPU Optimizer...")
try:
    from src.utils.gpu_optimizer import GPUOptimizer
    
    optimizer = GPUOptimizer(memory_fraction=0.5, enable_mixed_precision=False)
    print(f"   ✅ GPU Optimizer initialized (CPU mode: {not torch.cuda.is_available()})")
    
except Exception as e:
    print(f"   ❌ GPU optimizer test failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Model Forward Pass
print("\n5️⃣  Testing Model Forward Pass...")
try:
    # Create small model and test forward pass
    model = create_resonance_model('tiny', vocab_size=100)
    model.eval()
    
    # Create dummy input
    batch_size = 2
    seq_len = 10
    dummy_input = torch.randint(0, 100, (batch_size, seq_len))
    
    with torch.no_grad():
        output = model(dummy_input)
    
    print(f"   ✅ Forward pass successful: input {dummy_input.shape} → output {output.shape}")
    
except Exception as e:
    print(f"   ❌ Forward pass test failed: {e}")
    import traceback
    traceback.print_exc()

# Test 6: Basic Training Loop (1 epoch)
print("\n6️⃣  Testing Basic Training Loop...")
try:
    from torch.utils.data import DataLoader, TensorDataset
    import torch.nn as nn
    
    # Create tiny model
    model = create_resonance_model('tiny', vocab_size=100)
    model.train()
    
    # Create tiny dataset
    num_samples = 100
    seq_len = 10
    data = torch.randint(0, 100, (num_samples, seq_len))
    targets = torch.randint(0, 100, (num_samples, seq_len))
    
    dataset = TensorDataset(data, targets)
    dataloader = DataLoader(dataset, batch_size=10)
    
    criterion = nn.CrossEntropyLoss()
    optimizer_torch = torch.optim.Adam(model.parameters(), lr=0.001)
    
    # Train for 1 epoch
    total_loss = 0
    for batch_data, batch_targets in dataloader:
        optimizer_torch.zero_grad()
        outputs = model(batch_data)
        loss = criterion(outputs.view(-1, outputs.size(-1)), batch_targets.view(-1))
        loss.backward()
        optimizer_torch.step()
        total_loss += loss.item()
    
    avg_loss = total_loss / len(dataloader)
    print(f"   ✅ Training loop successful: avg loss = {avg_loss:.4f}")
    
except Exception as e:
    print(f"   ❌ Training loop test failed: {e}")
    import traceback
    traceback.print_exc()

# Summary
print("\n" + "="*70)
print("✅ CPU TEST SUITE COMPLETE")
print("="*70)
print("\n💡 Next steps:")
print("   1. Review any warnings or errors above")
print("   2. If all tests pass, the code is ready for GPU testing")
print("   3. Deploy to GPU instance and run full validation")
print("\n" + "="*70)
