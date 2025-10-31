"""
Verification Script for Resonance NN Installation
Run this after installing the package to verify everything works correctly.

Usage:
    python verify_installation.py
"""

import sys

def test_import():
    """Test basic imports"""
    print("🔍 Testing imports...")
    try:
        import resonance_nn
        print(f"   ✅ resonance_nn imported successfully (v{resonance_nn.__version__})")
        return True
    except ImportError as e:
        print(f"   ❌ Failed to import resonance_nn: {e}")
        return False

def test_dependencies():
    """Test required dependencies"""
    print("\n🔍 Testing dependencies...")
    
    dependencies = {
        'torch': '2.0.0',
        'numpy': '1.21.0',
        'scipy': '1.7.0'
    }
    
    all_ok = True
    for package, min_version in dependencies.items():
        try:
            module = __import__(package)
            version = getattr(module, '__version__', 'unknown')
            print(f"   ✅ {package} {version}")
        except ImportError:
            print(f"   ❌ {package} not found (required >= {min_version})")
            all_ok = False
    
    return all_ok

def test_model_creation():
    """Test creating models"""
    print("\n🔍 Testing model creation...")
    
    try:
        from resonance_nn import create_spectral_lm
        
        # Create a tiny model for testing
        model = create_spectral_lm('tiny', vocab_size=1000)
        num_params = model.get_num_params()
        
        print(f"   ✅ Created 'tiny' model with {num_params/1e6:.1f}M parameters")
        return True
    except Exception as e:
        print(f"   ❌ Failed to create model: {e}")
        return False

def test_forward_pass():
    """Test forward pass"""
    print("\n🔍 Testing forward pass...")
    
    try:
        import torch
        from resonance_nn import create_spectral_lm
        
        # Create model
        model = create_spectral_lm('tiny', vocab_size=1000)
        model.eval()
        
        # Create dummy input
        input_ids = torch.randint(0, 1000, (2, 128))
        
        # Forward pass
        with torch.no_grad():
            logits = model(input_ids)
        
        expected_shape = (2, 128, 1000)
        if logits.shape == expected_shape:
            print(f"   ✅ Forward pass successful, output shape: {logits.shape}")
            return True
        else:
            print(f"   ❌ Unexpected output shape: {logits.shape} (expected {expected_shape})")
            return False
            
    except Exception as e:
        print(f"   ❌ Forward pass failed: {e}")
        return False

def test_classifier():
    """Test classifier model"""
    print("\n🔍 Testing classifier...")
    
    try:
        import torch
        from resonance_nn import SpectralClassifier, SpectralConfig, ModalityType
        
        # Create config
        config = SpectralConfig(
            vocab_size=1000,
            embed_dim=256,
            hidden_dim=512,
            num_layers=4,
            max_seq_len=128,
            modality=ModalityType.TEXT
        )
        
        # Create classifier
        model = SpectralClassifier(config, num_classes=2)
        model.eval()
        
        # Test forward pass
        input_ids = torch.randint(0, 1000, (4, 128))
        with torch.no_grad():
            logits = model(input_ids)
        
        expected_shape = (4, 2)
        if logits.shape == expected_shape:
            print(f"   ✅ Classifier working, output shape: {logits.shape}")
            return True
        else:
            print(f"   ❌ Unexpected output shape: {logits.shape} (expected {expected_shape})")
            return False
            
    except Exception as e:
        print(f"   ❌ Classifier test failed: {e}")
        return False

def test_available_models():
    """Test listing available models"""
    print("\n🔍 Testing model configurations...")
    
    try:
        from resonance_nn import list_available_models
        
        print()
        list_available_models()
        return True
    except Exception as e:
        print(f"   ❌ Failed to list models: {e}")
        return False

def test_cuda_availability():
    """Check CUDA availability"""
    print("\n🔍 Checking GPU availability...")
    
    try:
        import torch
        
        if torch.cuda.is_available():
            device_name = torch.cuda.get_device_name(0)
            print(f"   ✅ CUDA available: {device_name}")
            
            # Test model on GPU
            from resonance_nn import create_spectral_lm
            model = create_spectral_lm('tiny', vocab_size=1000).cuda()
            input_ids = torch.randint(0, 1000, (2, 128)).cuda()
            
            with torch.no_grad():
                output = model(input_ids)
            
            print(f"   ✅ GPU forward pass successful")
            return True
        else:
            print("   ⚠️  CUDA not available (using CPU)")
            return True
            
    except Exception as e:
        print(f"   ❌ GPU test failed: {e}")
        return False

def main():
    """Run all verification tests"""
    print("="*80)
    print("Resonance NN - Installation Verification")
    print("="*80)
    
    tests = [
        test_import,
        test_dependencies,
        test_model_creation,
        test_forward_pass,
        test_classifier,
        test_available_models,
        test_cuda_availability,
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"   ❌ Test crashed: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "="*80)
    print("Summary")
    print("="*80)
    
    passed = sum(results)
    total = len(results)
    
    if passed == total:
        print(f"✅ All {total} tests passed! Installation successful.")
        print("\n🚀 You're ready to build your MVP!")
        print("\nNext steps:")
        print("  1. Import: from resonance_nn import create_spectral_lm")
        print("  2. Create model: model = create_spectral_lm('base', vocab_size=50257)")
        print("  3. See INSTALLATION_GUIDE.md for more examples")
        return 0
    else:
        print(f"⚠️  {passed}/{total} tests passed")
        print("\nPlease check the errors above and ensure:")
        print("  - Python >= 3.8")
        print("  - PyTorch >= 2.0.0")
        print("  - All dependencies installed")
        print("\nTry reinstalling: pip install --force-reinstall resonance_nn-0.1.0-py3-none-any.whl")
        return 1

if __name__ == '__main__':
    sys.exit(main())
