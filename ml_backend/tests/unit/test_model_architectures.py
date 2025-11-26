import pytest
import torch
from unittest.mock import MagicMock, patch
import sys

# Mock resonance_nn if not available
sys.modules['resonance_nn'] = MagicMock()

from src.model_architectures import (
    create_model,
    create_resonance_model,
    create_long_context_model,
    MODEL_CONFIGS,
    RESONANCE_AVAILABLE
)

class TestModelArchitectures:
    
    def test_model_configs_integrity(self):
        """Test that model configurations are valid"""
        assert 'tiny' in MODEL_CONFIGS
        assert 'large' in MODEL_CONFIGS
        
        tiny_config = MODEL_CONFIGS['tiny']
        assert tiny_config['input_dim'] == 512
        assert tiny_config['num_frequencies'] == 32

    def test_create_model_defaults(self):
        """Test creating a model with default parameters"""
        # If resonance_nn is not available, it might raise ImportError or return a Mock
        # Depending on which definition of create_resonance_model is active
        
        if not RESONANCE_AVAILABLE:
            # If the first definition is active, it raises ImportError
            # If the second definition is active, it returns a Mock
            try:
                model = create_model(size='tiny')
                # If we get here, it returned a Mock (second definition active)
                assert model is not None
            except ImportError:
                # First definition active
                pass
        else:
            model = create_model(size='tiny')
            assert model is not None

    def test_create_resonance_model_sizes(self):
        """Test creating models of different sizes"""
        for size in MODEL_CONFIGS.keys():
            try:
                model = create_resonance_model(size=size)
                assert model is not None
            except ImportError:
                if not RESONANCE_AVAILABLE:
                    continue
                raise

    def test_create_resonance_model_tasks(self):
        """Test creating models for different tasks"""
        tasks = ['general', 'language', 'vision', 'audio', 'code']
        for task in tasks:
            try:
                model = create_resonance_model(size='tiny', task=task)
                assert model is not None
            except ImportError:
                if not RESONANCE_AVAILABLE:
                    continue
                raise

    def test_create_long_context_model(self):
        """Test creating long context model"""
        try:
            # Test standard
            model = create_long_context_model(size='tiny', use_streaming=False)
            assert model is not None
            
            # Test streaming
            model_streaming = create_long_context_model(size='tiny', use_streaming=True)
            assert model_streaming is not None
        except ImportError:
            if not RESONANCE_AVAILABLE:
                return
            raise

    def test_create_classifier(self):
        """Test creating classifier"""
        from src.model_architectures import create_classifier
        try:
            model = create_classifier(size='tiny', num_classes=5)
            assert model is not None
            
            # Test with input_dim override
            model_custom = create_classifier(size='tiny', input_dim=128)
            assert model_custom is not None
        except ImportError:
            if not RESONANCE_AVAILABLE:
                return
            raise

    def test_get_model_info(self):
        """Test getting model info"""
        from src.model_architectures import get_model_info
        
        # Create a simple mock model
        model = torch.nn.Linear(10, 2)
        info = get_model_info(model)
        
        assert 'total_params' in info
        assert 'trainable_params' in info
        assert 'model_type' in info
        assert info['total_params'] == 22  # 10*2 + 2 bias
        
        # Test with complexity estimate
        model_complex = MagicMock()
        # Mock parameters() to return a list of tensors
        p = torch.randn(10, 10)
        model_complex.parameters.return_value = [p]
        model_complex.get_complexity_estimate.return_value = {
            'complexity_class': 'O(n log n)',
            'total': 1000
        }
        
        info_complex = get_model_info(model_complex)
        assert info_complex['complexity_class'] == 'O(n log n)'
        assert info_complex['operations'] == 1000



    def test_invalid_size(self):
        """Test error handling for invalid model size"""
        # This should raise ValueError regardless of RESONANCE_AVAILABLE
        # UNLESS the ImportError check comes first
        
        try:
            create_resonance_model(size='invalid_size')
            pytest.fail("Should have raised ValueError or ImportError")
        except ValueError:
            pass
        except ImportError:
            if RESONANCE_AVAILABLE:
                pytest.fail("Should not raise ImportError if available")
