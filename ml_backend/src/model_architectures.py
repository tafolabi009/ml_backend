"""
Model Architecture Wrappers
============================

This module provides proper imports and wrappers for our custom architecture:
- Resonance Neural Networks (Frequency-Domain with Holographic Memory)

This uses the NEURON_NEW architecture which is a revolutionary neural architecture
that replaces attention mechanisms with frequency-domain processing, achieving
O(n log n) complexity with holographic memory integration.

Key Features:
- O(n log n) Complexity (vs O(n²) for transformers)
- Ultra-Long Context (260K-300K tokens)
- Holographic Memory with provable capacity
- 4-6x Parameter Efficiency
- No Attention Mechanism - Pure frequency processing
"""

import torch
from typing import Optional, Dict, Any

# Import from resonance_nn package (NEURON_NEW)
try:
    from resonance_nn import (
        # Core models
        ResonanceNet,
        ResonanceEncoder,
        ResonanceAutoencoder,
        ResonanceClassifier,
        
        # Specialized models
        ResonanceLanguageModel,
        ResonanceCausalLM,
        ResonanceCodeModel,
        ResonanceVisionModel,
        ResonanceAudioModel,
        
        # Long context models
        LongContextResonanceNet,
        StreamingLongContextNet,
        
        # Core layers
        ResonanceLayer,
        MultiScaleResonanceLayer,
        AdaptiveResonanceLayer,
        ComplexWeight,
        
        # Holographic Memory
        HolographicMemory,
        
        # Embeddings
        HierarchicalVocabularyEmbedding,
        FrequencyCompressedEmbedding,
        AdaptiveEmbedding,
        ResonanceHashEmbedding,
        FrequencyPositionalEncoding,
        
        # Training
        ResonanceTrainer,
        ResonanceAutoEncoderTrainer,
        ResonanceClassifierTrainer,
        create_criterion,
        create_trainer,
        
        # Multimodal
        ResonanceVisionEncoder,
        ResonanceAudioEncoder,
        MultiModalResonanceFusion,
        CrossModalResonance,
        HolographicModalityBinder,
    )
    RESONANCE_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import resonance_nn: {e}")
    print("Install with: pip install git+https://github.com/tafolabi009/NEURON_NEW.git")
    print("Using mock implementations for testing...")
    RESONANCE_AVAILABLE = False
    
    # Create mock classes when resonance_nn not available
    class MockResonanceModule(torch.nn.Module):
        """Mock implementation when resonance_nn is not installed"""
        def __init__(self, *args, **kwargs):
            super().__init__()
            self.mock = True
            
        def forward(self, x):
            return x
    
    # Mock all imports
    ResonanceNet = MockResonanceModule
    ResonanceEncoder = MockResonanceModule
    ResonanceAutoencoder = MockResonanceModule
    ResonanceClassifier = MockResonanceModule
    ResonanceLanguageModel = MockResonanceModule
    ResonanceCausalLM = MockResonanceModule
    ResonanceCodeModel = MockResonanceModule
    ResonanceVisionModel = MockResonanceModule
    ResonanceAudioModel = MockResonanceModule
    LongContextResonanceNet = MockResonanceModule
    StreamingLongContextNet = MockResonanceModule
    ResonanceLayer = MockResonanceModule
    MultiScaleResonanceLayer = MockResonanceModule
    AdaptiveResonanceLayer = MockResonanceModule
    ComplexWeight = MockResonanceModule
    HolographicMemory = MockResonanceModule
    HierarchicalVocabularyEmbedding = MockResonanceModule
    FrequencyCompressedEmbedding = MockResonanceModule
    AdaptiveEmbedding = MockResonanceModule
    ResonanceHashEmbedding = MockResonanceModule
    FrequencyPositionalEncoding = MockResonanceModule
    ResonanceTrainer = object
    ResonanceAutoEncoderTrainer = object
    ResonanceClassifierTrainer = object
    ResonanceVisionEncoder = MockResonanceModule
    ResonanceAudioEncoder = MockResonanceModule
    MultiModalResonanceFusion = MockResonanceModule
    CrossModalResonance = MockResonanceModule
    HolographicModalityBinder = MockResonanceModule
    
    def create_criterion(*args, **kwargs):
        return torch.nn.MSELoss()
    
    def create_trainer(*args, **kwargs):
        return None


# Model size configurations matching config/ml_config.yaml
# Based on Resonance NN paper specifications
MODEL_CONFIGS = {
    'tiny': {
        'input_dim': 512,
        'num_frequencies': 32,
        'hidden_dim': 512,
        'num_layers': 4,
        'holographic_capacity': 100,
        'dropout': 0.1,
        'context_length': 2048,
        'batch_size': 128,
        'params': '~76M',  # Estimated
    },
    'small': {
        'input_dim': 1024,
        'num_frequencies': 64,
        'hidden_dim': 1024,
        'num_layers': 8,
        'holographic_capacity': 500,
        'dropout': 0.1,
        'context_length': 4096,
        'batch_size': 64,
        'params': '~454M',  # Estimated
    },
    'base': {
        'input_dim': 2048,
        'num_frequencies': 128,
        'hidden_dim': 2048,
        'num_layers': 12,
        'holographic_capacity': 1000,
        'dropout': 0.1,
        'context_length': 8192,
        'batch_size': 32,
        'params': '~983M',  # Estimated
    },
    'medium': {
        'input_dim': 3072,
        'num_frequencies': 192,
        'hidden_dim': 3072,
        'num_layers': 16,
        'holographic_capacity': 2000,
        'dropout': 0.1,
        'context_length': 16384,
        'batch_size': 16,
        'params': '~1.8B',  # Estimated
    },
    'large': {
        'input_dim': 4096,
        'num_frequencies': 256,
        'hidden_dim': 4096,
        'num_layers': 24,
        'holographic_capacity': 5000,
        'dropout': 0.1,
        'context_length': 32768,
        'batch_size': 8,
        'params': '~3.9B',  # Estimated
    }
}


def create_resonance_model(
    size='tiny',
    task='general',
    vocab_size=50257,
    use_memory=True,
    device='cpu',
    **kwargs
):
    """
    Create a Resonance Neural Network model with O(n log n) complexity.
    
    This creates the frequency-domain neural network from the NEURON_NEW architecture,
    which replaces attention mechanisms with FFT-based processing and holographic memory.
    
    Args:
        size: Model size ('tiny', 'small', 'base', 'medium', 'large')
        task: Task type ('general', 'language', 'vision', 'audio', 'code')
        vocab_size: Vocabulary size (for language models)
        use_memory: Enable holographic memory
        device: Device to place model on
        **kwargs: Additional config overrides
        
    Returns:
        ResonanceNet or specialized model instance
    """
    if not RESONANCE_AVAILABLE:
        raise ImportError("resonance_nn package not available. Install with: pip install git+https://github.com/tafolabi009/NEURON_NEW.git")
        
    if size not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model size: {size}. Choose from {list(MODEL_CONFIGS.keys())}")
    
    config = MODEL_CONFIGS[size].copy()
    config.update(kwargs)
    
    # Remove non-model params
    context_length = config.pop('context_length', 2048)
    batch_size = config.pop('batch_size', 32)
    params_est = config.pop('params', 'unknown')
    
    # Create appropriate model based on task
    if task == 'language':
        model = ResonanceLanguageModel(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
            max_seq_length=context_length,
        )
    elif task == 'code':
        model = ResonanceCodeModel(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
            max_seq_length=context_length,
        )
    elif task == 'vision':
        model = ResonanceVisionModel(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    elif task == 'audio':
        model = ResonanceAudioModel(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    else:  # general
        model = ResonanceNet(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    
    model = model.to(device)
    
    return model


def create_model(size='tiny', task='general', **kwargs):
    """
    Convenience wrapper for create_resonance_model.
    
    This is the main entry point for creating models in the ml_backend project.
    """
    return create_resonance_model(size=size, task=task, **kwargs)


def create_resonance_model(
    size='tiny',
    task='general',
    vocab_size=50257,
    use_memory=True,
    device='cpu',
    **kwargs
):
    """
    Create a Resonance Neural Network model with O(n log n) complexity.
    
    This creates the frequency-domain neural network from the NEURON_NEW architecture,
    which replaces attention mechanisms with FFT-based processing and holographic memory.
    
    Args:
        size: Model size ('tiny', 'small', 'base', 'medium', 'large')
        task: Task type ('general', 'language', 'vision', 'audio', 'code')
        vocab_size: Vocabulary size (for language models)
        use_memory: Enable holographic memory
        device: Device to place model on
        **kwargs: Additional config overrides
        
    Returns:
        ResonanceNet or specialized model instance
    """
    if size not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model size: {size}. Choose from {list(MODEL_CONFIGS.keys())}")
    
    config = MODEL_CONFIGS[size].copy()
    config.update(kwargs)
    
    # Remove non-model params
    context_length = config.pop('context_length', 2048)
    batch_size = config.pop('batch_size', 32)
    params_est = config.pop('params', 'unknown')
    
    # Create appropriate model based on task
    if task == 'language':
        model = ResonanceLanguageModel(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
            max_seq_length=context_length,
        )
    elif task == 'code':
        model = ResonanceCodeModel(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
            max_seq_length=context_length,
        )
    elif task == 'vision':
        model = ResonanceVisionModel(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    elif task == 'audio':
        model = ResonanceAudioModel(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    else:  # general
        model = ResonanceNet(
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'] if use_memory else 0,
            dropout=config['dropout'],
        )
    
    model = model.to(device)
    
    return model


def create_long_context_model(
    size='tiny',
    vocab_size=50257,
    max_seq_length=262144,  # 256K tokens
    use_streaming=False,
    device='cpu',
    **kwargs
):
    """
    Create a Long Context Resonance Network for ultra-long sequences.
    
    Supports up to 260K-300K tokens through hierarchical chunking.
    
    Args:
        size: Model size ('tiny', 'small', 'base', 'medium', 'large')
        vocab_size: Vocabulary size
        max_seq_length: Maximum sequence length (default: 256K)
        use_streaming: Use streaming variant for memory efficiency
        device: Device to place model on
        **kwargs: Additional config overrides
        
    Returns:
        LongContextResonanceNet or StreamingLongContextNet instance
    """
    if size not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model size: {size}. Choose from {list(MODEL_CONFIGS.keys())}")
    
    config = MODEL_CONFIGS[size].copy()
    config.update(kwargs)
    
    # Remove non-model params
    config.pop('context_length', None)
    config.pop('batch_size', None)
    config.pop('params', None)
    
    if use_streaming:
        model = StreamingLongContextNet(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'],
            dropout=config['dropout'],
            max_seq_length=max_seq_length,
        )
    else:
        model = LongContextResonanceNet(
            vocab_size=vocab_size,
            input_dim=config['input_dim'],
            num_frequencies=config['num_frequencies'],
            hidden_dim=config['hidden_dim'],
            num_layers=config['num_layers'],
            holographic_capacity=config['holographic_capacity'],
            dropout=config['dropout'],
            max_seq_length=max_seq_length,
        )
    
    model = model.to(device)
    
    return model


def create_classifier(
    size='tiny',
    num_classes=2,
    input_dim=None,
    device='cpu',
    **kwargs
):
    """
    Create a Resonance Classifier for classification tasks.
    
    Args:
        size: Model size ('tiny', 'small', 'base', 'medium', 'large')
        num_classes: Number of output classes
        input_dim: Input dimension (overrides size config)
        device: Device to place model on
        **kwargs: Additional config overrides
        
    Returns:
        ResonanceClassifier instance
    """
    if size not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model size: {size}. Choose from {list(MODEL_CONFIGS.keys())}")
    
    config = MODEL_CONFIGS[size].copy()
    config.update(kwargs)
    
    # Remove non-model params
    config.pop('context_length', None)
    config.pop('batch_size', None)
    config.pop('params', None)
    config.pop('holographic_capacity', None)  # Classifier doesn't use holographic memory
    
    if input_dim is not None:
        config['input_dim'] = input_dim
    
    model = ResonanceClassifier(
        input_dim=config['input_dim'],
        num_frequencies=config['num_frequencies'],
        hidden_dim=config['hidden_dim'],
        num_layers=config['num_layers'],
        num_classes=num_classes,
        dropout=config['dropout'],
    )
    
    model = model.to(device)
    
    return model


def get_model_info(model):
    """
    Get information about a model.
    
    Args:
        model: PyTorch model
        
    Returns:
        Dict with model info
    """
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    
    info = {
        'total_params': total_params,
        'trainable_params': trainable_params,
        'model_type': type(model).__name__,
        'size_mb': total_params * 4 / (1024 * 1024),  # Assuming float32
    }
    
    # Try to get complexity estimate if available
    if hasattr(model, 'get_complexity_estimate'):
        try:
            complexity = model.get_complexity_estimate(1024)  # Estimate for 1K tokens
            info['complexity_class'] = complexity.get('complexity_class', 'Unknown')
            info['operations'] = complexity.get('total', 0)
        except:
            pass
    
    return info


__all__ = [
    # Core models
    'ResonanceNet',
    'ResonanceEncoder',
    'ResonanceAutoencoder',
    'ResonanceClassifier',
    
    # Specialized models
    'ResonanceLanguageModel',
    'ResonanceCausalLM',
    'ResonanceCodeModel',
    'ResonanceVisionModel',
    'ResonanceAudioModel',
    
    # Long context models
    'LongContextResonanceNet',
    'StreamingLongContextNet',
    
    # Core layers
    'ResonanceLayer',
    'MultiScaleResonanceLayer',
    'AdaptiveResonanceLayer',
    'ComplexWeight',
    
    # Holographic Memory
    'HolographicMemory',
    
    # Embeddings
    'HierarchicalVocabularyEmbedding',
    'FrequencyCompressedEmbedding',
    'AdaptiveEmbedding',
    'ResonanceHashEmbedding',
    'FrequencyPositionalEncoding',
    
    # Training
    'ResonanceTrainer',
    'ResonanceAutoEncoderTrainer',
    'ResonanceClassifierTrainer',
    'create_criterion',
    'create_trainer',
    
    # Multimodal
    'ResonanceVisionEncoder',
    'ResonanceAudioEncoder',
    'MultiModalResonanceFusion',
    'CrossModalResonance',
    'HolographicModalityBinder',
    
    # Helper functions
    'create_resonance_model',
    'create_long_context_model',
    'create_classifier',
    'get_model_info',
    'MODEL_CONFIGS',
]
