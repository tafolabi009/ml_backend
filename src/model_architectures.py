"""
Model Architecture Wrappers
============================

This module provides proper imports and wrappers for our custom architectures:
- Resonance NN (FFT-based spectral models)
- Temporal Eigenstate Networks

These are the actual custom models we use, not simplified versions.
"""

import sys
import os
import site
from pathlib import Path

# Import from resonance_nn package
from resonance_nn import (
    SpectralLanguageModel,
    SpectralConfig,
    SpectralEncoder,
    SpectralClassifier,
    CONFIGS as RESONANCE_CONFIGS,
    HierarchicalFFT,
    MultiHeadFrequencyLayer,
    AdvancedSpectralGating,
    OptimizedFFT,
    SpectralLayer,
    RotaryPositionEmbedding,
)

# Import from temporal_eigenstate_networks package
# This package installs as 'src' in site-packages which conflicts with our local src/
# Solution: Directly load the module file from site-packages
import importlib.util

_site_packages = site.getsitepackages()[0]
_ten_model_path = os.path.join(_site_packages, 'src', 'model.py')

if not os.path.exists(_ten_model_path):
    raise ImportError(
        "temporal_eigenstate_networks not installed. "
        "Install with: pip install packages/temporal_eigenstate_networks-0.1.0-py3-none-any.whl"
    )

# Load the module directly from file
_spec = importlib.util.spec_from_file_location("_temporal_eigenstate_model", _ten_model_path)
_ten_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ten_module)

# Extract the classes we need
TemporalEigenstateNetwork = _ten_module.TemporalEigenstateNetwork
TemporalEigenstateConfig = _ten_module.TemporalEigenstateConfig
HierarchicalTEN = _ten_module.HierarchicalTEN
TemporalFlowCell = _ten_module.TemporalFlowCell
ResonanceBlock = _ten_module.ResonanceBlock
TEN_Encoder = _ten_module.TEN_Encoder
TEN_TimeSeries = _ten_module.TEN_TimeSeries
TEN_MultiModal = _ten_module.TEN_MultiModal


# Model size configurations matching config/ml_config.yaml
MODEL_CONFIGS = {
    'tiny': {
        'resonance': RESONANCE_CONFIGS['tiny'],  # 76M params
        'context_length': 2048,
        'batch_size': 128,
    },
    'small': {
        'resonance': RESONANCE_CONFIGS['small'],  # 454M params
        'context_length': 4096,
        'batch_size': 64,
    },
    'base': {
        'resonance': RESONANCE_CONFIGS['base'],  # 983M params
        'context_length': 8192,
        'batch_size': 32,
    },
    'medium': {
        'resonance': RESONANCE_CONFIGS['medium'],  # 1.8B params
        'context_length': 16384,
        'batch_size': 16,
    },
    'large': {
        'resonance': RESONANCE_CONFIGS['large'],  # 3.9B params
        'context_length': 32768,
        'batch_size': 8,
    }
}


def create_resonance_model(size='tiny', vocab_size=50257, **kwargs):
    """
    Create a Resonance NN (FFT-based spectral) model.
    
    Args:
        size: Model size ('tiny', 'small', 'base', 'medium', 'large')
        vocab_size: Vocabulary size
        **kwargs: Additional config overrides
        
    Returns:
        SpectralLanguageModel instance
    """
    if size not in MODEL_CONFIGS:
        raise ValueError(f"Unknown model size: {size}. Choose from {list(MODEL_CONFIGS.keys())}")
    
    # Get base config (already a SpectralConfig object)
    base_config = MODEL_CONFIGS[size]['resonance']
    
    # Create a new config dict from the base config
    config_dict = {
        'embed_dim': base_config.embed_dim,
        'hidden_dim': base_config.hidden_dim,
        'num_layers': base_config.num_layers,
        'num_heads': base_config.num_heads,
        'vocab_size': vocab_size,  # Override vocab size
        'max_seq_len': base_config.max_seq_len,
        'dropout': base_config.dropout,
        'use_hierarchical_fft': base_config.use_hierarchical_fft,
        'use_rope': base_config.use_rope,
        'use_phase_aware': base_config.use_phase_aware,
        'layer_type': base_config.layer_type,
        'modality': base_config.modality,
    }
    
    # Apply any additional overrides
    config_dict.update(kwargs)
    
    # Create new SpectralConfig
    spectral_config = SpectralConfig(**config_dict)
    
    # Create model
    model = SpectralLanguageModel(spectral_config)
    
    return model


def create_temporal_eigenstate_model(
    d_model=512,
    n_layers=6,
    n_heads=8,
    d_ff=2048,
    vocab_size=50257,
    max_seq_len=2048,
    num_eigenstates=16,
    **kwargs
):
    """
    Create a Temporal Eigenstate Network for time-series data.
    
    Args:
        d_model: Model dimension
        n_layers: Number of layers
        n_heads: Number of attention heads
        d_ff: Feed-forward dimension
        vocab_size: Vocabulary size
        max_seq_len: Maximum sequence length
        num_eigenstates: Number of temporal eigenstates
        **kwargs: Additional config overrides
        
    Returns:
        TemporalEigenstateNetwork instance
    """
    config = TemporalEigenstateConfig(
        d_model=d_model,
        n_heads=n_heads,
        n_layers=n_layers,
        d_ff=d_ff,
        vocab_size=vocab_size,
        max_seq_len=max_seq_len,
        num_eigenstates=num_eigenstates,
        **kwargs
    )
    
    model = TemporalEigenstateNetwork(config)
    
    return model


def get_model_info(model):
    """
    Get information about a model.
    
    Args:
        model: PyTorch model
        
    Returns:
        Dict with model info
    """
    import torch
    
    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    
    return {
        'total_params': total_params,
        'trainable_params': trainable_params,
        'model_type': type(model).__name__,
        'size_mb': total_params * 4 / (1024 * 1024),  # Assuming float32
    }


__all__ = [
    # Resonance NN
    'SpectralLanguageModel',
    'SpectralConfig',
    'SpectralEncoder',
    'SpectralClassifier',
    'HierarchicalFFT',
    'MultiHeadFrequencyLayer',
    'AdvancedSpectralGating',
    'OptimizedFFT',
    'SpectralLayer',
    'RotaryPositionEmbedding',
    'RESONANCE_CONFIGS',
    
    # Temporal Eigenstate Networks
    'TemporalEigenstateNetwork',
    'TemporalEigenstateConfig',
    'HierarchicalTEN',
    'TemporalFlowCell',
    'ResonanceBlock',
    'TEN_Encoder',
    'TEN_TimeSeries',
    'TEN_MultiModal',
    
    # Helper functions
    'create_resonance_model',
    'create_temporal_eigenstate_model',
    'get_model_info',
    'MODEL_CONFIGS',
]
