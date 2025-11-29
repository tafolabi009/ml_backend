"""
Test configuration and fixtures for Validation Service tests.
"""
import sys
from pathlib import Path

# Add validation_service to path so modules can be imported
validation_service_root = Path(__file__).parent.parent
if str(validation_service_root) not in sys.path:
    sys.path.insert(0, str(validation_service_root))

import pytest
import numpy as np
import torch


@pytest.fixture(scope="session")
def device():
    """Get the compute device for tests."""
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    np.random.seed(42)
    return pd.DataFrame({
        'feature_1': np.random.randn(1000),
        'feature_2': np.random.randn(1000),
        'feature_3': np.random.randint(0, 10, 1000),
        'category': np.random.choice(['A', 'B', 'C'], 1000),
    })


@pytest.fixture
def sample_config():
    """Create a sample cascade training configuration."""
    return {
        'cascade_training': {
            'num_variants_per_tier': {
                'tier_1': 2,
                'tier_2': 1,
                'tier_3': 1
            },
            'max_epochs': 2,
            'max_epochs_per_model': 1,
            'batch_size': 4,
            'learning_rate': 0.001,
            'weight_decay': 0.01,
            'gradient_clip_norm': 1.0,
            'early_stopping_patience': 1,
            'tiers': {
                'tier_1': {'size': 'tiny', 'variants': 2},
                'tier_2': {'size': 'small', 'variants': 1},
                'tier_3': {'size': 'base', 'variants': 1}
            }
        }
    }


@pytest.fixture
def hardware_config():
    """Create a sample hardware configuration."""
    return {
        'gpu_config': {
            'num_gpus': 1,
            'memory_per_gpu': 16.0,
        }
    }
