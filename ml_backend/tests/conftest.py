"""
Test configuration and fixtures for ML Backend tests.

This module sets up the Python path so that src can be imported
without requiring package installation in editable mode.
"""
import sys
import os
from pathlib import Path

# Add the ml_backend directory to Python path so 'src' can be imported
ml_backend_root = Path(__file__).parent.parent
if str(ml_backend_root) not in sys.path:
    sys.path.insert(0, str(ml_backend_root))

import pytest
import torch
import numpy as np


@pytest.fixture(scope="session")
def device():
    """Get the compute device for tests."""
    return torch.device("cuda" if torch.cuda.is_available() else "cpu")


@pytest.fixture
def sample_tensor():
    """Create a sample tensor for testing."""
    return torch.randn(10, 64)


@pytest.fixture
def sample_numpy_array():
    """Create a sample numpy array for testing."""
    return np.random.randn(100, 10).astype(np.float32)
