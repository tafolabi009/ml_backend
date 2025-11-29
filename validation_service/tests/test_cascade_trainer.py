"""
Unit tests for CascadeTrainer.

Tests the multi-scale cascade training functionality using
Resonance Neural Networks.
"""
import pytest
import torch
import numpy as np
from unittest.mock import MagicMock, patch, AsyncMock
from dataclasses import asdict

# Note: CascadeTrainer imports model_architectures which may not be available
# in validation_service. These tests mock the heavy dependencies.


class TestModelResult:
    """Tests for ModelResult dataclass."""

    def test_model_result_creation(self):
        """ModelResult should store all training results."""
        # Import here to avoid import errors if module not available
        try:
            from validation_engine.cascade_trainer import ModelResult
        except ImportError:
            pytest.skip("CascadeTrainer not available - model_architectures dependency")
        
        result = ModelResult(
            tier=1,
            variant=0,
            model_size_params=76_000_000,
            training_loss=0.05,
            validation_loss=0.06,
            training_time_seconds=120.5,
            convergence_epoch=10,
            collapse_detected=False,
            gradient_stats={'mean': 0.01, 'std': 0.001},
            loss_curve=[0.5, 0.3, 0.1, 0.06],
            spectral_metrics={'fft_power': 0.8}
        )
        
        assert result.tier == 1
        assert result.variant == 0
        assert result.model_size_params == 76_000_000
        assert result.training_loss == 0.05
        assert result.collapse_detected is False
        assert len(result.loss_curve) == 4

    def test_model_result_to_dict(self):
        """ModelResult should convert to dictionary."""
        try:
            from validation_engine.cascade_trainer import ModelResult
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        result = ModelResult(
            tier=1, variant=0, model_size_params=1000,
            training_loss=0.1, validation_loss=0.2,
            training_time_seconds=10.0, convergence_epoch=5,
            collapse_detected=False, gradient_stats={},
            loss_curve=[], spectral_metrics={}
        )
        
        result_dict = asdict(result)
        assert 'tier' in result_dict
        assert 'training_loss' in result_dict


class TestCascadeProgress:
    """Tests for CascadeProgress dataclass."""

    def test_cascade_progress_creation(self):
        """CascadeProgress should track training progress."""
        try:
            from validation_engine.cascade_trainer import CascadeProgress
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        progress = CascadeProgress(
            dataset_id='ds_123',
            validation_id='val_456',
            current_tier=1,
            current_variant=2,
            models_completed=5,
            models_total=18,
            progress_percent=27.8,
            current_model_result=None,
            estimated_completion='2025-01-01T12:00:00',
            gpu_utilization={0: 85.5, 1: 90.2},
            current_loss=0.05
        )
        
        assert progress.dataset_id == 'ds_123'
        assert progress.models_completed == 5
        assert progress.models_total == 18
        assert 0 in progress.gpu_utilization


class TestCascadeTrainerInit:
    """Tests for CascadeTrainer initialization."""

    def test_trainer_initialization(self, sample_config, hardware_config):
        """CascadeTrainer should initialize with valid config."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available - model_architectures dependency")
        
        with patch('validation_engine.cascade_trainer.create_resonance_model'):
            trainer = CascadeTrainer(
                dataset_id='ds_test',
                validation_id='val_test',
                config=sample_config,
                hardware_config=hardware_config,
                progress_callback=None
            )
            
            assert trainer.dataset_id == 'ds_test'
            assert trainer.validation_id == 'val_test'
            assert trainer.num_gpus == 1

    def test_trainer_with_progress_callback(self, sample_config, hardware_config):
        """CascadeTrainer should accept progress callback."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        callback = MagicMock()
        
        with patch('validation_engine.cascade_trainer.create_resonance_model'):
            trainer = CascadeTrainer(
                dataset_id='ds_test',
                validation_id='val_test',
                config=sample_config,
                hardware_config=hardware_config,
                progress_callback=callback
            )
            
            assert trainer.progress_callback is callback


class TestCascadeTrainerDevice:
    """Tests for device selection in CascadeTrainer."""

    def test_trainer_uses_cuda_when_available(self, sample_config, hardware_config):
        """Trainer should use CUDA when available."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        with patch('torch.cuda.is_available', return_value=True):
            with patch('validation_engine.cascade_trainer.create_resonance_model'):
                trainer = CascadeTrainer(
                    dataset_id='ds_test',
                    validation_id='val_test',
                    config=sample_config,
                    hardware_config=hardware_config
                )
                assert str(trainer.device) == 'cuda'

    def test_trainer_falls_back_to_cpu(self, sample_config, hardware_config):
        """Trainer should fall back to CPU when CUDA unavailable."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        with patch('torch.cuda.is_available', return_value=False):
            with patch('validation_engine.cascade_trainer.create_resonance_model'):
                trainer = CascadeTrainer(
                    dataset_id='ds_test',
                    validation_id='val_test',
                    config=sample_config,
                    hardware_config=hardware_config
                )
                assert str(trainer.device) == 'cpu'


class TestTierConfiguration:
    """Tests for cascade tier configuration."""

    def test_tier_config_parsing(self, sample_config, hardware_config):
        """Trainer should parse tier configuration correctly."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        with patch('validation_engine.cascade_trainer.create_resonance_model'):
            trainer = CascadeTrainer(
                dataset_id='ds_test',
                validation_id='val_test',
                config=sample_config,
                hardware_config=hardware_config
            )
            
            cascade_config = trainer.cascade_config
            assert 'tiers' in cascade_config or 'num_variants_per_tier' in cascade_config


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_gpu_config(self, sample_config):
        """Trainer should handle minimal GPU config."""
        try:
            from validation_engine.cascade_trainer import CascadeTrainer
        except ImportError:
            pytest.skip("CascadeTrainer not available")
        
        minimal_hardware = {'gpu_config': {'num_gpus': 0}}
        
        with patch('torch.cuda.is_available', return_value=False):
            with patch('validation_engine.cascade_trainer.create_resonance_model'):
                trainer = CascadeTrainer(
                    dataset_id='ds_test',
                    validation_id='val_test',
                    config=sample_config,
                    hardware_config=minimal_hardware
                )
                assert trainer.num_gpus == 0
