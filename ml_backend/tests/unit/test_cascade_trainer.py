import pytest
import torch
import torch.nn as nn
from unittest.mock import MagicMock, patch, AsyncMock
import asyncio
import numpy as np

from src.validation_engine.cascade_trainer import CascadeTrainer, ModelResult, CascadeProgress

class TestCascadeTrainer:
    
    @pytest.fixture
    def config(self):
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
    def hardware_config(self):
        return {
            'gpu_config': {
                'num_gpus': 1,
                'gpu_ids': [0],
                'tier_allocation': {
                    'tier_1_micro': {'gpus': [0], 'batch_size': 4},
                    'tier_2_mini': {'gpus': [0], 'batch_size': 4},
                    'tier_3_medium': {'gpus': [0], 'batch_size': 4}
                }
            }
        }
        
    @pytest.fixture
    def trainer(self, config, hardware_config):
        with patch('src.validation_engine.cascade_trainer.torch.cuda.is_available', return_value=False):
            return CascadeTrainer(
                dataset_id="ds_1",
                validation_id="val_1",
                config=config,
                hardware_config=hardware_config
            )

    def test_initialization(self, trainer):
        assert trainer.dataset_id == "ds_1"
        assert trainer.validation_id == "val_1"
        assert trainer.num_gpus == 1
        assert trainer.device.type == 'cpu'

    @pytest.mark.asyncio
    async def test_train_cascade_mock(self, trainer):
        # Mock data
        vocab_size = 100
        train_data = torch.randint(0, vocab_size, (100, 10))
        val_data = torch.randint(0, vocab_size, (20, 10))
        
        # Mock model creation
        mock_model = MagicMock(spec=nn.Module)
        mock_model.parameters.return_value = [torch.randn(10, 10)]
        mock_model.train.return_value = None
        mock_model.eval.return_value = None
        mock_model.to.return_value = mock_model
        # Mock forward pass
        def model_forward(x):
            batch_size = x.shape[0]
            seq_len = x.shape[1] if len(x.shape) > 1 else 10
            return torch.randn(batch_size, seq_len, vocab_size, requires_grad=True)
        mock_model.side_effect = model_forward
        # mock_model.return_value = torch.randn(4, 10, 100, requires_grad=True) # batch, seq, vocab
        
        # Mock optimizer
        mock_optimizer = MagicMock()
        
        with patch('src.validation_engine.cascade_trainer.create_resonance_model', return_value=mock_model), \
             patch('src.validation_engine.cascade_trainer.torch.optim.AdamW', return_value=mock_optimizer), \
             patch('src.validation_engine.cascade_trainer.DataLoader') as mock_loader, \
             patch('src.validation_engine.cascade_trainer.TensorDataset') as mock_dataset, \
             patch('src.validation_engine.cascade_trainer.torch.device', return_value=torch.device('cpu')):
            
            # Mock loader iteration
            # Inputs: (batch, seq), Targets: (batch, seq)
            # Targets must be Long for CrossEntropyLoss
            mock_loader.return_value.__iter__.return_value = [
                (torch.randn(4, 10), torch.randint(0, 100, (4, 10)))
            ]
            mock_loader.return_value.__len__.return_value = 1
            
            results = await trainer.train_cascade(train_data, val_data, vocab_size)
            
            assert len(results) > 0
            assert isinstance(results[0], ModelResult)
            assert results[0].tier == 1

    def test_detect_collapse_signals(self, trainer):
        # Test with decreasing loss (no collapse)
        loss_curve = [1.0, 0.8, 0.6, 0.4]
        # Entropy > 2.0 for healthy
        metrics = {'spectral_entropy': 3.0, 'frequency_concentration': 10.0}
        assert not trainer._detect_collapse_signals(loss_curve, metrics)
        
        # Test with increasing loss (collapse) - high variance
        loss_curve_bad = [0.4, 2.0, 0.8, 3.0]
        assert trainer._detect_collapse_signals(loss_curve_bad, metrics)
        
        # Test with low entropy (collapse)
        metrics_bad = {'spectral_entropy': 1.0, 'frequency_concentration': 10.0}
        assert trainer._detect_collapse_signals(loss_curve, metrics_bad)

    def test_calculate_spectral_metrics(self, trainer):
        model = MagicMock()
        # Mock parameters as list of tensors
        model.parameters.return_value = [torch.randn(10, 10)]
        # Mock forward pass
        model.return_value = torch.randn(10, 100)
        
        val_data = torch.randn(20, 10)
        device = torch.device('cpu')
        
        metrics = trainer._calculate_spectral_metrics(model, val_data, device)
        assert 'spectral_entropy' in metrics
        assert 'frequency_concentration' in metrics
