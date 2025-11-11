"""
Multi-Scale Cascade Trainer
Uses Resonance Neural Networks (Frequency-Domain with Holographic Memory)
NO ATTENTION MECHANISM - Pure frequency-domain processing with O(n log n) complexity

Trains 18 models across 3 tiers:
- Tier 1: 10x tiny models (76M params) - Fast screening
- Tier 2: 5x small models (454M params) - Correlation analysis  
- Tier 3: 3x base models (983M params) - Final validation
"""

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel as DDP
import logging
from typing import Dict, List, Optional, Tuple, Iterator
from dataclasses import dataclass, asdict
import time
import numpy as np
from pathlib import Path
import asyncio
from datetime import datetime

# Import our custom architectures (Resonance NN from NEURON_NEW)
from src.model_architectures import (
    create_resonance_model,
    create_long_context_model,
    create_classifier,
    get_model_info,
    ResonanceNet,
    ResonanceLanguageModel,
    ResonanceClassifier,
    MODEL_CONFIGS
)

logger = logging.getLogger(__name__)


@dataclass
class ModelResult:
    """Results from training a single model"""
    tier: int
    variant: int
    model_size_params: int
    training_loss: float
    validation_loss: float
    training_time_seconds: float
    convergence_epoch: int
    collapse_detected: bool
    gradient_stats: Dict[str, float]
    loss_curve: List[float]
    spectral_metrics: Dict[str, float]  # FFT-specific metrics
    

@dataclass
class CascadeProgress:
    """Real-time progress update (streamed every 10 seconds)"""
    dataset_id: str
    validation_id: str
    current_tier: int
    current_variant: int
    models_completed: int
    models_total: int
    progress_percent: float
    current_model_result: Optional[ModelResult]
    estimated_completion: str
    gpu_utilization: Dict[int, float]
    current_loss: float
    

class CascadeTrainer:
    """
    Multi-scale cascade trainer using Resonance Neural Networks.
    
    Uses frequency-domain processing with O(n log n) complexity and holographic memory.
    Trains models in parallel across 4x H200 GPUs.
    """
    
    def __init__(
        self,
        dataset_id: str,
        validation_id: str,
        config: Dict,
        hardware_config: Dict,
        progress_callback: Optional[callable] = None
    ):
        self.dataset_id = dataset_id
        self.validation_id = validation_id
        self.config = config
        self.hardware_config = hardware_config
        self.progress_callback = progress_callback
        
        # GPU setup
        self.num_gpus = hardware_config['gpu_config']['num_gpus']
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Cascade configuration
        self.cascade_config = config['cascade_training']
        self.num_variants = config['cascade_training']['num_variants_per_tier']
        self.total_models = sum(self.num_variants.values())
        
        # Results storage
        self.results: List[ModelResult] = []
        self.completed_models = 0
        
        # Progress tracking
        self.start_time = None
        self.last_progress_update = time.time()
        
        logger.info(f"Initialized CascadeTrainer for {dataset_id}")
        logger.info(f"Total models to train: {self.total_models}")
        logger.info(f"Using {self.num_gpus}x H200 GPUs")
    
    async def train_cascade(
        self,
        train_data: torch.Tensor,
        val_data: torch.Tensor,
        vocab_size: int
    ) -> List[ModelResult]:
        """
        Train multi-scale cascade asynchronously.
        Returns list of all model results for collapse analysis.
        """
        self.start_time = time.time()
        logger.info(f"Starting cascade training at {datetime.now()}")
        
        # Train each tier sequentially (within tier: parallel)
        await self._train_tier_1(train_data, val_data, vocab_size)
        await self._train_tier_2(train_data, val_data, vocab_size)
        await self._train_tier_3(train_data, val_data, vocab_size)
        
        total_time = time.time() - self.start_time
        logger.info(f"Cascade training completed in {total_time/3600:.1f} hours")
        logger.info(f"Trained {len(self.results)} models successfully")
        
        return self.results
    
    async def _train_tier_1(
        self,
        train_data: torch.Tensor,
        val_data: torch.Tensor,
        vocab_size: int
    ):
        """
        Tier 1: Train 10x tiny models (76M params) in parallel
        Uses 2 GPUs, can train 5 models per GPU simultaneously
        """
        logger.info("=" * 80)
        logger.info("TIER 1: Training 10x Tiny Models (76M parameters)")
        logger.info("=" * 80)
        
        tier = 1
        num_variants = self.num_variants['tier_1']
        gpu_allocation = self.hardware_config['gpu_config']['tier_allocation']['tier_1_micro']
        
        # Create models
        models = []
        for variant_id in range(num_variants):
            model = create_resonance_model('tiny', vocab_size=vocab_size)
            models.append((variant_id, model))
        
        # Train in parallel batches (5 per GPU)
        batch_size = 5
        for i in range(0, num_variants, batch_size):
            batch_models = models[i:i+batch_size]
            
            # Train batch in parallel
            tasks = [
                self._train_single_model(
                    tier=tier,
                    variant_id=variant_id,
                    model=model,
                    train_data=train_data[:2_000_000],  # 2M rows for tier 1
                    val_data=val_data[:200_000],
                    gpu_id=gpu_allocation['gpus'][idx % len(gpu_allocation['gpus'])],
                    batch_size=gpu_allocation['batch_size']
                )
                for idx, (variant_id, model) in enumerate(batch_models)
            ]
            
            # Wait for batch to complete
            batch_results = await asyncio.gather(*tasks)
            self.results.extend(batch_results)
        
        logger.info(f"Tier 1 completed: {num_variants} models trained")
    
    async def _train_tier_2(
        self,
        train_data: torch.Tensor,
        val_data: torch.Tensor,
        vocab_size: int
    ):
        """
        Tier 2: Train 5x small models (454M params)
        Uses 3 GPUs for more memory-intensive models
        """
        logger.info("=" * 80)
        logger.info("TIER 2: Training 5x Small Models (454M parameters)")
        logger.info("=" * 80)
        
        tier = 2
        num_variants = self.num_variants['tier_2']
        gpu_allocation = self.hardware_config['gpu_config']['tier_allocation']['tier_2_mini']
        
        models = []
        for variant_id in range(num_variants):
            model = create_resonance_model('small', vocab_size=vocab_size)
            models.append((variant_id, model))
        
        # Train in parallel (distribute across 3 GPUs)
        tasks = [
            self._train_single_model(
                tier=tier,
                variant_id=variant_id,
                model=model,
                train_data=train_data[:10_000_000],  # 10M rows for tier 2
                val_data=val_data[:500_000],
                gpu_id=gpu_allocation['gpus'][variant_id % len(gpu_allocation['gpus'])],
                batch_size=gpu_allocation['batch_size']
            )
            for variant_id, model in models
        ]
        
        results = await asyncio.gather(*tasks)
        self.results.extend(results)
        
        logger.info(f"Tier 2 completed: {num_variants} models trained")
    
    async def _train_tier_3(
        self,
        train_data: torch.Tensor,
        val_data: torch.Tensor,
        vocab_size: int
    ):
        """
        Tier 3: Train 3x base models (983M params)
        Uses all 4 GPUs with model parallelism
        """
        logger.info("=" * 80)
        logger.info("TIER 3: Training 3x Base Models (983M parameters)")
        logger.info("=" * 80)
        
        tier = 3
        num_variants = self.num_variants['tier_3']
        gpu_allocation = self.hardware_config['gpu_config']['tier_allocation']['tier_3_medium']
        
        models = []
        for variant_id in range(num_variants):
            model = create_resonance_model('base', vocab_size=vocab_size)
            models.append((variant_id, model))
        
        # Train sequentially (each uses all 4 GPUs)
        for variant_id, model in models:
            result = await self._train_single_model(
                tier=tier,
                variant_id=variant_id,
                model=model,
                train_data=train_data,  # Full 20M rows
                val_data=val_data,
                gpu_id=None,  # Use all GPUs with DDP
                batch_size=gpu_allocation['batch_size'],
                use_ddp=True
            )
            self.results.append(result)
        
        logger.info(f"Tier 3 completed: {num_variants} models trained")
    
    async def _train_single_model(
        self,
        tier: int,
        variant_id: int,
        model: nn.Module,
        train_data: torch.Tensor,
        val_data: torch.Tensor,
        gpu_id: Optional[int],
        batch_size: int,
        use_ddp: bool = False
    ) -> ModelResult:
        """Train a single model and return results"""
        
        model_start_time = time.time()
        
        # Move model to GPU
        if use_ddp and self.num_gpus > 1:
            # Use all GPUs with DDP
            device = torch.device(f'cuda:0')
            model = model.to(device)
            model = DDP(model, device_ids=None)
        elif gpu_id is not None:
            device = torch.device(f'cuda:{gpu_id}')
            model = model.to(device)
        else:
            device = self.device
            model = model.to(device)
        
        logger.info(f"Training Tier {tier} Variant {variant_id} on {device}")
        
        # Setup training
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=self.cascade_config['learning_rate'],
            weight_decay=self.cascade_config['weight_decay']
        )
        
        criterion = nn.CrossEntropyLoss()
        
        # Create dataloaders
        train_dataset = TensorDataset(train_data[:-1], train_data[1:])
        train_loader = DataLoader(
            train_dataset,
            batch_size=batch_size,
            shuffle=True,
            pin_memory=True,
            num_workers=4
        )
        
        # Training loop
        loss_curve = []
        best_val_loss = float('inf')
        patience_counter = 0
        convergence_epoch = 0
        
        for epoch in range(self.cascade_config['max_epochs_per_model']):
            model.train()
            epoch_losses = []
            
            for batch_idx, (inputs, targets) in enumerate(train_loader):
                inputs, targets = inputs.to(device), targets.to(device)
                
                optimizer.zero_grad()
                
                # Forward pass (using FFT-based spectral processing)
                outputs = model(inputs)
                
                # Calculate loss
                loss = criterion(
                    outputs.view(-1, outputs.size(-1)),
                    targets.view(-1)
                )
                
                # Backward pass
                loss.backward()
                
                # Gradient clipping
                torch.nn.utils.clip_grad_norm_(
                    model.parameters(),
                    self.cascade_config['gradient_clip_norm']
                )
                
                optimizer.step()
                
                epoch_losses.append(loss.item())
                
                # Progress update every 10 seconds
                await self._maybe_send_progress_update(
                    tier, variant_id, loss.item(), device
                )
            
            # Epoch statistics
            avg_loss = np.mean(epoch_losses)
            loss_curve.append(avg_loss)
            
            # Validation
            val_loss = await self._validate_model(model, val_data, criterion, device)
            
            logger.info(
                f"Tier {tier} Variant {variant_id} Epoch {epoch+1}: "
                f"Train Loss={avg_loss:.4f}, Val Loss={val_loss:.4f}"
            )
            
            # Early stopping
            if val_loss < best_val_loss:
                best_val_loss = val_loss
                convergence_epoch = epoch + 1
                patience_counter = 0
            else:
                patience_counter += 1
            
            if patience_counter >= self.cascade_config['early_stopping_patience']:
                logger.info(f"Early stopping at epoch {epoch+1}")
                break
        
        # Calculate spectral metrics (FFT-specific)
        spectral_metrics = self._calculate_spectral_metrics(model, val_data, device)
        
        # Detect collapse signals
        collapse_detected = self._detect_collapse_signals(
            loss_curve, spectral_metrics
        )
        
        # Calculate gradient statistics
        gradient_stats = self._get_gradient_stats(model)
        
        training_time = time.time() - model_start_time
        
        # Create result
        result = ModelResult(
            tier=tier,
            variant=variant_id,
            model_size_params=sum(p.numel() for p in model.parameters()),
            training_loss=loss_curve[-1],
            validation_loss=best_val_loss,
            training_time_seconds=training_time,
            convergence_epoch=convergence_epoch,
            collapse_detected=collapse_detected,
            gradient_stats=gradient_stats,
            loss_curve=loss_curve,
            spectral_metrics=spectral_metrics
        )
        
        self.completed_models += 1
        
        # Send final progress update for this model
        await self._send_progress_update(tier, variant_id, 0.0, device, result)
        
        # Clear GPU cache
        if hasattr(model, 'module'):
            del model.module
        del model
        torch.cuda.empty_cache()
        
        return result
    
    async def _validate_model(
        self,
        model: nn.Module,
        val_data: torch.Tensor,
        criterion: nn.Module,
        device: torch.device
    ) -> float:
        """Validate model on validation set"""
        model.eval()
        val_losses = []
        
        with torch.no_grad():
            # Validate in batches
            batch_size = 128
            for i in range(0, len(val_data) - 1, batch_size):
                inputs = val_data[i:i+batch_size-1].to(device)
                targets = val_data[i+1:i+batch_size].to(device)
                
                outputs = model(inputs)
                loss = criterion(
                    outputs.view(-1, outputs.size(-1)),
                    targets.view(-1)
                )
                val_losses.append(loss.item())
        
        return np.mean(val_losses)
    
    def _calculate_spectral_metrics(
        self,
        model: nn.Module,
        val_data: torch.Tensor,
        device: torch.device
    ) -> Dict[str, float]:
        """
        Calculate FFT-specific spectral metrics.
        These replace attention-based metrics since we use FFT processing.
        """
        model.eval()
        
        with torch.no_grad():
            # Get sample batch
            sample = val_data[:1000].to(device)
            
            # Forward pass to get internal representations
            outputs = model(sample)
            
            # Calculate spectral characteristics
            # These are model-specific to spectral processing
            spectral_energy = outputs.abs().pow(2).mean().item()
            spectral_entropy = self._calculate_spectral_entropy(outputs)
            frequency_concentration = self._calculate_frequency_concentration(outputs)
            
        return {
            'spectral_energy': spectral_energy,
            'spectral_entropy': spectral_entropy,
            'frequency_concentration': frequency_concentration,
            'representation_variance': outputs.var().item()
        }
    
    def _calculate_spectral_entropy(self, outputs: torch.Tensor) -> float:
        """Calculate entropy of spectral representation"""
        # Normalize to probability distribution
        probs = torch.softmax(outputs, dim=-1)
        entropy = -(probs * torch.log(probs + 1e-10)).sum(dim=-1).mean()
        return entropy.item()
    
    def _calculate_frequency_concentration(self, outputs: torch.Tensor) -> float:
        """Calculate how concentrated energy is in frequency domain"""
        fft_output = torch.fft.rfft(outputs, dim=-1)
        power_spectrum = fft_output.abs().pow(2)
        # Calculate concentration (inverse of flatness)
        return (power_spectrum.max() / power_spectrum.mean()).item()
    
    def _detect_collapse_signals(
        self,
        loss_curve: List[float],
        spectral_metrics: Dict[str, float]
    ) -> bool:
        """
        Detect collapse signals from training dynamics.
        Uses FFT-specific metrics instead of attention patterns.
        """
        # Check for loss oscillations
        if len(loss_curve) > 3:
            loss_variance = np.var(loss_curve[-5:])
            if loss_variance > 1.0:  # High variance indicates instability
                return True
        
        # Check spectral entropy (low entropy = collapse)
        if spectral_metrics['spectral_entropy'] < 2.0:
            return True
        
        # Check frequency concentration (too high = mode collapse)
        if spectral_metrics['frequency_concentration'] > 100.0:
            return True
        
        return False
    
    def _get_gradient_stats(self, model: nn.Module) -> Dict[str, float]:
        """Get gradient statistics for collapse detection"""
        total_norm = 0.0
        max_grad = 0.0
        min_grad = float('inf')
        
        for p in model.parameters():
            if p.grad is not None:
                param_norm = p.grad.data.norm(2).item()
                total_norm += param_norm ** 2
                max_grad = max(max_grad, p.grad.data.abs().max().item())
                min_grad = min(min_grad, p.grad.data.abs().min().item())
        
        total_norm = total_norm ** 0.5
        
        return {
            'total_norm': total_norm,
            'max_grad': max_grad,
            'min_grad': min_grad,
            'grad_range': max_grad - min_grad
        }
    
    async def _maybe_send_progress_update(
        self,
        tier: int,
        variant_id: int,
        current_loss: float,
        device: torch.device,
        result: Optional[ModelResult] = None
    ):
        """Send progress update if 10 seconds elapsed"""
        current_time = time.time()
        
        if current_time - self.last_progress_update >= 10.0:
            await self._send_progress_update(
                tier, variant_id, current_loss, device, result
            )
            self.last_progress_update = current_time
    
    async def _send_progress_update(
        self,
        tier: int,
        variant_id: int,
        current_loss: float,
        device: torch.device,
        result: Optional[ModelResult] = None
    ):
        """Send real-time progress update"""
        if self.progress_callback is None:
            return
        
        # Calculate progress
        progress_percent = (self.completed_models / self.total_models) * 100
        
        # Estimate completion time
        if self.start_time and self.completed_models > 0:
            elapsed = time.time() - self.start_time
            avg_time_per_model = elapsed / self.completed_models
            remaining_models = self.total_models - self.completed_models
            eta_seconds = avg_time_per_model * remaining_models
            estimated_completion = datetime.now().timestamp() + eta_seconds
        else:
            estimated_completion = datetime.now().timestamp()
        
        # Get GPU utilization
        gpu_utilization = {}
        for gpu_id in range(self.num_gpus):
            try:
                torch.cuda.set_device(gpu_id)
                mem_allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
                mem_total = torch.cuda.get_device_properties(gpu_id).total_memory / (1024**3)
                gpu_utilization[gpu_id] = (mem_allocated / mem_total) * 100
            except:
                gpu_utilization[gpu_id] = 0.0
        
        progress = CascadeProgress(
            dataset_id=self.dataset_id,
            validation_id=self.validation_id,
            current_tier=tier,
            current_variant=variant_id,
            models_completed=self.completed_models,
            models_total=self.total_models,
            progress_percent=progress_percent,
            current_model_result=result,
            estimated_completion=datetime.fromtimestamp(estimated_completion).isoformat(),
            gpu_utilization=gpu_utilization,
            current_loss=current_loss
        )
        
        # Call async callback
        await self.progress_callback(progress)
