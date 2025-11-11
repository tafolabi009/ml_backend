"""
GPU Optimizer - Maximize GPU utilization for massive datasets
Target: >80% GPU utilization across 4x H200 GPUs

Features:
- Mixed precision training (FP16/BF16)
- Gradient checkpointing
- Kernel fusion optimization
- Multi-GPU orchestration
- Real-time monitoring
"""

import torch
import torch.nn as nn
from torch.cuda.amp import autocast, GradScaler
from torch.nn.parallel import DistributedDataParallel as DDP
import torch.distributed as dist
from typing import Optional, Dict, Any, List
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class GPUMetrics:
    """GPU utilization metrics"""
    gpu_id: int
    utilization_percent: float
    memory_used_gb: float
    memory_total_gb: float
    memory_percent: float
    temperature_c: float
    power_watts: float


@dataclass
class OptimizationConfig:
    """GPU optimization configuration"""
    use_mixed_precision: bool = True
    precision: str = "bf16"  # "fp16" or "bf16"
    gradient_checkpointing: bool = True
    compile_model: bool = True  # PyTorch 2.0+ compilation
    use_flash_attention: bool = False  # Not applicable for FFT-based models
    channels_last: bool = True  # Memory format optimization
    cudnn_benchmark: bool = True
    tf32_enabled: bool = True  # TensorFloat-32 on Ampere+
    num_workers: int = 8  # DataLoader workers
    pin_memory: bool = True
    prefetch_factor: int = 2


class GPUOptimizer:
    """
    Optimizes GPU utilization for maximum throughput.
    Targets >80% utilization across 4x H200 GPUs.
    """
    
    def __init__(
        self, 
        config: Optional[OptimizationConfig] = None,
        # Convenience parameters (will be used if config is None)
        memory_fraction: Optional[float] = None,
        enable_mixed_precision: Optional[bool] = None,
        use_gpu: bool = True
    ):
        # Handle both config object and convenience parameters
        if config is None:
            config = OptimizationConfig()
            if enable_mixed_precision is not None:
                config.use_mixed_precision = enable_mixed_precision
        
        self.config = config
        self.use_gpu = use_gpu and torch.cuda.is_available()
        
        # Only apply GPU settings if GPU is available
        if not self.use_gpu:
            logger.info("GPUOptimizer running in CPU mode")
            self.scaler = None
            return
        
        # Set memory fraction if specified
        if memory_fraction is not None and self.use_gpu:
            torch.cuda.set_per_process_memory_fraction(memory_fraction)
        
        # Enable TF32 for faster matrix operations on Ampere+
        if self.config.tf32_enabled and self.use_gpu:
            torch.backends.cuda.matmul.allow_tf32 = True
            torch.backends.cudnn.allow_tf32 = True
        
        # Enable cuDNN benchmarking for optimal kernels
        if self.config.cudnn_benchmark:
            torch.backends.cudnn.benchmark = True
        
        # Initialize grad scaler for mixed precision
        if self.config.use_mixed_precision and self.use_gpu:
            self.scaler = GradScaler()
        else:
            self.scaler = None
        
        logger.info(f"GPUOptimizer initialized: {self.config.precision} precision, "
                   f"checkpointing={'ON' if self.config.gradient_checkpointing else 'OFF'}, "
                   f"GPU={'ON' if self.use_gpu else 'OFF (CPU mode)'}")
    
    def optimize_model(self, model: nn.Module, distributed: bool = False) -> nn.Module:
        """
        Apply optimization techniques to model.
        
        Args:
            model: PyTorch model
            distributed: Whether to use DDP
        
        Returns:
            Optimized model
        """
        logger.info("Optimizing model for GPU performance...")
        
        # 1. Apply gradient checkpointing if enabled
        if self.config.gradient_checkpointing:
            model = self._apply_gradient_checkpointing(model)
        
        # 2. Convert to channels-last memory format (for conv layers)
        if self.config.channels_last:
            try:
                model = model.to(memory_format=torch.channels_last)
                logger.info("Converted model to channels_last memory format")
            except:
                pass  # Not all models support this
        
        # 3. Compile model with PyTorch 2.0+ (if available)
        if self.config.compile_model and hasattr(torch, 'compile'):
            try:
                model = torch.compile(model, mode="max-autotune")
                logger.info("Compiled model with torch.compile")
            except Exception as e:
                logger.warning(f"Could not compile model: {e}")
        
        # 4. Wrap with DDP for distributed training
        if distributed and torch.cuda.device_count() > 1:
            model = DDP(model, find_unused_parameters=False)
            logger.info(f"Wrapped model with DDP (world size: {dist.get_world_size()})")
        
        return model
    
    def _apply_gradient_checkpointing(self, model: nn.Module) -> nn.Module:
        """Apply gradient checkpointing to reduce memory usage"""
        # Check if model has checkpoint method
        if hasattr(model, 'gradient_checkpointing_enable'):
            model.gradient_checkpointing_enable()
            logger.info("Enabled gradient checkpointing")
        else:
            # Manual checkpointing for custom models
            logger.warning("Model doesn't support automatic checkpointing")
        
        return model
    
    def create_optimized_dataloader(
        self,
        dataset: torch.utils.data.Dataset,
        batch_size: int,
        shuffle: bool = True
    ) -> torch.utils.data.DataLoader:
        """
        Create optimized DataLoader for maximum throughput.
        
        Args:
            dataset: PyTorch dataset
            batch_size: Batch size
            shuffle: Whether to shuffle
        
        Returns:
            Optimized DataLoader
        """
        return torch.utils.data.DataLoader(
            dataset,
            batch_size=batch_size,
            shuffle=shuffle,
            num_workers=self.config.num_workers,
            pin_memory=self.config.pin_memory,
            prefetch_factor=self.config.prefetch_factor,
            persistent_workers=True if self.config.num_workers > 0 else False
        )
    
    def training_step(
        self,
        model: nn.Module,
        batch: Any,
        criterion: nn.Module,
        optimizer: torch.optim.Optimizer
    ) -> torch.Tensor:
        """
        Optimized training step with mixed precision.
        
        Args:
            model: Model
            batch: Input batch
            criterion: Loss function
            optimizer: Optimizer
        
        Returns:
            Loss value
        """
        # Mixed precision training
        if self.config.use_mixed_precision:
            with autocast(dtype=torch.bfloat16 if self.config.precision == "bf16" else torch.float16):
                output = model(batch)
                loss = criterion(output)
            
            # Scaled backward pass
            self.scaler.scale(loss).backward()
            
            # Gradient clipping (optional)
            # self.scaler.unscale_(optimizer)
            # torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            
            # Optimizer step
            self.scaler.step(optimizer)
            self.scaler.update()
        else:
            # Standard training
            output = model(batch)
            loss = criterion(output)
            loss.backward()
            optimizer.step()
        
        optimizer.zero_grad(set_to_none=True)  # More efficient than zero_grad()
        
        return loss.detach()
    
    def get_gpu_metrics(self) -> List[GPUMetrics]:
        """Get real-time GPU metrics"""
        metrics = []
        
        if not torch.cuda.is_available():
            return metrics
        
        try:
            import pynvml
            pynvml.nvmlInit()
            
            for gpu_id in range(torch.cuda.device_count()):
                handle = pynvml.nvmlDeviceGetHandleByIndex(gpu_id)
                
                # Utilization
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                
                # Memory
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                memory_used_gb = mem_info.used / (1024**3)
                memory_total_gb = mem_info.total / (1024**3)
                memory_percent = (mem_info.used / mem_info.total) * 100
                
                # Temperature
                temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
                
                # Power
                power = pynvml.nvmlDeviceGetPowerUsage(handle) / 1000  # Convert to watts
                
                metrics.append(GPUMetrics(
                    gpu_id=gpu_id,
                    utilization_percent=util.gpu,
                    memory_used_gb=memory_used_gb,
                    memory_total_gb=memory_total_gb,
                    memory_percent=memory_percent,
                    temperature_c=temp,
                    power_watts=power
                ))
            
            pynvml.nvmlShutdown()
        
        except Exception as e:
            logger.warning(f"Could not get GPU metrics: {e}")
        
        return metrics
    
    def monitor_utilization(self, target_utilization: float = 80.0, duration_seconds: int = 60):
        """
        Monitor GPU utilization and provide optimization suggestions.
        
        Args:
            target_utilization: Target utilization percentage
            duration_seconds: Monitoring duration
        """
        logger.info(f"Monitoring GPU utilization for {duration_seconds}s (target: {target_utilization}%)...")
        
        start_time = time.time()
        samples = []
        
        while time.time() - start_time < duration_seconds:
            metrics = self.get_gpu_metrics()
            if metrics:
                samples.append(metrics)
            time.sleep(1)
        
        if not samples:
            logger.warning("No GPU metrics collected")
            return
        
        # Analyze metrics
        avg_utilization = {}
        avg_memory = {}
        
        for gpu_id in range(len(samples[0])):
            utils = [sample[gpu_id].utilization_percent for sample in samples]
            mems = [sample[gpu_id].memory_percent for sample in samples]
            
            avg_utilization[gpu_id] = sum(utils) / len(utils)
            avg_memory[gpu_id] = sum(mems) / len(mems)
        
        # Report
        logger.info("=" * 60)
        logger.info("GPU UTILIZATION REPORT")
        logger.info("=" * 60)
        
        for gpu_id in avg_utilization:
            util = avg_utilization[gpu_id]
            mem = avg_memory[gpu_id]
            
            status = "✅ EXCELLENT" if util >= target_utilization else "⚠️ SUBOPTIMAL"
            
            logger.info(f"GPU {gpu_id}: {util:.1f}% utilization, {mem:.1f}% memory - {status}")
        
        overall_util = sum(avg_utilization.values()) / len(avg_utilization)
        logger.info(f"\nOverall Average: {overall_util:.1f}%")
        
        # Provide suggestions if underutilized
        if overall_util < target_utilization:
            logger.info("\nOptimization Suggestions:")
            logger.info("- Increase batch size to maximize GPU saturation")
            logger.info("- Reduce num_workers in DataLoader if CPU bottleneck")
            logger.info("- Enable mixed precision (BF16) if not already enabled")
            logger.info("- Use larger models or increase model complexity")
            logger.info("- Profile with torch.profiler to identify bottlenecks")
        
        logger.info("=" * 60)
    
    @staticmethod
    def setup_distributed(rank: int, world_size: int):
        """Setup distributed training"""
        import os
        os.environ['MASTER_ADDR'] = 'localhost'
        os.environ['MASTER_PORT'] = '12355'
        
        dist.init_process_group("nccl", rank=rank, world_size=world_size)
        torch.cuda.set_device(rank)
    
    @staticmethod
    def cleanup_distributed():
        """Cleanup distributed training"""
        dist.destroy_process_group()


# ==================== UTILITY FUNCTIONS ====================

def profile_model(model: nn.Module, input_sample: torch.Tensor, num_steps: int = 100):
    """
    Profile model performance with torch.profiler.
    
    Args:
        model: Model to profile
        input_sample: Sample input
        num_steps: Number of profiling steps
    """
    logger.info(f"Profiling model for {num_steps} steps...")
    
    with torch.profiler.profile(
        activities=[
            torch.profiler.ProfilerActivity.CPU,
            torch.profiler.ProfilerActivity.CUDA,
        ],
        record_shapes=True,
        profile_memory=True,
        with_stack=True
    ) as prof:
        for _ in range(num_steps):
            model(input_sample)
    
    # Print summary
    logger.info("\nTop 10 operations by CUDA time:")
    logger.info(prof.key_averages().table(sort_by="cuda_time_total", row_limit=10))
    
    # Export trace for visualization
    prof.export_chrome_trace("model_profile.json")
    logger.info("Exported trace to model_profile.json (open in chrome://tracing)")


def estimate_memory_usage(
    batch_size: int,
    sequence_length: int,
    hidden_size: int,
    num_layers: int,
    vocab_size: int
) -> float:
    """
    Estimate GPU memory usage for FFT-based model.
    
    Returns:
        Estimated memory in GB
    """
    # Model parameters
    param_memory = (
        num_layers * (hidden_size ** 2 * 4 + hidden_size * vocab_size) * 4  # FP32 bytes
    ) / (1024 ** 3)
    
    # Activations (forward pass)
    activation_memory = (
        batch_size * sequence_length * hidden_size * num_layers * 4
    ) / (1024 ** 3)
    
    # Gradients (backward pass)
    gradient_memory = param_memory
    
    # Optimizer states (Adam: 2x parameters)
    optimizer_memory = param_memory * 2
    
    total_memory = param_memory + activation_memory + gradient_memory + optimizer_memory
    
    logger.info(f"Estimated memory usage:")
    logger.info(f"  Parameters: {param_memory:.2f} GB")
    logger.info(f"  Activations: {activation_memory:.2f} GB")
    logger.info(f"  Gradients: {gradient_memory:.2f} GB")
    logger.info(f"  Optimizer: {optimizer_memory:.2f} GB")
    logger.info(f"  Total: {total_memory:.2f} GB")
    
    return total_memory
