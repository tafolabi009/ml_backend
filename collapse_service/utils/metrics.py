"""
Prometheus Metrics Exporter
============================

Provides real-time metrics for monitoring validation pipeline performance.
"""

import time
import psutil
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    CollectorRegistry,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

logger = logging.getLogger(__name__)

# Create registry
registry = CollectorRegistry()

# Define metrics

# Counters (cumulative)
validation_requests_total = Counter(
    'synthos_validation_requests_total',
    'Total number of validation requests',
    ['status', 'dataset_format'],
    registry=registry
)

validation_errors_total = Counter(
    'synthos_validation_errors_total',
    'Total number of validation errors',
    ['error_type', 'stage'],
    registry=registry
)

# Gauges (current value)
active_validations = Gauge(
    'synthos_active_validations',
    'Number of currently active validations',
    registry=registry
)

gpu_utilization_percent = Gauge(
    'synthos_gpu_utilization_percent',
    'GPU utilization percentage',
    ['gpu_id'],
    registry=registry
)

gpu_memory_used_bytes = Gauge(
    'synthos_gpu_memory_used_bytes',
    'GPU memory used in bytes',
    ['gpu_id'],
    registry=registry
)

cpu_percent = Gauge(
    'synthos_cpu_percent',
    'CPU utilization percentage',
    registry=registry
)

memory_used_bytes = Gauge(
    'synthos_memory_used_bytes',
    'System memory used in bytes',
    registry=registry
)

# Histograms (distribution)
validation_duration_seconds = Histogram(
    'synthos_validation_duration_seconds',
    'Duration of validation in seconds',
    ['stage'],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
    registry=registry
)

dataset_rows_processed = Histogram(
    'synthos_dataset_rows_processed',
    'Number of rows processed',
    buckets=(100, 1000, 10000, 100000, 1000000, 10000000),
    registry=registry
)

# Summaries (quantiles)
collapse_score = Summary(
    'synthos_collapse_score',
    'Collapse detection score',
    registry=registry
)

diversity_score = Summary(
    'synthos_diversity_score',
    'Diversity analysis score',
    registry=registry
)


@dataclass
class MetricsCollector:
    """
    Collects and exports metrics for monitoring.
    """

    registry: CollectorRegistry = field(default_factory=lambda: registry)
    start_time: Optional[float] = None
    stage_start_times: Dict[str, float] = field(default_factory=dict)

    def start_validation(self):
        """Called when validation starts"""
        self.start_time = time.time()
        active_validations.inc()
        logger.debug("Started validation metrics collection")

    def end_validation(self, status: str, dataset_format: str):
        """Called when validation ends"""
        active_validations.dec()
        validation_requests_total.labels(status=status, dataset_format=dataset_format).inc()
        
        if self.start_time:
            duration = time.time() - self.start_time
            validation_duration_seconds.labels(stage='total').observe(duration)
        
        logger.debug(f"Ended validation metrics collection: {status}")

    def record_error(self, error_type: str, stage: str):
        """Record an error occurrence"""
        validation_errors_total.labels(error_type=error_type, stage=stage).inc()
        logger.warning(f"Recorded error: {error_type} in stage {stage}")

    def start_stage(self, stage_name: str):
        """Called when a pipeline stage starts"""
        self.stage_start_times[stage_name] = time.time()
        logger.debug(f"Started stage: {stage_name}")

    def end_stage(self, stage_name: str):
        """Called when a pipeline stage ends"""
        if stage_name in self.stage_start_times:
            duration = time.time() - self.stage_start_times[stage_name]
            validation_duration_seconds.labels(stage=stage_name).observe(duration)
            logger.debug(f"Ended stage: {stage_name} ({duration:.2f}s)")

    def record_dataset_size(self, num_rows: int):
        """Record dataset size"""
        dataset_rows_processed.observe(num_rows)

    def record_collapse_score(self, score: float):
        """Record collapse detection score"""
        collapse_score.observe(score)

    def record_diversity_score(self, score: float):
        """Record diversity analysis score"""
        diversity_score.observe(score)

    def update_gpu_metrics(self):
        """Update GPU metrics (if available)"""
        try:
            import pynvml
            pynvml.nvmlInit()
            
            device_count = pynvml.nvmlDeviceGetCount()
            for i in range(device_count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                
                # Utilization
                util = pynvml.nvmlDeviceGetUtilizationRates(handle)
                gpu_utilization_percent.labels(gpu_id=str(i)).set(util.gpu)
                
                # Memory
                mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                gpu_memory_used_bytes.labels(gpu_id=str(i)).set(mem_info.used)
            
            pynvml.nvmlShutdown()
        except Exception as e:
            logger.debug(f"Could not collect GPU metrics: {e}")

    def update_system_metrics(self):
        """Update system metrics (CPU, memory)"""
        try:
            # CPU usage
            cpu_percent.set(psutil.cpu_percent(interval=0.1))
            
            # Memory usage
            mem = psutil.virtual_memory()
            memory_used_bytes.set(mem.used)
        except Exception as e:
            logger.warning(f"Could not collect system metrics: {e}")

    def get_metrics_text(self) -> bytes:
        """Get metrics in Prometheus text format"""
        return generate_latest(self.registry)

    def get_metrics_content_type(self) -> str:
        """Get content type for Prometheus metrics"""
        return CONTENT_TYPE_LATEST


# Global metrics collector instance
metrics_collector = MetricsCollector()


def get_current_metrics() -> Dict[str, Any]:
    """
    Get current metrics as dictionary (for debugging/display).
    """
    try:
        import pynvml
        pynvml.nvmlInit()
        gpu_count = pynvml.nvmlDeviceGetCount()
        
        gpu_metrics = []
        for i in range(gpu_count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            util = pynvml.nvmlDeviceGetUtilizationRates(handle)
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(handle)
            
            gpu_metrics.append({
                'gpu_id': i,
                'utilization_percent': util.gpu,
                'memory_used_mb': mem_info.used / (1024 * 1024),
                'memory_total_mb': mem_info.total / (1024 * 1024),
            })
        
        pynvml.nvmlShutdown()
    except Exception:
        gpu_metrics = []

    mem = psutil.virtual_memory()
    
    return {
        'timestamp': datetime.now().isoformat(),
        'system': {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_used_mb': mem.used / (1024 * 1024),
            'memory_total_mb': mem.total / (1024 * 1024),
            'memory_percent': mem.percent,
        },
        'gpus': gpu_metrics,
    }


# Usage example:
# from src.utils.metrics import metrics_collector
#
# metrics_collector.start_validation()
# metrics_collector.start_stage('data_loading')
# # ... do work ...
# metrics_collector.end_stage('data_loading')
# metrics_collector.record_collapse_score(85.5)
# metrics_collector.end_validation('completed', 'parquet')
