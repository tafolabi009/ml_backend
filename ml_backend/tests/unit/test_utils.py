import pytest
import numpy as np
import torch
import torch.nn as nn
from unittest.mock import MagicMock, patch, call
import time

from src.utils.gpu_optimizer import GPUOptimizer, OptimizationConfig, GPUMetrics
from src.utils.error_handling import (
    CircuitBreaker, with_retries, RetryableError,
    ResourceExhaustedError, TransientError, ValidationError,
    ErrorCategory, classify_error, safe_divide
)
from src.utils.metrics import MetricsCollector


class TestGPUOptimizer:
    
    @pytest.fixture
    def optimizer(self):
        """Create GPU optimizer instance"""
        return GPUOptimizer()
    
    def test_optimization_config(self):
        """Test OptimizationConfig initialization"""
        config = OptimizationConfig(
            batch_size=32,
            num_workers=4,
            pin_memory=True,
            mixed_precision=True
        )
        assert config.batch_size == 32
        assert config.num_workers == 4
        assert config.pin_memory is True
        assert config.mixed_precision is True
    
    @patch('torch.cuda.is_available', return_value=False)
    def test_no_cuda_available(self, mock_cuda, optimizer):
        """Test when CUDA is not available"""
        assert not optimizer.is_cuda_available()
    
    @patch('torch.cuda.is_available', return_value=True)
    @patch('torch.cuda.device_count', return_value=2)
    def test_cuda_available(self, mock_count, mock_available, optimizer):
        """Test when CUDA is available"""
        assert optimizer.is_cuda_available()
        assert optimizer.get_device_count() == 2
    
    @patch('torch.cuda.is_available', return_value=True)
    def test_get_optimal_batch_size(self, mock_cuda, optimizer):
        """Test getting optimal batch size"""
        # Should return a reasonable batch size
        batch_size = optimizer.get_optimal_batch_size(model_size_mb=100)
        assert isinstance(batch_size, int)
        assert batch_size > 0
    
    @patch('torch.cuda.is_available', return_value=True)
    def test_optimize_dataloader_config(self, mock_cuda, optimizer):
        """Test optimizing dataloader configuration"""
        config = optimizer.optimize_dataloader_config()
        
        assert isinstance(config, OptimizationConfig)
        assert config.batch_size > 0
        assert config.num_workers >= 0


class TestCircuitBreaker:
    
    def test_circuit_breaker_success(self):
        """Test circuit breaker with successful calls"""
        breaker = CircuitBreaker(failure_threshold=3, timeout=1.0)
        
        def successful_func():
            return "success"
        
        # Should succeed
        for _ in range(5):
            result = breaker.call(successful_func)
            assert result == "success"
    
    def test_circuit_breaker_opens(self):
        """Test circuit breaker opens after threshold"""
        breaker = CircuitBreaker(failure_threshold=3, timeout=0.5)
        
        def failing_func():
            raise ValueError("Error")
        
        # First 3 calls should fail normally
        for _ in range(3):
            with pytest.raises(ValueError):
                breaker.call(failing_func)
        
        # Circuit should now be open
        with pytest.raises(Exception):  # Circuit open error
            breaker.call(failing_func)
    
    def test_circuit_breaker_half_open(self):
        """Test circuit breaker half-open state"""
        breaker = CircuitBreaker(failure_threshold=2, timeout=0.1)
        
        call_count = [0]
        
        def sometimes_failing():
            call_count[0] += 1
            if call_count[0] <= 2:
                raise ValueError("Error")
            return "success"
        
        # Fail twice to open circuit
        for _ in range(2):
            with pytest.raises(ValueError):
                breaker.call(sometimes_failing)
        
        # Wait for timeout
        time.sleep(0.2)
        
        # Next call should work (half-open state)
        result = breaker.call(sometimes_failing)
        assert result == "success"


class TestRetries:
    
    def test_with_retries_success(self):
        """Test retry decorator with successful function"""
        @with_retries(max_attempts=3, delay=0.01)
        def successful_func():
            return "success"
        
        result = successful_func()
        assert result == "success"
    
    def test_with_retries_eventually_succeeds(self):
        """Test retry with function that eventually succeeds"""
        call_count = [0]
        
        @with_retries(max_attempts=3, delay=0.01)
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 2:
                raise RetryableError("Temporary error")
            return "success"
        
        result = flaky_func()
        assert result == "success"
        assert call_count[0] == 2
    
    def test_with_retries_exhausted(self):
        """Test retry exhausts all attempts"""
        call_count = [0]
        
        @with_retries(max_attempts=3, delay=0.01)
        def always_fails():
            call_count[0] += 1
            raise RetryableError("Always fails")
        
        with pytest.raises(RetryableError):
            always_fails()
        
        assert call_count[0] == 3
    
    def test_with_retries_non_retryable(self):
        """Test retry with non-retryable error"""
        call_count = [0]
        
        @with_retries(max_attempts=3, delay=0.01)
        def raises_validation_error():
            call_count[0] += 1
            raise ValueError("Not retryable")
        
        with pytest.raises(ValueError):
            raises_validation_error()
        
        # Should only be called once for non-retryable errors
        assert call_count[0] == 1


class TestErrorClassification:
    
    def test_classify_resource_exhausted(self):
        """Test classifying resource exhausted errors"""
        error = ResourceExhaustedError("Out of memory")
        category = classify_error(error)
        assert category == ErrorCategory.RESOURCE
    
    def test_classify_transient(self):
        """Test classifying transient errors"""
        error = TransientError("Network timeout")
        category = classify_error(error)
        assert category == ErrorCategory.TRANSIENT
    
    def test_classify_validation(self):
        """Test classifying validation errors"""
        error = ValidationError("Invalid input")
        category = classify_error(error)
        assert category == ErrorCategory.VALIDATION
    
    def test_classify_unknown(self):
        """Test classifying unknown errors"""
        error = Exception("Unknown error")
        category = classify_error(error)
        assert category == ErrorCategory.UNKNOWN


class TestSafeDivide:
    
    def test_safe_divide_normal(self):
        """Test safe division with normal values"""
        result = safe_divide(10, 2)
        assert result == 5.0
    
    def test_safe_divide_by_zero(self):
        """Test safe division by zero returns default"""
        result = safe_divide(10, 0)
        assert result == 0.0
    
    def test_safe_divide_custom_default(self):
        """Test safe division with custom default"""
        result = safe_divide(10, 0, default=float('inf'))
        assert result == float('inf')
    
    def test_safe_divide_float_inputs(self):
        """Test safe division with float inputs"""
        result = safe_divide(7.5, 2.5)
        assert abs(result - 3.0) < 1e-10


class TestMetricsCollector:
    
    @pytest.fixture
    def collector(self):
        """Create a metrics collector"""
        return MetricsCollector()
    
    def test_record_metric(self, collector):
        """Test recording a simple metric"""
        collector.record('test_metric', 100)
        
        metrics = collector.get_metrics()
        assert 'test_metric' in metrics
    
    def test_record_multiple_metrics(self, collector):
        """Test recording multiple metrics"""
        collector.record('metric1', 10)
        collector.record('metric2', 20)
        collector.record('metric3', 30)
        
        metrics = collector.get_metrics()
        assert 'metric1' in metrics
        assert 'metric2' in metrics
        assert 'metric3' in metrics
    
    def test_increment_counter(self, collector):
        """Test incrementing a counter"""
        collector.increment('counter')
        collector.increment('counter')
        collector.increment('counter')
        
        metrics = collector.get_metrics()
        assert metrics.get('counter', 0) == 3
    
    def test_record_timing(self, collector):
        """Test recording timing information"""
        collector.start_timer('operation')
        time.sleep(0.01)
        collector.stop_timer('operation')
        
        metrics = collector.get_metrics()
        assert 'operation' in metrics or 'operation_time' in metrics
    
    def test_get_summary(self, collector):
        """Test getting metrics summary"""
        collector.record('value1', 10)
        collector.record('value2', 20)
        
        summary = collector.get_summary()
        assert isinstance(summary, dict)
    
    def test_reset_metrics(self, collector):
        """Test resetting metrics"""
        collector.record('test', 100)
        collector.reset()
        
        metrics = collector.get_metrics()
        assert len(metrics) == 0
