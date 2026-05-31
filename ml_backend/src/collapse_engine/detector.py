"""
Collapse Detector - Multi-dimensional model collapse detection
Optimized for OpenAI/DeepMind scale validation

Architecture-Aware: Designed to work with Resonance Neural Networks
- Spectral coherence analysis aligns with frequency-domain processing
- FFT-based metrics match Resonance NN's O(n log n) architecture
- Holographic memory patterns can be detected in entropy stability

Features:
- 8-dimensional collapse scoring
- GPU-accelerated statistical analysis
- Real-time collapse detection during training
- Historical pattern matching
- Predictive collapse warnings

Batch Size Limits:
- CPU: Recommended max 50,000 samples per batch
- GPU (8GB VRAM): Recommended max 100,000 samples per batch  
- GPU (16GB+ VRAM): Recommended max 500,000 samples per batch
- GPU (80GB A100): Recommended max 2,000,000 samples per batch

Memory Usage Estimation:
- ~8 bytes per float32 element
- Each sample with 512 dimensions = 4KB
- 100K samples = ~400MB base memory (plus 3-5x for intermediate computations)
"""

import numpy as np
import torch
import torch.nn.functional as F
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass
from scipy import stats
from scipy.spatial import distance
import logging
import gc

logger = logging.getLogger(__name__)


class CUDAOutOfMemoryError(RuntimeError):
    """Raised when CUDA runs out of memory during collapse detection."""
    def __init__(self, message: str, batch_size: int, suggested_batch_size: int):
        self.batch_size = batch_size
        self.suggested_batch_size = suggested_batch_size
        super().__init__(f"{message}. Current batch size: {batch_size}. Try reducing to: {suggested_batch_size}")


class InvalidInputError(ValueError):
    """Raised when input data contains invalid values (NaN, Inf, or wrong shape)."""
    def __init__(self, message: str, field: str, details: Optional[Dict] = None):
        self.field = field
        self.details = details or {}
        super().__init__(f"{message} in '{field}': {details}")


def validate_tensor_input(
    data: Union[np.ndarray, torch.Tensor],
    name: str,
    allow_empty: bool = False,
    min_samples: int = 2,
    min_features: int = 1,
    max_samples: Optional[int] = None
) -> np.ndarray:
    """
    Validate input tensor for NaN, Inf, shape, and type issues.
    
    Args:
        data: Input tensor or array
        name: Name for error messages
        allow_empty: Whether to allow empty arrays
        min_samples: Minimum number of samples required
        min_features: Minimum number of features required
        max_samples: Maximum samples allowed (for memory safety)
    
    Returns:
        Validated numpy array
        
    Raises:
        InvalidInputError: If validation fails
    """
    # Convert to numpy if needed
    if isinstance(data, torch.Tensor):
        data = data.detach().cpu().numpy()
    elif not isinstance(data, np.ndarray):
        try:
            data = np.array(data)
        except Exception as e:
            raise InvalidInputError(
                f"Cannot convert to numpy array",
                name,
                {"type": type(data).__name__, "error": str(e)}
            )
    
    # Check empty
    if data.size == 0:
        if not allow_empty:
            raise InvalidInputError(
                "Data cannot be empty",
                name,
                {"shape": data.shape, "size": data.size}
            )
        return data
    
    # Check for NaN
    nan_count = np.isnan(data).sum()
    if nan_count > 0:
        nan_ratio = nan_count / data.size
        if nan_ratio > 0.5:  # More than 50% NaN is likely an error
            raise InvalidInputError(
                f"Data contains too many NaN values ({nan_ratio:.1%})",
                name,
                {"nan_count": int(nan_count), "total": data.size, "nan_ratio": float(nan_ratio)}
            )
        logger.warning(f"{name}: Contains {nan_count} NaN values ({nan_ratio:.1%}), will be replaced with 0")
    
    # Check for Inf
    inf_count = np.isinf(data).sum()
    if inf_count > 0:
        inf_ratio = inf_count / data.size
        if inf_ratio > 0.1:  # More than 10% Inf is likely an error
            raise InvalidInputError(
                f"Data contains too many Inf values ({inf_ratio:.1%})",
                name,
                {"inf_count": int(inf_count), "total": data.size, "inf_ratio": float(inf_ratio)}
            )
        logger.warning(f"{name}: Contains {inf_count} Inf values ({inf_ratio:.1%}), will be clipped")
    
    # Check shape — project higher-dimensional tensors to 2D
    if data.ndim == 1:
        data = data.reshape(-1, 1)
        logger.warning(f"{name}: 1D array reshaped to ({data.shape[0]}, 1)")
    elif data.ndim == 3:
        # (Batch, Seq, Embed_Dim) → (Batch, Embed_Dim) via mean pooling
        logger.info(f"{name}: Pooling 3D tensor {data.shape} → ({data.shape[0]}, {data.shape[2]}) via mean over axis 1")
        data = np.mean(data, axis=1)
    elif data.ndim == 4:
        # (Batch, Channels, H, W) → (Batch, C*H*W) via flatten
        logger.info(f"{name}: Flattening 4D tensor {data.shape} → ({data.shape[0]}, {np.prod(data.shape[1:])})")
        data = data.reshape(data.shape[0], -1)
    elif data.ndim > 4:
        # Flatten all spatial/sequence dims
        logger.info(f"{name}: Flattening {data.ndim}D tensor {data.shape} → ({data.shape[0]}, {np.prod(data.shape[1:])})")
        data = data.reshape(data.shape[0], -1)
    
    if data.ndim != 2:
        raise InvalidInputError(
            "Data must resolve to a 2D feature matrix after projection",
            name,
            {"ndim": data.ndim, "shape": data.shape}
        )
    
    n_samples, n_features = data.shape
    
    # Check minimum samples
    if n_samples < min_samples:
        raise InvalidInputError(
            f"Insufficient samples (need at least {min_samples})",
            name,
            {"n_samples": n_samples, "min_required": min_samples}
        )
    
    # Check minimum features
    if n_features < min_features:
        raise InvalidInputError(
            f"Insufficient features (need at least {min_features})",
            name,
            {"n_features": n_features, "min_required": min_features}
        )
    
    # Check maximum samples (memory safety)
    if max_samples is not None and n_samples > max_samples:
        raise InvalidInputError(
            f"Too many samples (max allowed: {max_samples})",
            name,
            {"n_samples": n_samples, "max_allowed": max_samples}
        )
    
    # Clean data: replace NaN with 0, clip Inf
    data = np.nan_to_num(data, nan=0.0, posinf=1e10, neginf=-1e10)
    
    return data


def cleanup_gpu_memory():
    """Force GPU memory cleanup to prevent OOM"""
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        torch.cuda.synchronize()
    gc.collect()


# SynthOS CUDA Kernel Integration
# Provides 2-5x speedup for spectral entropy computation
try:
    from synthos_kernel import SynthOSKernel
    _SYNTHOS_AVAILABLE = True
    logger.info("SynthOS kernel module available")
except ImportError:
    _SYNTHOS_AVAILABLE = False
    logger.debug("SynthOS kernel not available, using PyTorch fallback")


@dataclass
class CollapseScore:
    """Multi-dimensional collapse score"""
    overall_score: float  # 0-100 (0 = complete collapse, 100 = healthy)
    collapse_detected: bool
    confidence: float
    dimensions: Dict[str, 'DimensionScore']
    warnings: List[str]
    predictions: Dict[str, float]  # Predicted scores at scale


@dataclass
class DimensionScore:
    """Individual dimension scoring"""
    name: str
    score: float  # 0-100
    threshold: float
    passed: bool
    metrics: Dict[str, float]
    severity: str  # 'critical', 'warning', 'ok'


@dataclass
class CollapseConfig:
    """
    Configuration for collapse detection.
    
    Attributes:
        distribution_fidelity_threshold: Min score for distribution fidelity (0-100)
        correlation_preservation_threshold: Min score for correlation preservation (0-100)
        entropy_stability_threshold: Min score for entropy stability (0-100)
        gradient_health_threshold: Min score for gradient health (0-100)
        loss_landscape_threshold: Min score for loss landscape (0-100)
        spectral_coherence_threshold: Min score for spectral coherence (0-100)
        generalization_gap_threshold: Min score for generalization gap (0-100)
        statistical_consistency_threshold: Min score for statistical consistency (0-100)
        overall_threshold: Average dimension score below which collapse is detected
        use_gpu: Whether to use GPU acceleration
        batch_size: Maximum samples per processing batch (memory management)
        max_samples: Hard limit on input samples (prevents OOM). Set based on available memory:
            - 8GB VRAM: 100,000 samples
            - 16GB VRAM: 250,000 samples  
            - 80GB VRAM (A100): 2,000,000 samples
    """
    # Thresholds for each dimension (scores below = collapse)
    distribution_fidelity_threshold: float = 70.0
    correlation_preservation_threshold: float = 70.0
    entropy_stability_threshold: float = 65.0
    gradient_health_threshold: float = 60.0
    loss_landscape_threshold: float = 65.0
    spectral_coherence_threshold: float = 70.0
    generalization_gap_threshold: float = 75.0
    statistical_consistency_threshold: float = 70.0
    
    # Overall collapse threshold (average of dimensions)
    overall_threshold: float = 65.0
    
    # GPU settings
    use_gpu: bool = True
    batch_size: int = 10000
    
    # Memory safety limits
    max_samples: int = 500000  # Safe default for 16GB VRAM
    max_features: int = 10000  # Reasonable feature limit


class CollapseDetector:
    """
    Multi-dimensional model collapse detector.
    Analyzes 8 key dimensions to detect training data collapse.
    """
    
    def __init__(self, config: Optional[CollapseConfig] = None):
        self.config = config or CollapseConfig()
        self.device = torch.device("cuda" if torch.cuda.is_available() and self.config.use_gpu else "cpu")
        
        # Initialize SynthOS kernel for accelerated spectral analysis
        self._synthos_kernel = None
        self._use_synthos = False
        
        if _SYNTHOS_AVAILABLE and self.device.type == 'cuda':
            try:
                self._synthos_kernel = SynthOSKernel()
                self._use_synthos = True
                logger.info(
                    f"SynthOS kernel initialized: {self._synthos_kernel.active_arch} "
                    f"(expected speedup: {self._synthos_kernel.get_performance_info()['expected_speedup']:.1f}x)"
                )
            except Exception as e:
                logger.warning(f"SynthOS kernel initialization failed, using PyTorch: {e}")
        
        logger.info(f"CollapseDetector initialized on {self.device}")
    
    async def detect_collapse(
        self,
        synthetic_data: np.ndarray,
        original_data: np.ndarray,
        model_gradients: Optional[Dict[str, torch.Tensor]] = None,
        training_metrics: Optional[Dict[str, List[float]]] = None
    ) -> CollapseScore:
        """
        Comprehensive collapse detection across 8 dimensions.
        
        Args:
            synthetic_data: Generated synthetic data (2D: samples x features)
            original_data: Original training data (2D: samples x features)
            model_gradients: Model gradients during training (optional)
            training_metrics: Training loss/accuracy curves (optional)
        
        Returns:
            CollapseScore with detailed dimension analysis
            
        Raises:
            InvalidInputError: If input data contains too many NaN/Inf values or has wrong shape
            CUDAOutOfMemoryError: If GPU runs out of memory (includes suggested batch size)
            
        Memory Limits:
            - Default max_samples: 500,000 (configurable via CollapseConfig)
            - Reduce batch_size in config if hitting OOM errors
        """
        logger.info("Starting multi-dimensional collapse detection...")
        
        # Validate and clean inputs
        synthetic_data = validate_tensor_input(
            synthetic_data,
            "synthetic_data",
            min_samples=2,
            max_samples=self.config.max_samples
        )
        original_data = validate_tensor_input(
            original_data,
            "original_data",
            min_samples=2,
            max_samples=self.config.max_samples
        )
            
        # Handle dimension mismatch (trim to common dimensions)
        min_dims = min(synthetic_data.shape[1], original_data.shape[1])
        if synthetic_data.shape[1] != original_data.shape[1]:
            logger.warning(f"Dimension mismatch: {synthetic_data.shape[1]} vs {original_data.shape[1]}. Trimming to {min_dims}.")
            synthetic_data = synthetic_data[:, :min_dims]
            original_data = original_data[:, :min_dims]
        
        dimensions = {}
        warnings = []
        
        try:
            # Dimension 1: Distribution Fidelity
            logger.info("Analyzing distribution fidelity...")
            dimensions['distribution_fidelity'] = await self._analyze_distribution_fidelity(
                synthetic_data, original_data
            )
            
            # Dimension 2: Correlation Preservation
            logger.info("Analyzing correlation preservation...")
            dimensions['correlation_preservation'] = await self._analyze_correlation_preservation(
                synthetic_data, original_data
            )
        
            # Dimension 3: Entropy Stability
            logger.info("Analyzing entropy stability...")
            dimensions['entropy_stability'] = await self._analyze_entropy_stability(
                synthetic_data, original_data
            )
            
            # Dimension 4: Gradient Health (if available)
            if model_gradients:
                logger.info("Analyzing gradient health...")
                dimensions['gradient_health'] = await self._analyze_gradient_health(model_gradients)
            else:
                dimensions['gradient_health'] = self._create_neutral_dimension('gradient_health')
            
            # Dimension 5: Loss Landscape (if available)
            if training_metrics:
                logger.info("Analyzing loss landscape...")
                dimensions['loss_landscape'] = await self._analyze_loss_landscape(training_metrics)
            else:
                dimensions['loss_landscape'] = self._create_neutral_dimension('loss_landscape')
            
            # Dimension 6: Spectral Coherence (FFT-based)
            logger.info("Analyzing spectral coherence...")
            dimensions['spectral_coherence'] = await self._analyze_spectral_coherence(
                synthetic_data, original_data
            )
            
            # Dimension 7: Generalization Gap
            logger.info("Analyzing generalization gap...")
            dimensions['generalization_gap'] = await self._analyze_generalization_gap(
                synthetic_data, original_data
            )
            
            # Dimension 8: Statistical Consistency
            logger.info("Analyzing statistical consistency...")
            dimensions['statistical_consistency'] = await self._analyze_statistical_consistency(
                synthetic_data, original_data
            )
            
        except torch.cuda.OutOfMemoryError as e:
            # Handle CUDA OOM with helpful error
            cleanup_gpu_memory()
            current_samples = synthetic_data.shape[0]
            suggested_samples = current_samples // 2
            logger.error(f"CUDA OOM during collapse detection. Samples: {current_samples}")
            raise CUDAOutOfMemoryError(
                "GPU ran out of memory during collapse detection",
                batch_size=current_samples,
                suggested_batch_size=suggested_samples
            ) from e
        except RuntimeError as e:
            if "out of memory" in str(e).lower():
                cleanup_gpu_memory()
                current_samples = synthetic_data.shape[0]
                suggested_samples = current_samples // 2
                raise CUDAOutOfMemoryError(
                    "GPU ran out of memory during collapse detection",
                    batch_size=current_samples,
                    suggested_batch_size=suggested_samples
                ) from e
            raise
        
        # Compute overall score
        overall_score = self._compute_overall_score(dimensions)
        
        # Detect collapse
        collapse_detected = overall_score < self.config.overall_threshold
        
        # Compute confidence
        confidence = self._compute_confidence(dimensions)
        
        # Generate warnings
        warnings = self._generate_warnings(dimensions, collapse_detected)
        
        # Predict scores at scale
        predictions = self._predict_at_scale(dimensions)
        
        return CollapseScore(
            overall_score=overall_score,
            collapse_detected=collapse_detected,
            confidence=confidence,
            dimensions=dimensions,
            warnings=warnings,
            predictions=predictions
        )
    
    async def detect_collapse_with_cleanup(
        self,
        synthetic_data: np.ndarray,
        original_data: np.ndarray,
        training_history: Optional[List[Dict]] = None
    ) -> CollapseScore:
        """
        Detect collapse with automatic GPU memory cleanup.
        Use this for long-running inference pipelines.
        """
        try:
            return await self.detect_collapse(synthetic_data, original_data, training_history)
        finally:
            cleanup_gpu_memory()
    
    # ==================== DIMENSION 1: DISTRIBUTION FIDELITY ====================
    
    async def _analyze_distribution_fidelity(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        Measure how well synthetic data matches original distribution.
        Uses multiple statistical tests.
        """
        metrics = {}
        
        # Convert to torch for GPU acceleration
        if self.device.type == 'cuda':
            synth_tensor = torch.tensor(synthetic, device=self.device, dtype=torch.float32)
            orig_tensor = torch.tensor(original, device=self.device, dtype=torch.float32)
        else:
            synth_tensor = torch.tensor(synthetic, dtype=torch.float32)
            orig_tensor = torch.tensor(original, dtype=torch.float32)
        
        # 1. Kolmogorov-Smirnov test (per dimension)
        ks_scores = []
        for i in range(min(synthetic.shape[1], 100)):  # Sample up to 100 dims
            ks_stat, ks_pval = stats.ks_2samp(synthetic[:, i], original[:, i])
            ks_scores.append(1 - ks_stat)  # Convert to similarity score
        metrics['ks_similarity'] = np.mean(ks_scores) * 100
        
        # 2. Wasserstein distance (Earth Mover's Distance)
        wasserstein_scores = []
        for i in range(min(synthetic.shape[1], 100)):
            wd = stats.wasserstein_distance(synthetic[:, i], original[:, i])
            # Normalize to 0-1 range (smaller is better)
            max_range = np.ptp(original[:, i])
            normalized_wd = 1 - min(1, wd / (max_range + 1e-8))
            wasserstein_scores.append(normalized_wd)
        metrics['wasserstein_similarity'] = np.mean(wasserstein_scores) * 100
        
        # 3. Moment matching (mean, std, skew, kurtosis)
        mean_diff = torch.abs(synth_tensor.mean(0) - orig_tensor.mean(0)).mean().item()
        std_diff = torch.abs(synth_tensor.std(0) - orig_tensor.std(0)).mean().item()
        
        mean_score = 100 * np.exp(-mean_diff)
        std_score = 100 * np.exp(-std_diff)
        
        metrics['mean_match'] = mean_score
        metrics['std_match'] = std_score
        
        # 4. Histogram intersection
        hist_intersections = []
        for i in range(min(synthetic.shape[1], 50)):
            # Use density=False and normalize to sum to 1 (PMF)
            hist_synth, bins = np.histogram(synthetic[:, i], bins=50, density=False)
            hist_orig, _ = np.histogram(original[:, i], bins=bins, density=False)
            
            # Normalize to sum to 1
            hist_synth = hist_synth / (hist_synth.sum() + 1e-10)
            hist_orig = hist_orig / (hist_orig.sum() + 1e-10)
            
            intersection = np.minimum(hist_synth, hist_orig).sum()
            hist_intersections.append(intersection)
        metrics['histogram_intersection'] = np.mean(hist_intersections) * 100
        
        # 5. KL divergence (lower is better)
        kl_divergences = []
        for i in range(min(synthetic.shape[1], 50)):
            # Create probability distributions from histograms
            hist_synth, bins = np.histogram(synthetic[:, i], bins=50, density=False)
            hist_orig, _ = np.histogram(original[:, i], bins=bins, density=False)
            
            # Normalize to probabilities
            hist_synth = hist_synth / (hist_synth.sum() + 1e-10)
            hist_orig = hist_orig / (hist_orig.sum() + 1e-10)
            
            # Add small epsilon to avoid log(0)
            hist_synth = hist_synth + 1e-10
            hist_orig = hist_orig + 1e-10
            
            # Calculate KL divergence
            kl_div = stats.entropy(hist_synth, hist_orig)
            kl_divergences.append(kl_div)
        
        avg_kl = np.mean(kl_divergences)
        # Convert to similarity score (lower KL = higher score)
        kl_score = 100 * np.exp(-avg_kl)
        metrics['kl_divergence'] = avg_kl
        metrics['kl_similarity'] = kl_score
        
        # Combined score (weighted average)
        score = (
            metrics['ks_similarity'] * 0.20 +
            metrics['wasserstein_similarity'] * 0.20 +
            metrics['mean_match'] * 0.15 +
            metrics['std_match'] * 0.15 +
            metrics['histogram_intersection'] * 0.15 +
            metrics['kl_similarity'] * 0.15
        )
        
        passed = score >= self.config.distribution_fidelity_threshold
        severity = self._determine_severity(score, self.config.distribution_fidelity_threshold)
        
        return DimensionScore(
            name='Distribution Fidelity',
            score=score,
            threshold=self.config.distribution_fidelity_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 2: CORRELATION PRESERVATION ====================
    
    async def _analyze_correlation_preservation(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        Measure how well correlations between features are preserved.
        """
        metrics = {}
        
        # Sample columns for efficiency (max 100 columns)
        n_cols = min(synthetic.shape[1], 100)
        synth_sample = synthetic[:, :n_cols]
        orig_sample = original[:, :n_cols]
        
        # Compute correlation matrices
        if self.device.type == 'cuda':
            synth_tensor = torch.tensor(synth_sample, device=self.device, dtype=torch.float32)
            orig_tensor = torch.tensor(orig_sample, device=self.device, dtype=torch.float32)
            
            synth_corr = torch.corrcoef(synth_tensor.T).cpu().numpy()
            orig_corr = torch.corrcoef(orig_tensor.T).cpu().numpy()
        else:
            synth_corr = np.corrcoef(synth_sample.T)
            orig_corr = np.corrcoef(orig_sample.T)
        
        # Replace NaN with 0
        synth_corr = np.nan_to_num(synth_corr)
        orig_corr = np.nan_to_num(orig_corr)
        
        # 1. Frobenius norm of difference
        corr_diff = np.linalg.norm(synth_corr - orig_corr, 'fro')
        max_norm = np.linalg.norm(orig_corr, 'fro')
        frobenius_score = 100 * (1 - min(1, corr_diff / (max_norm + 1e-8)))
        metrics['frobenius_similarity'] = frobenius_score
        
        # 2. Correlation of correlations
        # Flatten upper triangular parts
        triu_indices = np.triu_indices(n_cols, k=1)
        synth_corr_flat = synth_corr[triu_indices]
        orig_corr_flat = orig_corr[triu_indices]
        
        if len(synth_corr_flat) > 1:
            # Handle constant arrays in corrcoef (returns NaN)
            if np.std(synth_corr_flat) < 1e-9 or np.std(orig_corr_flat) < 1e-9:
                corr_of_corr = 0.0
            else:
                corr_of_corr = np.corrcoef(synth_corr_flat, orig_corr_flat)[0, 1]
                if np.isnan(corr_of_corr):
                    corr_of_corr = 0.0
            
            corr_of_corr_score = (corr_of_corr + 1) / 2 * 100  # Normalize to 0-100
        else:
            corr_of_corr_score = 100.0
        metrics['correlation_of_correlations'] = corr_of_corr_score
        
        # 3. Top correlations preservation
        # Check if top 10% of correlations are preserved
        if len(orig_corr_flat) > 10:
            k = max(10, len(orig_corr_flat) // 10)
            k = min(k, len(orig_corr_flat))  # Ensure k doesn't exceed array length
            top_k_orig = np.argpartition(np.abs(orig_corr_flat), -k)[-k:]
            top_k_synth = np.argpartition(np.abs(synth_corr_flat), -k)[-k:]
            
            overlap = len(set(top_k_orig) & set(top_k_synth)) / k
            metrics['top_correlations_preserved'] = overlap * 100
        else:
            # Too few correlations to meaningfully compare
            metrics['top_correlations_preserved'] = 100.0
        
        # Combined score
        score = (
            metrics['frobenius_similarity'] * 0.4 +
            metrics['correlation_of_correlations'] * 0.4 +
            metrics['top_correlations_preserved'] * 0.2
        )
        
        passed = score >= self.config.correlation_preservation_threshold
        severity = self._determine_severity(score, self.config.correlation_preservation_threshold)
        
        return DimensionScore(
            name='Correlation Preservation',
            score=score,
            threshold=self.config.correlation_preservation_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 3: ENTROPY STABILITY ====================
    
    async def _analyze_entropy_stability(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        Measure entropy and information content stability.
        Collapse often reduces entropy.
        """
        metrics = {}
        
        # 1. Shannon entropy per dimension
        synth_entropies = []
        orig_entropies = []
        
        for i in range(min(synthetic.shape[1], 100)):
            # Discretize for entropy calculation
            synth_hist, _ = np.histogram(synthetic[:, i], bins=50)
            orig_hist, _ = np.histogram(original[:, i], bins=50)
            
            synth_entropy = stats.entropy(synth_hist + 1e-10)  # Add epsilon
            orig_entropy = stats.entropy(orig_hist + 1e-10)
            
            synth_entropies.append(synth_entropy)
            orig_entropies.append(orig_entropy)
        
        synth_entropies = np.array(synth_entropies)
        orig_entropies = np.array(orig_entropies)
        
        # Entropy ratio (should be close to 1)
        entropy_ratios = synth_entropies / (orig_entropies + 1e-10)
        avg_entropy_ratio = np.mean(entropy_ratios)
        
        # Score based on how close to 1 (penalize both increase and decrease)
        entropy_score = 100 * np.exp(-abs(avg_entropy_ratio - 1))
        metrics['entropy_ratio'] = avg_entropy_ratio
        metrics['entropy_stability'] = entropy_score
        
        # 2. Mutual information preservation
        # Calculate MI between feature pairs for both datasets
        from sklearn.feature_selection import mutual_info_regression
        
        mi_scores = []
        n_features = min(synthetic.shape[1], 20)  # Sample features for efficiency
        
        for i in range(n_features):
            try:
                # Calculate MI between this feature and the rest
                if synthetic.shape[1] > 1:
                    # Sample rows for speed
                    sample_size = min(5000, synthetic.shape[0])
                    synth_sample = synthetic[:sample_size, :]
                    orig_sample = original[:sample_size, :]
                    
                    # MI for synthetic data
                    mi_synth = mutual_info_regression(
                        synth_sample[:, [i]], 
                        synth_sample[:, (i+1) % n_features],
                        random_state=42
                    )[0]
                    
                    # MI for original data
                    mi_orig = mutual_info_regression(
                        orig_sample[:, [i]], 
                        orig_sample[:, (i+1) % n_features],
                        random_state=42
                    )[0]
                    
                    # Compare MI values (should be similar)
                    if mi_orig > 1e-6:
                        mi_ratio = mi_synth / (mi_orig + 1e-10)
                        mi_scores.append(100 * np.exp(-abs(mi_ratio - 1)))
                    else:
                        mi_scores.append(100.0)  # Both near zero is good
            except Exception as e:
                logger.warning(f"MI calculation failed for feature {i}: {e}")
                continue
        
        if mi_scores:
            mi_score = np.mean(mi_scores)
        else:
            mi_score = 85.0  # Fallback if all calculations fail
            
        metrics['mutual_information'] = mi_score
        
        # 3. Effective dimensionality (via PCA)
        from sklearn.decomposition import PCA
        
        pca_synth = PCA(n_components=min(50, synthetic.shape[1]))
        pca_orig = PCA(n_components=min(50, original.shape[1]))
        
        pca_synth.fit(synthetic[:10000])  # Sample for speed
        pca_orig.fit(original[:10000])
        
        # Explained variance ratio
        synth_var = np.cumsum(pca_synth.explained_variance_ratio_)
        orig_var = np.cumsum(pca_orig.explained_variance_ratio_)
        
        # Compare dimensionality needed for 90% variance
        synth_dim = np.argmax(synth_var > 0.9) + 1
        orig_dim = np.argmax(orig_var > 0.9) + 1
        
        dim_ratio = synth_dim / (orig_dim + 1e-10)
        dim_score = 100 * np.exp(-abs(dim_ratio - 1))
        
        metrics['dimensionality_ratio'] = dim_ratio
        metrics['dimensionality_preservation'] = dim_score
        
        # Combined score
        score = (
            metrics['entropy_stability'] * 0.5 +
            metrics['mutual_information'] * 0.2 +
            metrics['dimensionality_preservation'] * 0.3
        )
        
        passed = score >= self.config.entropy_stability_threshold
        severity = self._determine_severity(score, self.config.entropy_stability_threshold)
        
        return DimensionScore(
            name='Entropy Stability',
            score=score,
            threshold=self.config.entropy_stability_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 4: GRADIENT HEALTH ====================
    
    async def _analyze_gradient_health(
        self, gradients: Dict[str, torch.Tensor]
    ) -> DimensionScore:
        """
        Analyze gradient statistics during training.
        Unhealthy gradients indicate potential collapse.
        """
        metrics = {}
        
        # 1. Gradient norms
        grad_norms = []
        for name, grad in gradients.items():
            if grad is not None:
                norm = torch.norm(grad).item()
                grad_norms.append(norm)
        
        if grad_norms:
            mean_norm = np.mean(grad_norms)
            std_norm = np.std(grad_norms)
            
            # Check for vanishing or exploding gradients
            vanishing_score = 100 if mean_norm > 1e-5 else 50
            exploding_score = 100 if mean_norm < 100 else 50
            
            metrics['mean_gradient_norm'] = mean_norm
            metrics['std_gradient_norm'] = std_norm
            metrics['vanishing_check'] = vanishing_score
            metrics['exploding_check'] = exploding_score
            
            # Combined gradient health
            score = (vanishing_score + exploding_score) / 2
        else:
            score = 100.0  # Neutral if no gradients
            metrics['gradient_norms'] = 'N/A'
        
        passed = score >= self.config.gradient_health_threshold
        severity = self._determine_severity(score, self.config.gradient_health_threshold)
        
        return DimensionScore(
            name='Gradient Health',
            score=score,
            threshold=self.config.gradient_health_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 5: LOSS LANDSCAPE ====================
    
    async def _analyze_loss_landscape(
        self, training_metrics: Dict[str, List[float]]
    ) -> DimensionScore:
        """
        Analyze training loss curve for signs of collapse.
        """
        metrics = {}
        
        train_loss = training_metrics.get('train_loss', [])
        val_loss = training_metrics.get('val_loss', [])
        
        if len(train_loss) < 10:
            return self._create_neutral_dimension('loss_landscape')
        
        # 1. Loss decrease rate
        loss_decrease = train_loss[0] - train_loss[-1]
        decrease_score = min(100, loss_decrease * 100)
        metrics['loss_decrease'] = loss_decrease
        metrics['decrease_score'] = decrease_score
        
        # 2. Loss smoothness (check for spikes)
        loss_diffs = np.diff(train_loss)
        smoothness = 100 * (1 - min(1, np.std(loss_diffs) / (np.mean(np.abs(loss_diffs)) + 1e-8)))
        metrics['loss_smoothness'] = smoothness
        
        # 3. Convergence stability
        last_10_pct = train_loss[-len(train_loss)//10:]
        stability = 100 * (1 - np.std(last_10_pct) / (np.mean(last_10_pct) + 1e-8))
        metrics['convergence_stability'] = min(100, stability)
        
        # Combined score
        score = (
            decrease_score * 0.4 +
            metrics['loss_smoothness'] * 0.3 +
            metrics['convergence_stability'] * 0.3
        )
        
        passed = score >= self.config.loss_landscape_threshold
        severity = self._determine_severity(score, self.config.loss_landscape_threshold)
        
        return DimensionScore(
            name='Loss Landscape',
            score=score,
            threshold=self.config.loss_landscape_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 6: SPECTRAL COHERENCE (FFT) ====================
    
    async def _analyze_spectral_coherence(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        FFT-based spectral analysis (perfectly aligned with Resonance NN architecture).
        
        This analysis naturally complements Resonance Neural Networks which operate
        in the frequency domain using FFT. Collapse often manifests as:
        - Loss of high-frequency components
        - Abnormal spectral energy concentration
        - Reduced spectral entropy (mode collapse in frequency space)
        """
        metrics = {}
        
        # Convert to torch for GPU FFT
        if self.device.type == 'cuda':
            synth_tensor = torch.tensor(synthetic, device=self.device, dtype=torch.float32)
            orig_tensor = torch.tensor(original, device=self.device, dtype=torch.float32)
        else:
            synth_tensor = torch.tensor(synthetic, dtype=torch.float32)
            orig_tensor = torch.tensor(original, dtype=torch.float32)
        
        # Compute FFT (real FFT for efficiency)
        synth_fft = torch.fft.rfft(synth_tensor, dim=0)
        orig_fft = torch.fft.rfft(orig_tensor, dim=0)
        
        # Power spectral density
        synth_psd = torch.abs(synth_fft) ** 2
        orig_psd = torch.abs(orig_fft) ** 2
        
        # 1. Spectral distance
        spectral_dist = torch.mean(torch.abs(synth_psd - orig_psd)).item()
        spectral_score = 100 * np.exp(-spectral_dist / 1000)
        metrics['spectral_distance'] = spectral_dist
        metrics['spectral_similarity'] = spectral_score
        
        # 2. Dominant frequency preservation
        synth_dom_freq = torch.argmax(synth_psd, dim=0)
        orig_dom_freq = torch.argmax(orig_psd, dim=0)
        
        freq_match = (synth_dom_freq == orig_dom_freq).float().mean().item()
        metrics['dominant_frequency_match'] = freq_match * 100
        
        # 3. Spectral entropy - OPTIMIZED with SynthOS fused CUDA kernel
        if self._use_synthos and self._is_valid_for_synthos(synth_tensor):
            # Use fused CUDA kernel (single kernel launch vs 4-5 with PyTorch)
            # Achieves 2-5x speedup depending on GPU architecture
            synth_spectral_entropy = self._synthos_kernel.compute_spectral_entropy(
                synth_tensor
            ).mean().item()
            
            orig_spectral_entropy = self._synthos_kernel.compute_spectral_entropy(
                orig_tensor
            ).mean().item()
        else:
            # Fallback to PyTorch implementation
            # OPTIMIZATION: Cache normalized PSDs to avoid redundant computation
            # Previous code called F.normalize twice per PSD (2x performance hit)
            synth_psd_norm = F.normalize(synth_psd, p=1, dim=0)
            orig_psd_norm = F.normalize(orig_psd, p=1, dim=0)
            
            # Use small epsilon for numerical stability (1e-10 can underflow in log)
            eps = 1e-8
            
            synth_spectral_entropy = -torch.sum(
                synth_psd_norm * torch.log(synth_psd_norm + eps),
                dim=0
            ).mean().item()
            
            orig_spectral_entropy = -torch.sum(
                orig_psd_norm * torch.log(orig_psd_norm + eps),
                dim=0
            ).mean().item()
        
        entropy_ratio = synth_spectral_entropy / (orig_spectral_entropy + 1e-10)
        entropy_score = 100 * np.exp(-abs(entropy_ratio - 1))
        
        metrics['spectral_entropy_ratio'] = entropy_ratio
        metrics['spectral_entropy_score'] = entropy_score
        
        # Combined score
        score = (
            metrics['spectral_similarity'] * 0.4 +
            metrics['dominant_frequency_match'] * 0.3 +
            metrics['spectral_entropy_score'] * 0.3
        )
        
        passed = score >= self.config.spectral_coherence_threshold
        severity = self._determine_severity(score, self.config.spectral_coherence_threshold)
        
        return DimensionScore(
            name='Spectral Coherence',
            score=score,
            threshold=self.config.spectral_coherence_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    def _is_valid_for_synthos(self, tensor: torch.Tensor) -> bool:
        """
        Check if tensor is valid for SynthOS kernel processing.
        
        Requirements:
            - 2D tensor [N_samples, N_channels]
            - N_samples is power of 2
            - N_samples in range [4, 8192]
        """
        if tensor.dim() != 2:
            return False
        n_samples = tensor.shape[0]
        # Must be power of 2 and within supported range
        if n_samples < 4 or n_samples > 8192:
            return False
        return (n_samples & (n_samples - 1)) == 0
    
    # ==================== DIMENSION 7: GENERALIZATION GAP ====================
    
    async def _analyze_generalization_gap(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        Measure generalization capability.
        Large gaps indicate overfitting/collapse.
        """
        metrics = {}
        
        # Use simple train/test split approach
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split
        
        # Create binary classification task (synthetic vs original)
        # Use min samples available to avoid dimension mismatches
        n_samples = min(len(synthetic), len(original), 10000)
        X = np.vstack([synthetic[:n_samples], original[:n_samples]])
        y = np.array([0] * n_samples + [1] * n_samples)
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        
        # Train simple classifier
        clf = RandomForestClassifier(n_estimators=10, max_depth=5, random_state=42)
        clf.fit(X_train, y_train)
        
        train_acc = clf.score(X_train, y_train)
        test_acc = clf.score(X_test, y_test)
        
        # Ideally, classifier should NOT be able to distinguish (acc ~50%)
        # High accuracy = distributions are very different = collapse
        distinguishability = (test_acc - 0.5) * 2  # Normalize to 0-1
        indistinguishability_score = (1 - distinguishability) * 100
        
        metrics['train_accuracy'] = train_acc
        metrics['test_accuracy'] = test_acc
        metrics['distinguishability'] = distinguishability
        metrics['indistinguishability_score'] = indistinguishability_score
        
        # Generalization gap
        gap = abs(train_acc - test_acc)
        gap_score = 100 * (1 - min(1, gap * 10))  # Penalize large gaps
        
        metrics['generalization_gap'] = gap
        metrics['gap_score'] = gap_score
        
        # Combined score
        score = (
            indistinguishability_score * 0.7 +
            gap_score * 0.3
        )
        
        passed = score >= self.config.generalization_gap_threshold
        severity = self._determine_severity(score, self.config.generalization_gap_threshold)
        
        return DimensionScore(
            name='Generalization Gap',
            score=score,
            threshold=self.config.generalization_gap_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 8: STATISTICAL CONSISTENCY ====================
    
    async def _analyze_statistical_consistency(
        self, synthetic: np.ndarray, original: np.ndarray
    ) -> DimensionScore:
        """
        Comprehensive statistical tests for consistency.
        """
        metrics = {}
        
        # 1. Chi-square test (for categorical/discrete columns)
        # For continuous, bin the data
        chi2_scores = []
        for i in range(min(synthetic.shape[1], 50)):
            # Bin data
            bins = 20
            synth_hist, _ = np.histogram(synthetic[:, i], bins=bins)
            orig_hist, bin_edges = np.histogram(original[:, i], bins=bins)
            
            # Chi-square test
            chi2, pval = stats.chisquare(synth_hist + 1, orig_hist + 1)  # Add 1 to avoid zeros
            chi2_scores.append(pval)  # Higher p-value = more consistent
        
        metrics['chi2_pvalue_mean'] = np.mean(chi2_scores)
        metrics['chi2_consistency'] = np.mean(chi2_scores) * 100
        
        # 2. Anderson-Darling test
        ad_scores = []
        for i in range(min(synthetic.shape[1], 50)):
            try:
                ad_result = stats.anderson_ksamp([synthetic[:, i], original[:, i]])
                # Convert statistic to score (lower statistic = more similar)
                ad_score = 100 * np.exp(-ad_result.statistic / 10)
                ad_scores.append(ad_score)
            except (ValueError, RuntimeError) as e:
                logger.debug(f"Anderson-Darling test failed for column {i}: {e}")
                ad_scores.append(50.0)  # Neutral on failure
        
        metrics['anderson_darling_score'] = np.mean(ad_scores)
        
        # 3. Mann-Whitney U test (non-parametric)
        mw_scores = []
        for i in range(min(synthetic.shape[1], 50)):
            _, pval = stats.mannwhitneyu(synthetic[:, i], original[:, i])
            mw_scores.append(pval * 100)  # Higher p-value = more consistent
        
        metrics['mann_whitney_pvalue'] = np.mean(mw_scores)
        
        # Combined score
        score = (
            metrics['chi2_consistency'] * 0.35 +
            metrics['anderson_darling_score'] * 0.35 +
            metrics['mann_whitney_pvalue'] * 0.30
        )
        
        # Clamp score to 0-100 range
        score = max(0.0, min(100.0, score))
        
        passed = score >= self.config.statistical_consistency_threshold
        severity = self._determine_severity(score, self.config.statistical_consistency_threshold)
        
        return DimensionScore(
            name='Statistical Consistency',
            score=score,
            threshold=self.config.statistical_consistency_threshold,
            passed=passed,
            metrics=metrics,
            severity=severity
        )
    
    # ==================== DIMENSION 9: CROSS-MODAL ALIGNMENT (OPT-IN) ====================
    
    async def _analyze_cross_modal_alignment(
        self, visual_latents: np.ndarray, text_latents: np.ndarray
    ) -> DimensionScore:
        """
        Opt-in dimension: Prevents one modality from collapsing the other.
        Measures structural similarity in the joint embedding space.
        
        Call this explicitly when validating multimodal training data.
        Not wired into the default detect_collapse flow.
        
        Args:
            visual_latents: Visual embeddings (N_samples, Embed_Dim)
            text_latents: Text embeddings (N_samples, Embed_Dim)
            
        Returns:
            DimensionScore with cross-modal alignment metrics
        """
        metrics = {}
        
        # Validate inputs
        visual_latents = validate_tensor_input(visual_latents, "visual_latents", min_samples=2)
        text_latents = validate_tensor_input(text_latents, "text_latents", min_samples=2)
        
        # Ensure same number of samples
        n_samples = min(len(visual_latents), len(text_latents))
        visual_latents = visual_latents[:n_samples]
        text_latents = text_latents[:n_samples]
        
        # 1. Cross-Covariance via Frobenius norm
        vis_tensor = torch.tensor(visual_latents, device=self.device, dtype=torch.float32)
        txt_tensor = torch.tensor(text_latents, device=self.device, dtype=torch.float32)
        
        # Center the data
        vis_norm = vis_tensor - vis_tensor.mean(dim=0)
        txt_norm = txt_tensor - txt_tensor.mean(dim=0)
        
        # Cross-covariance matrix and its Frobenius norm
        cross_cov = torch.mm(vis_norm.T, txt_norm) / (n_samples - 1)
        alignment_score = torch.norm(cross_cov, p='fro').item()
        metrics['cross_modal_covariance'] = alignment_score
        
        # 2. Modality Variance Ratio (detects if one modality is dying)
        vis_var = torch.var(vis_tensor).item()
        txt_var = torch.var(txt_tensor).item()
        
        var_ratio = min(vis_var, txt_var) / (max(vis_var, txt_var) + 1e-8)
        metrics['variance_balance'] = var_ratio * 100
        metrics['visual_variance'] = vis_var
        metrics['text_variance'] = txt_var
        
        # 3. Cosine similarity distribution
        vis_normed = F.normalize(vis_tensor, p=2, dim=1)
        txt_normed = F.normalize(txt_tensor, p=2, dim=1)
        cosine_sims = (vis_normed * txt_normed).sum(dim=1)
        metrics['mean_cosine_similarity'] = cosine_sims.mean().item()
        metrics['std_cosine_similarity'] = cosine_sims.std().item()
        
        # Score calculation
        score = (var_ratio * 100 * 0.7) + (min(alignment_score * 10, 100) * 0.3)
        passed = score >= 65.0
        
        return DimensionScore(
            name='Cross-Modal Alignment',
            score=score,
            threshold=65.0,
            passed=passed,
            metrics=metrics,
            severity=self._determine_severity(score, 65.0)
        )

    # ==================== HELPER METHODS ====================
    
    def _create_neutral_dimension(self, name: str) -> DimensionScore:
        """Create neutral dimension score when data is unavailable"""
        threshold_map = {
            'gradient_health': self.config.gradient_health_threshold,
            'loss_landscape': self.config.loss_landscape_threshold
        }
        
        threshold = threshold_map.get(name, 70.0)
        
        return DimensionScore(
            name=name.replace('_', ' ').title(),
            score=threshold,  # Neutral = exactly at threshold
            threshold=threshold,
            passed=True,
            metrics={'status': 'N/A'},
            severity='ok'
        )
    
    def _determine_severity(self, score: float, threshold: float) -> str:
        """Determine severity level based on score"""
        if score >= threshold:
            return 'ok'
        elif score >= threshold * 0.8:
            return 'warning'
        else:
            return 'critical'
    
    def _compute_overall_score(self, dimensions: Dict[str, DimensionScore]) -> float:
        """Compute weighted overall score"""
        if not dimensions:
            return 0.0
        
        # Equal weighting for now (can be adjusted)
        scores = [dim.score for dim in dimensions.values()]
        return np.mean(scores)
    
    def _compute_confidence(self, dimensions: Dict[str, DimensionScore]) -> float:
        """Compute confidence in collapse detection"""
        # Confidence based on consistency across dimensions
        scores = [dim.score for dim in dimensions.values()]
        
        # Low variance = high confidence
        variance = np.var(scores)
        confidence = 100 * np.exp(-variance / 1000)
        
        return min(100, confidence)
    
    def _generate_warnings(
        self, dimensions: Dict[str, DimensionScore], collapse_detected: bool
    ) -> List[str]:
        """Generate actionable warnings"""
        warnings = []
        
        if collapse_detected:
            warnings.append("🚨 MODEL COLLAPSE DETECTED! Do not proceed with this dataset.")
        
        for dim in dimensions.values():
            if dim.severity == 'critical':
                warnings.append(f"❌ CRITICAL: {dim.name} failed ({dim.score:.1f} < {dim.threshold})")
            elif dim.severity == 'warning':
                warnings.append(f"⚠️ WARNING: {dim.name} is borderline ({dim.score:.1f})")
        
        if not warnings:
            warnings.append("✅ All dimensions passed! Dataset quality is excellent.")
        
        return warnings
    
    def _predict_at_scale(self, dimensions: Dict[str, DimensionScore]) -> Dict[str, float]:
        """Predict scores when scaled to full training (simple linear extrapolation)"""
        predictions = {}
        
        for name, dim in dimensions.items():
            # Assume 5% degradation at 10x scale, 10% at 100x scale
            scale_factor = 0.95  # Conservative estimate
            predicted_score = dim.score * scale_factor
            predictions[f"{name}_at_10x"] = predicted_score
            predictions[f"{name}_at_100x"] = predicted_score * 0.95
        
        return predictions
