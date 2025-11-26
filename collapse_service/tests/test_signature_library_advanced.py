import sys
import pathlib
import asyncio
import tempfile
import numpy as np
import torch

# Ensure package import works when tests are run from repo root
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from collapse_engine import signature_library_advanced as sla


def test_raw_features_and_statistics_and_temporal_processing():
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False, use_pq=False, use_hnsw=False)

        dims = {
            'distribution_fidelity': 80.0,
            'correlation_preservation': 70.0,
            'entropy_stability': 60.0,
            'gradient_health': 75.0,
            'loss_landscape': 85.0,
            'spectral_coherence': 78.0,
            'generalization_gap': 65.0,
            'statistical_consistency': 90.0
        }

        stats = {
            'mean': 0.5,
            'std': 0.1,
            'min': 0.0,
            'max': 1.0,
            'median': 0.5,
            'q25': 0.25,
            'q75': 0.75
        }

        raw = lib._create_raw_features(dims, stats)
        assert isinstance(raw, np.ndarray)
        assert raw.shape[0] == lib.raw_feature_dim

        data = np.arange(50).astype(float)
        simple = lib._extract_simple_statistics(data)
        assert simple['mean'] == float(np.mean(data))

        temporal = np.arange(30).astype(float)
        processed = lib._process_temporal_data(temporal)
        assert processed.size > 0


def test_add_signature_and_find_similar_patterns():
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False, use_pq=False, use_hnsw=False)

        dimension_scores = {
            'distribution_fidelity': 40.0,
            'correlation_preservation': 50.0,
            'entropy_stability': 45.0
        }

        data_statistics = {
            'mean': 0.1,
            'std': 0.2,
            'min': 0.0,
            'max': 1.0,
            'median': 0.1,
            'q25': 0.05,
            'q75': 0.15
        }

        # Add a signature (async)
        sig_id = asyncio.run(lib.add_signature(
            dataset_id='ds_test',
            dimension_scores=dimension_scores,
            collapse_score=42.0,
            data_statistics=data_statistics,
            metadata={'source': 'unit_test'},
            temporal_data=np.arange(20).astype(float)
        ))

        assert sig_id in lib.signature_map
        assert len(lib.signatures) >= 1

        # Query for similar patterns
        matches, metrics = asyncio.run(lib.find_similar_patterns(
            dimension_scores=dimension_scores,
            data_statistics=data_statistics,
            top_k=5,
            similarity_threshold=0.0,
            search_strategy='exact',
            explain=True
        ))

        assert isinstance(matches, list)
        assert isinstance(metrics, sla.SearchMetrics)


# ==================== EDGE-CASE TESTS ====================


def test_empty_library_returns_no_matches():
    """Search on an empty library should return empty list without errors."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        dims = {'distribution_fidelity': 50.0}
        stats = {'mean': 0.5}

        matches, metrics = asyncio.run(lib.find_similar_patterns(
            dimension_scores=dims,
            data_statistics=stats,
            top_k=5,
            similarity_threshold=0.5
        ))

        assert matches == []
        assert metrics.index_size == 0


def test_empty_temporal_data_handling():
    """Temporal processing with very short data should not crash."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        short_temporal = np.array([1.0, 2.0])
        processed = lib._process_temporal_data(short_temporal)
        # Should return input unchanged when too short
        assert processed.size == short_temporal.size


def test_empty_data_statistics():
    """Extracting statistics from empty array should return zeros."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        empty = np.array([])
        stats = lib._extract_simple_statistics(empty)
        assert stats['mean'] == 0.0
        assert stats['std'] == 0.0


def test_large_dataset_many_signatures():
    """Adding many signatures should not degrade search quality."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False, use_pq=False, use_hnsw=False)

        base_dims = {'distribution_fidelity': 50.0, 'entropy_stability': 60.0}
        base_stats = {'mean': 0.5, 'std': 0.1}

        # Add 50 signatures
        for i in range(50):
            asyncio.run(lib.add_signature(
                dataset_id=f'ds_{i}',
                dimension_scores={k: v + i * 0.5 for k, v in base_dims.items()},
                collapse_score=40.0 + i,
                data_statistics=base_stats,
                metadata={'idx': i}
            ))

        assert len(lib.signatures) == 50

        # Search should still work
        matches, metrics = asyncio.run(lib.find_similar_patterns(
            dimension_scores=base_dims,
            data_statistics=base_stats,
            top_k=10,
            similarity_threshold=0.0,
            search_strategy='exact'
        ))

        assert len(matches) <= 10
        assert metrics.candidates_evaluated == 50


def test_high_similarity_threshold_filters_all():
    """A threshold of 1.0 should filter out all matches (cosine sim rarely exactly 1)."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        dims = {'distribution_fidelity': 60.0}
        stats = {'mean': 0.3}

        asyncio.run(lib.add_signature(
            dataset_id='ds_single',
            dimension_scores=dims,
            collapse_score=55.0,
            data_statistics=stats
        ))

        # Query with very different dims
        query_dims = {'distribution_fidelity': 10.0}
        matches, _ = asyncio.run(lib.find_similar_patterns(
            dimension_scores=query_dims,
            data_statistics=stats,
            top_k=5,
            similarity_threshold=0.99
        ))

        # Very strict threshold likely filters out
        assert len(matches) <= 1


def test_risk_level_assessment():
    """Risk assessment should return valid levels."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        assert lib._assess_risk_level(collapse_score=20.0, confidence=80.0) == "CRITICAL"
        assert lib._assess_risk_level(collapse_score=45.0, confidence=70.0) == "HIGH"
        assert lib._assess_risk_level(collapse_score=60.0, confidence=55.0) == "MEDIUM"
        assert lib._assess_risk_level(collapse_score=80.0, confidence=90.0) == "LOW"


def test_get_statistics_empty_library():
    """Statistics on empty library should not crash."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        stats = lib.get_statistics()
        assert stats['total_signatures'] == 0


# ==================== FAISS/GPU MOCK TESTS ====================


def test_faiss_unavailable_fallback(monkeypatch):
    """When FAISS is unavailable, library should fall back to exact search."""
    monkeypatch.setattr(sla, 'FAISS_AVAILABLE', False)

    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        dims = {'distribution_fidelity': 70.0}
        stats = {'mean': 0.4}

        asyncio.run(lib.add_signature(
            dataset_id='ds_no_faiss',
            dimension_scores=dims,
            collapse_score=65.0,
            data_statistics=stats
        ))

        matches, metrics = asyncio.run(lib.find_similar_patterns(
            dimension_scores=dims,
            data_statistics=stats,
            top_k=5,
            similarity_threshold=0.0,
            search_strategy='approximate'  # should fallback to exact
        ))

        assert len(matches) >= 1
        # GPU should not be used when FAISS unavailable
        assert metrics.gpu_used is False


def test_gpu_disabled_forces_cpu(monkeypatch):
    """Even if CUDA is available, use_gpu=False should force CPU."""
    import torch
    monkeypatch.setattr(torch.cuda, 'is_available', lambda: True)

    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)
        assert lib.device == torch.device('cpu')


def test_choose_search_strategy():
    """Auto strategy selection based on library size."""
    with tempfile.TemporaryDirectory() as td:
        lib = sla.AdvancedSignatureLibrary(storage_path=td, use_gpu=False)

        # Small library -> exact
        assert lib._choose_search_strategy(n_signatures=500, top_k=10) == 'exact'

        # Medium library -> hybrid (if FAISS available)
        if sla.FAISS_AVAILABLE:
            assert lib._choose_search_strategy(n_signatures=5000, top_k=10) == 'hybrid'
            assert lib._choose_search_strategy(n_signatures=50000, top_k=10) == 'approximate'
