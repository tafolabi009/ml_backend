import sys
import pathlib
import asyncio
import tempfile
import numpy as np

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
