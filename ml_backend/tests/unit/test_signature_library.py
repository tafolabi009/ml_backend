import pytest
import numpy as np
import torch
from unittest.mock import MagicMock, patch, mock_open, AsyncMock
import sys
import asyncio

from src.collapse_engine.signature_library import (
    AdvancedSignatureLibrary,
    CollapseSignature,
    MatchResult
)

class TestAdvancedSignatureLibrary:
    
    @pytest.fixture
    def library(self):
        with patch('src.collapse_engine.signature_library.Path') as mock_path:
            with patch('src.collapse_engine.signature_library.h5py') as mock_h5py:
                # Force FAISS to be unavailable to test fallback
                with patch('src.collapse_engine.signature_library.FAISS_AVAILABLE', False):
                    # Mock autoencoder to avoid torch overhead/issues
                    with patch('src.collapse_engine.signature_library.SignatureAutoencoder') as mock_ae:
                        mock_ae_instance = MagicMock()
                        # Mock encode to return a tensor
                        mock_ae_instance.encode.return_value = torch.randn(1, 256)
                        mock_ae.return_value = mock_ae_instance
                        
                        # Patch background tasks to avoid dangling tasks
                        with patch('src.collapse_engine.signature_library.AdvancedSignatureLibrary._save_signature_async', new_callable=AsyncMock) as mock_save:
                            with patch('src.collapse_engine.signature_library.AdvancedSignatureLibrary._build_index', new_callable=AsyncMock) as mock_build:
                                with patch('src.collapse_engine.signature_library.AdvancedSignatureLibrary._update_clusters', new_callable=AsyncMock) as mock_cluster:
                                    with patch('src.collapse_engine.signature_library.asyncio.create_task') as mock_create_task:
                                        lib = AdvancedSignatureLibrary(
                                            storage_path="/tmp/test_sigs",
                                            use_gpu=False,
                                            use_pq=False,
                                            use_hnsw=False
                                        )
                                        # Manually set autoencoder to mock instance
                                        lib.autoencoder = mock_ae_instance
                                        return lib

    def test_add_signature(self, library):
        """Test adding a signature"""
        # Mock autoencoder encode output
        library.autoencoder.encode.return_value = torch.randn(1, 256)
        
        async def run_test():
            sig_id = await library.add_signature(
                dataset_id="dataset_1",
                dimension_scores={'dim1': 50.0},
                collapse_score=60.0,
                data_statistics={'mean': 0.0},
                metadata={'meta': 'data'}
            )
            return sig_id
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            sig_id = loop.run_until_complete(run_test())
        finally:
            loop.close()
        
        assert len(library.signatures) == 1
        assert library.signatures[0].signature_id == sig_id
        assert library.signatures[0].dataset_id == "dataset_1"

    def test_find_similar_patterns_empty(self, library):
        """Test search with empty library"""
        async def run_test():
            return await library.find_similar_patterns(
                dimension_scores={},
                data_statistics={}
            )
            
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            matches, metrics = loop.run_until_complete(run_test())
        finally:
            loop.close()

        assert len(matches) == 0
        assert metrics.candidates_evaluated == 0

    def test_find_similar_patterns_exact(self, library):
        """Test exact search"""
        # Add a signature first
        library.autoencoder.encode.return_value = torch.randn(1, 256)
        
        async def run_test():
            await library.add_signature(
                dataset_id="dataset_1",
                dimension_scores={'dim1': 50.0},
                collapse_score=60.0,
                data_statistics={'mean': 0.0}
            )
            
            # Search
            return await library.find_similar_patterns(
                dimension_scores={'dim1': 50.0},
                data_statistics={'mean': 0.0},
                search_strategy="exact",
                top_k=1
            )
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            matches, metrics = loop.run_until_complete(run_test())
        finally:
            loop.close()
        
        assert len(matches) == 1
        assert matches[0].dataset_id == "dataset_1"
        assert metrics.query_type == "exact"

    def test_create_raw_features(self, library):
        """Test feature vector creation"""
        dim_scores = {
            'distribution_fidelity': 80.0,
            'correlation_preservation': 70.0
        }
        stats = {
            'mean': 0.5,
            'std': 1.0,
            'skewness': 0.1,
            'kurtosis': 3.0
        }
        
        features = library._create_raw_features(dim_scores, stats)
        
        assert isinstance(features, np.ndarray)
        assert len(features) == library.raw_feature_dim
        # Check first few values (normalized dim scores)
        assert features[0] == 0.8  # 80.0 / 100.0
        assert features[1] == 0.7  # 70.0 / 100.0

    def test_compute_confidence(self, library):
        """Test confidence computation"""
        sig = CollapseSignature(
            signature_id="test",
            vector=np.zeros(256),
            raw_features=np.zeros(128),
            dataset_id="d1",
            collapse_score=50.0,
            dimension_scores={'dim1': 40.0},
            metadata={},
            timestamp="2023-01-01",
            prediction_accuracy=0.9,
            usage_count=5
        )
        
        conf = library._compute_confidence(
            similarity=0.8,
            signature=sig,
            current_dimension_scores={'dim1': 40.0}
        )
        
        assert 0 <= conf <= 100
        # Should be high because similarity is high and dimensions align
        assert conf > 50

    def test_explain_match(self, library):
        """Test match explanation"""
        sig = CollapseSignature(
            signature_id="test",
            vector=np.zeros(256),
            raw_features=np.zeros(128),
            dataset_id="d1",
            collapse_score=40.0,
            dimension_scores={'dim1': 40.0, 'dim2': 80.0},
            metadata={},
            timestamp="2023-01-01",
            usage_count=10
        )
        
        explanation = library._explain_match(
            signature=sig,
            current_dimension_scores={'dim1': 45.0, 'dim2': 20.0},
            similarity=0.85
        )
        
        assert 'matching_dimensions' in explanation
        assert 'mismatching_dimensions' in explanation
        
        # dim1 (40 vs 45) should match
        match_dims = [d['dimension'] for d in explanation['matching_dimensions']]
        assert 'dim1' in match_dims
        
        # dim2 (80 vs 20) should mismatch
        mismatch_dims = [d['dimension'] for d in explanation['mismatching_dimensions']]
        assert 'dim2' in mismatch_dims
        
        assert len(explanation['key_insights']) > 0

    def test_update_signature(self, library):
        """Test updating a signature"""
        # Add signature
        library.autoencoder.encode.return_value = torch.randn(1, 256)
        
        async def run_test():
            sig_id = await library.add_signature(
                dataset_id="dataset_1",
                dimension_scores={},
                collapse_score=60.0,
                data_statistics={}
            )
            
            # Update
            await library.update_signature(
                signature_id=sig_id,
                new_collapse_score=50.0,
                accuracy_feedback=1.0
            )
            return sig_id
            
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            sig_id = loop.run_until_complete(run_test())
        finally:
            loop.close()
        
        sig = library.signatures[0]
        assert sig.collapse_score == 50.0
        assert sig.usage_count == 1
        assert sig.prediction_accuracy > 0

    def test_get_statistics(self, library):
        """Test statistics generation"""
        stats = library.get_statistics()
        assert stats['total_signatures'] == 0
        
        # Add signature manually to list
        library.signatures.append(CollapseSignature(
            signature_id="test",
            vector=np.zeros(256),
            raw_features=np.zeros(128),
            dataset_id="d1",
            collapse_score=50.0,
            dimension_scores={},
            metadata={},
            timestamp="2023-01-01",
            cluster_id=0
        ))
        
        stats = library.get_statistics()
        assert stats['total_signatures'] == 1
        assert stats['collapse_score_distribution']['mean'] == 50.0
