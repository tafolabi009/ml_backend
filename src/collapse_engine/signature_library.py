"""
Signature Library - Historical collapse pattern matching
Optimized for fast lookup with LSH (Locality-Sensitive Hashing)

Features:
- Vectorized collapse signatures
- Fast similarity search with FAISS
- Persistent storage with HDF5
- Pattern matching for early detection
- Historical knowledge base
"""

import numpy as np
import torch
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import h5py
import json
from pathlib import Path
import hashlib
import logging

logger = logging.getLogger(__name__)

try:
    import faiss
    FAISS_AVAILABLE = True
except ImportError:
    logger.warning("FAISS not available. Using brute-force search.")
    FAISS_AVAILABLE = False


@dataclass
class CollapseSignature:
    """Vectorized collapse pattern signature"""
    signature_id: str
    vector: np.ndarray  # 512-dim embedding
    dataset_id: str
    collapse_score: float
    dimension_scores: Dict[str, float]
    metadata: Dict[str, Any]
    timestamp: str


@dataclass
class MatchResult:
    """Pattern match result"""
    signature_id: str
    similarity: float  # 0-1
    dataset_id: str
    collapse_score: float
    confidence: float
    recommendations: List[str]


class SignatureLibrary:
    """
    Maintains library of historical collapse patterns.
    Enables fast pattern matching for early detection.
    """
    
    def __init__(
        self,
        storage_path: str = "/workspaces/ml_backend/data/signatures",
        embedding_dim: int = 512,
        use_gpu: bool = True
    ):
        self.storage_path = Path(storage_path)
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        self.embedding_dim = embedding_dim
        self.device = torch.device("cuda" if torch.cuda.is_available() and use_gpu else "cpu")
        
        # HDF5 file for persistent storage
        self.hdf5_path = self.storage_path / "signatures.h5"
        
        # FAISS index for fast similarity search
        self.index = None
        self.signatures = []  # List of CollapseSignature objects
        
        # Load existing signatures
        self._load_signatures()
        
        logger.info(f"SignatureLibrary initialized with {len(self.signatures)} signatures")
    
    def add_signature(
        self,
        dataset_id: str,
        dimension_scores: Dict[str, float],
        collapse_score: float,
        data_statistics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Add new collapse pattern to library.
        
        Args:
            dataset_id: Dataset identifier
            dimension_scores: Scores from collapse detector
            collapse_score: Overall collapse score
            data_statistics: Statistical features of dataset
            metadata: Additional metadata
        
        Returns:
            signature_id
        """
        # Create signature vector
        vector = self._create_signature_vector(dimension_scores, data_statistics)
        
        # Generate signature ID
        signature_id = self._generate_signature_id(dataset_id, vector)
        
        # Create signature object
        signature = CollapseSignature(
            signature_id=signature_id,
            vector=vector,
            dataset_id=dataset_id,
            collapse_score=collapse_score,
            dimension_scores=dimension_scores,
            metadata=metadata or {},
            timestamp=self._get_timestamp()
        )
        
        # Add to in-memory list
        self.signatures.append(signature)
        
        # Add to FAISS index
        self._add_to_index(vector)
        
        # Persist to disk
        self._save_signature(signature)
        
        logger.info(f"Added signature {signature_id} for dataset {dataset_id}")
        
        return signature_id
    
    async def find_similar_patterns(
        self,
        dimension_scores: Dict[str, float],
        data_statistics: Dict[str, Any],
        top_k: int = 5,
        similarity_threshold: float = 0.7
    ) -> List[MatchResult]:
        """
        Find similar collapse patterns in library.
        
        Args:
            dimension_scores: Current dimension scores
            data_statistics: Current dataset statistics
            top_k: Number of similar patterns to return
            similarity_threshold: Minimum similarity (0-1)
        
        Returns:
            List of MatchResult objects
        """
        if len(self.signatures) == 0:
            logger.warning("No signatures in library")
            return []
        
        # Create query vector
        query_vector = self._create_signature_vector(dimension_scores, data_statistics)
        
        # Search for similar vectors
        similarities, indices = self._search_index(query_vector, top_k)
        
        # Create match results
        matches = []
        for sim, idx in zip(similarities, indices):
            if sim >= similarity_threshold and idx < len(self.signatures):
                signature = self.signatures[idx]
                
                # Compute confidence based on similarity and historical accuracy
                confidence = self._compute_match_confidence(sim, signature)
                
                # Generate recommendations based on historical pattern
                recommendations = self._generate_pattern_recommendations(signature)
                
                match = MatchResult(
                    signature_id=signature.signature_id,
                    similarity=sim,
                    dataset_id=signature.dataset_id,
                    collapse_score=signature.collapse_score,
                    confidence=confidence,
                    recommendations=recommendations
                )
                matches.append(match)
        
        return matches
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get library statistics"""
        if len(self.signatures) == 0:
            return {
                'total_signatures': 0,
                'avg_collapse_score': 0.0,
                'collapse_rate': 0.0
            }
        
        collapse_scores = [sig.collapse_score for sig in self.signatures]
        collapsed_count = sum(1 for score in collapse_scores if score < 65)
        
        return {
            'total_signatures': len(self.signatures),
            'avg_collapse_score': np.mean(collapse_scores),
            'collapse_rate': collapsed_count / len(self.signatures),
            'min_score': np.min(collapse_scores),
            'max_score': np.max(collapse_scores)
        }
    
    # ==================== SIGNATURE CREATION ====================
    
    def _create_signature_vector(
        self,
        dimension_scores: Dict[str, float],
        data_statistics: Dict[str, Any]
    ) -> np.ndarray:
        """
        Create embedding vector from dimension scores and statistics.
        
        Vector structure (512-dim):
        - [0:8]: Dimension scores (normalized)
        - [8:16]: Dimension score gradients (rate of change)
        - [16:64]: Statistical features (means, stds, correlations)
        - [64:512]: Learned embedding (PCA/autoencoder of full stats)
        """
        vector = np.zeros(self.embedding_dim, dtype=np.float32)
        
        # Part 1: Dimension scores (8 dimensions)
        dim_names = [
            'distribution_fidelity', 'correlation_preservation', 'entropy_stability',
            'gradient_health', 'loss_landscape', 'spectral_coherence',
            'generalization_gap', 'statistical_consistency'
        ]
        
        for i, dim_name in enumerate(dim_names):
            score = dimension_scores.get(dim_name, 0.0)
            vector[i] = score / 100.0  # Normalize to [0, 1]
        
        # Part 2: Statistical features
        stats_start = 16
        
        # Extract key statistics
        if 'mean' in data_statistics:
            vector[stats_start:stats_start+16] = np.array(data_statistics['mean'][:16])
        
        if 'std' in data_statistics:
            vector[stats_start+16:stats_start+32] = np.array(data_statistics['std'][:16])
        
        # Part 3: Random projection for remaining dimensions (placeholder)
        # In production, use learned embeddings or PCA
        remaining_start = 64
        if remaining_start < self.embedding_dim:
            # Fill with hash of dataset features for determinism
            hash_val = hash(str(data_statistics))
            rng = np.random.RandomState(hash_val % (2**32))
            vector[remaining_start:] = rng.randn(self.embedding_dim - remaining_start) * 0.1
        
        # Normalize vector
        norm = np.linalg.norm(vector)
        if norm > 0:
            vector = vector / norm
        
        return vector
    
    def _generate_signature_id(self, dataset_id: str, vector: np.ndarray) -> str:
        """Generate unique signature ID"""
        # Hash of dataset ID + vector
        hash_input = f"{dataset_id}_{vector.tobytes().hex()[:32]}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    # ==================== FAISS INDEX ====================
    
    def _init_index(self):
        """Initialize FAISS index"""
        if not FAISS_AVAILABLE:
            return
        
        if self.device.type == 'cuda' and faiss.get_num_gpus() > 0:
            # GPU index
            res = faiss.StandardGpuResources()
            index_flat = faiss.IndexFlatIP(self.embedding_dim)  # Inner product (cosine similarity)
            self.index = faiss.index_cpu_to_gpu(res, 0, index_flat)
            logger.info("Initialized GPU FAISS index")
        else:
            # CPU index
            self.index = faiss.IndexFlatIP(self.embedding_dim)
            logger.info("Initialized CPU FAISS index")
    
    def _add_to_index(self, vector: np.ndarray):
        """Add vector to FAISS index"""
        if not FAISS_AVAILABLE:
            return
        
        if self.index is None:
            self._init_index()
        
        # FAISS expects (n, d) shape
        vector_2d = vector.reshape(1, -1).astype('float32')
        self.index.add(vector_2d)
    
    def _search_index(self, query_vector: np.ndarray, top_k: int) -> Tuple[np.ndarray, np.ndarray]:
        """Search FAISS index for similar vectors"""
        if not FAISS_AVAILABLE or self.index is None:
            # Fallback to brute-force search
            return self._brute_force_search(query_vector, top_k)
        
        # FAISS expects (n, d) shape
        query_2d = query_vector.reshape(1, -1).astype('float32')
        
        # Search (returns distances and indices)
        k = min(top_k, len(self.signatures))
        distances, indices = self.index.search(query_2d, k)
        
        # Convert inner product to cosine similarity
        # (already normalized vectors, so inner product = cosine similarity)
        similarities = distances[0]
        indices = indices[0]
        
        return similarities, indices
    
    def _brute_force_search(self, query_vector: np.ndarray, top_k: int) -> Tuple[np.ndarray, np.ndarray]:
        """Brute-force similarity search (fallback)"""
        similarities = []
        
        for signature in self.signatures:
            # Cosine similarity
            sim = np.dot(query_vector, signature.vector)
            similarities.append(sim)
        
        similarities = np.array(similarities)
        
        # Get top-k indices
        k = min(top_k, len(similarities))
        top_indices = np.argpartition(similarities, -k)[-k:]
        top_indices = top_indices[np.argsort(similarities[top_indices])][::-1]
        
        top_similarities = similarities[top_indices]
        
        return top_similarities, top_indices
    
    # ==================== PERSISTENCE ====================
    
    def _save_signature(self, signature: CollapseSignature):
        """Save signature to HDF5 file"""
        with h5py.File(self.hdf5_path, 'a') as f:
            # Create group for this signature
            if signature.signature_id in f:
                del f[signature.signature_id]  # Overwrite if exists
            
            grp = f.create_group(signature.signature_id)
            
            # Save data
            grp.create_dataset('vector', data=signature.vector)
            grp.attrs['dataset_id'] = signature.dataset_id
            grp.attrs['collapse_score'] = signature.collapse_score
            grp.attrs['timestamp'] = signature.timestamp
            
            # Save dimension scores as JSON
            grp.attrs['dimension_scores'] = json.dumps(signature.dimension_scores)
            grp.attrs['metadata'] = json.dumps(signature.metadata)
    
    def _load_signatures(self):
        """Load signatures from HDF5 file"""
        if not self.hdf5_path.exists():
            logger.info("No existing signatures found")
            return
        
        try:
            with h5py.File(self.hdf5_path, 'r') as f:
                for signature_id in f.keys():
                    grp = f[signature_id]
                    
                    # Load data
                    vector = grp['vector'][:]
                    dataset_id = grp.attrs['dataset_id']
                    collapse_score = grp.attrs['collapse_score']
                    timestamp = grp.attrs['timestamp']
                    dimension_scores = json.loads(grp.attrs['dimension_scores'])
                    metadata = json.loads(grp.attrs['metadata'])
                    
                    # Create signature object
                    signature = CollapseSignature(
                        signature_id=signature_id,
                        vector=vector,
                        dataset_id=dataset_id,
                        collapse_score=collapse_score,
                        dimension_scores=dimension_scores,
                        metadata=metadata,
                        timestamp=timestamp
                    )
                    
                    self.signatures.append(signature)
                    
                    # Add to index
                    self._add_to_index(vector)
            
            logger.info(f"Loaded {len(self.signatures)} signatures from disk")
        
        except Exception as e:
            logger.error(f"Error loading signatures: {e}")
    
    # ==================== RECOMMENDATIONS ====================
    
    def _compute_match_confidence(self, similarity: float, signature: CollapseSignature) -> float:
        """Compute confidence in pattern match"""
        # Factors: similarity score, historical collapse rate
        
        # Base confidence from similarity
        base_confidence = similarity * 100
        
        # Adjust based on historical accuracy (placeholder)
        # In production, track prediction accuracy for each signature
        historical_accuracy = 0.85  # Default 85%
        
        confidence = base_confidence * historical_accuracy
        
        return min(100, confidence)
    
    def _generate_pattern_recommendations(self, signature: CollapseSignature) -> List[str]:
        """Generate recommendations based on historical pattern"""
        recommendations = []
        
        # Check which dimensions failed in historical pattern
        for dim_name, score in signature.dimension_scores.items():
            if score < 65:
                recommendations.append(
                    f"⚠️ Historical pattern shows {dim_name} failure. "
                    f"Review this dimension carefully."
                )
        
        # Overall recommendation
        if signature.collapse_score < 50:
            recommendations.append(
                "❌ Historical pattern indicates high collapse risk. "
                "Consider regenerating dataset with different parameters."
            )
        elif signature.collapse_score < 65:
            recommendations.append(
                "⚠️ Historical pattern shows borderline quality. "
                "Proceed with caution and monitor closely."
            )
        
        # Add dataset-specific recommendations from metadata
        if 'recommendations' in signature.metadata:
            recommendations.extend(signature.metadata['recommendations'])
        
        return recommendations[:5]  # Limit to top 5
