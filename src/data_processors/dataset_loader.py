"""
Dataset Loader - Handles all major dataset formats
Supports: CSV, JSON, Parquet, HDF5, Arrow, Feather, Excel
Optimized for large datasets with streaming support
"""

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import h5py
import json
import logging
from pathlib import Path
from typing import Union, Iterator, Dict, Any, Optional, List
from dataclasses import dataclass
import numpy as np
from enum import Enum

logger = logging.getLogger(__name__)


class DatasetFormat(Enum):
    """Supported dataset formats"""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"
    HDF5 = "hdf5"
    ARROW = "arrow"
    FEATHER = "feather"
    EXCEL = "excel"
    TSV = "tsv"


@dataclass
class DatasetMetadata:
    """Metadata about loaded dataset"""
    format: DatasetFormat
    total_rows: int
    total_columns: int
    column_names: List[str]
    column_types: Dict[str, str]
    file_size_mb: float
    estimated_memory_mb: float
    has_nulls: bool
    null_counts: Dict[str, int]


class DatasetLoader:
    """
    Universal dataset loader with support for all major formats.
    Optimized for streaming large datasets that don't fit in memory.
    """
    
    def __init__(self, chunk_size: int = 100000):
        """
        Initialize dataset loader.
        
        Args:
            chunk_size: Number of rows to load per chunk for streaming
        """
        self.chunk_size = chunk_size
        self.supported_formats = {
            '.csv': DatasetFormat.CSV,
            '.json': DatasetFormat.JSON,
            '.jsonl': DatasetFormat.JSONL,
            '.parquet': DatasetFormat.PARQUET,
            '.h5': DatasetFormat.HDF5,
            '.hdf5': DatasetFormat.HDF5,
            '.arrow': DatasetFormat.ARROW,
            '.feather': DatasetFormat.FEATHER,
            '.xlsx': DatasetFormat.EXCEL,
            '.xls': DatasetFormat.EXCEL,
            '.tsv': DatasetFormat.TSV,
        }
    
    def detect_format(self, file_path: Union[str, Path]) -> DatasetFormat:
        """Detect dataset format from file extension"""
        file_path = Path(file_path)
        extension = file_path.suffix.lower()
        
        if extension not in self.supported_formats:
            raise ValueError(
                f"Unsupported format: {extension}. "
                f"Supported: {list(self.supported_formats.keys())}"
            )
        
        return self.supported_formats[extension]
    
    def get_metadata(self, file_path: Union[str, Path]) -> DatasetMetadata:
        """
        Get dataset metadata without loading full dataset.
        Fast preview of dataset characteristics.
        """
        file_path = Path(file_path)
        format_type = self.detect_format(file_path)
        
        logger.info(f"Analyzing metadata for {file_path.name} ({format_type.value})")
        
        if format_type == DatasetFormat.PARQUET:
            return self._get_parquet_metadata(file_path)
        elif format_type == DatasetFormat.CSV:
            return self._get_csv_metadata(file_path)
        elif format_type == DatasetFormat.HDF5:
            return self._get_hdf5_metadata(file_path)
        elif format_type in [DatasetFormat.JSON, DatasetFormat.JSONL]:
            return self._get_json_metadata(file_path, format_type)
        else:
            # Generic fallback - load small sample
            return self._get_generic_metadata(file_path, format_type)
    
    def load_full(self, file_path: Union[str, Path]) -> pd.DataFrame:
        """
        Load entire dataset into memory.
        Use only for datasets that fit in RAM (<10GB typically).
        """
        file_path = Path(file_path)
        format_type = self.detect_format(file_path)
        
        logger.info(f"Loading full dataset: {file_path.name}")
        
        loaders = {
            DatasetFormat.CSV: lambda: pd.read_csv(file_path),
            DatasetFormat.TSV: lambda: pd.read_csv(file_path, sep='\t'),
            DatasetFormat.JSON: lambda: pd.read_json(file_path),
            DatasetFormat.JSONL: lambda: pd.read_json(file_path, lines=True),
            DatasetFormat.PARQUET: lambda: pd.read_parquet(file_path),
            DatasetFormat.HDF5: lambda: self._load_hdf5(file_path),
            DatasetFormat.ARROW: lambda: pa.ipc.open_file(file_path).read_pandas(),
            DatasetFormat.FEATHER: lambda: pd.read_feather(file_path),
            DatasetFormat.EXCEL: lambda: pd.read_excel(file_path),
        }
        
        try:
            df = loaders[format_type]()
            logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            raise
    
    def stream_chunks(
        self, 
        file_path: Union[str, Path],
        chunk_size: Optional[int] = None
    ) -> Iterator[pd.DataFrame]:
        """
        Stream dataset in chunks for memory-efficient processing.
        Essential for large datasets (>10GB).
        
        Yields:
            DataFrame chunks of specified size
        """
        file_path = Path(file_path)
        format_type = self.detect_format(file_path)
        chunk_size = chunk_size or self.chunk_size
        
        logger.info(f"Streaming {file_path.name} in chunks of {chunk_size:,} rows")
        
        if format_type == DatasetFormat.CSV:
            yield from pd.read_csv(file_path, chunksize=chunk_size)
        
        elif format_type == DatasetFormat.TSV:
            yield from pd.read_csv(file_path, sep='\t', chunksize=chunk_size)
        
        elif format_type == DatasetFormat.JSONL:
            yield from pd.read_json(file_path, lines=True, chunksize=chunk_size)
        
        elif format_type == DatasetFormat.PARQUET:
            yield from self._stream_parquet(file_path, chunk_size)
        
        elif format_type == DatasetFormat.HDF5:
            yield from self._stream_hdf5(file_path, chunk_size)
        
        else:
            # Fallback: load full and chunk manually
            df = self.load_full(file_path)
            for i in range(0, len(df), chunk_size):
                yield df.iloc[i:i+chunk_size]
    
    def _get_parquet_metadata(self, file_path: Path) -> DatasetMetadata:
        """Fast metadata extraction from Parquet files"""
        parquet_file = pq.ParquetFile(file_path)
        schema = parquet_file.schema_arrow
        
        total_rows = parquet_file.metadata.num_rows
        column_names = schema.names
        column_types = {name: str(schema.field(name).type) for name in column_names}
        
        # Estimate memory usage
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        estimated_memory_mb = file_size_mb * 1.5  # Rough estimate
        
        return DatasetMetadata(
            format=DatasetFormat.PARQUET,
            total_rows=total_rows,
            total_columns=len(column_names),
            column_names=column_names,
            column_types=column_types,
            file_size_mb=file_size_mb,
            estimated_memory_mb=estimated_memory_mb,
            has_nulls=True,  # Would need to scan to determine
            null_counts={}
        )
    
    def _get_csv_metadata(self, file_path: Path) -> DatasetMetadata:
        """Fast metadata extraction from CSV files"""
        # Read first chunk to get column info
        first_chunk = pd.read_csv(file_path, nrows=1000)
        
        # Count total rows (fast line count)
        with open(file_path, 'r') as f:
            total_rows = sum(1 for _ in f) - 1  # Subtract header
        
        column_names = first_chunk.columns.tolist()
        column_types = {col: str(dtype) for col, dtype in first_chunk.dtypes.items()}
        
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        estimated_memory_mb = file_size_mb * 2.0  # CSV expands in memory
        
        null_counts = first_chunk.isnull().sum().to_dict()
        
        return DatasetMetadata(
            format=DatasetFormat.CSV,
            total_rows=total_rows,
            total_columns=len(column_names),
            column_names=column_names,
            column_types=column_types,
            file_size_mb=file_size_mb,
            estimated_memory_mb=estimated_memory_mb,
            has_nulls=any(count > 0 for count in null_counts.values()),
            null_counts=null_counts
        )
    
    def _get_hdf5_metadata(self, file_path: Path) -> DatasetMetadata:
        """Fast metadata extraction from HDF5 files"""
        with h5py.File(file_path, 'r') as f:
            # Assume main dataset is at root or first key
            dataset_key = list(f.keys())[0]
            dataset = f[dataset_key]
            
            total_rows = dataset.shape[0]
            total_columns = dataset.shape[1] if len(dataset.shape) > 1 else 1
            
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            estimated_memory_mb = dataset.nbytes / (1024 * 1024)
            
            return DatasetMetadata(
                format=DatasetFormat.HDF5,
                total_rows=total_rows,
                total_columns=total_columns,
                column_names=[f"col_{i}" for i in range(total_columns)],
                column_types={"all": str(dataset.dtype)},
                file_size_mb=file_size_mb,
                estimated_memory_mb=estimated_memory_mb,
                has_nulls=False,
                null_counts={}
            )
    
    def _get_json_metadata(
        self, 
        file_path: Path, 
        format_type: DatasetFormat
    ) -> DatasetMetadata:
        """Metadata extraction from JSON/JSONL files"""
        if format_type == DatasetFormat.JSONL:
            # Count lines for JSONL
            with open(file_path, 'r') as f:
                total_rows = sum(1 for _ in f)
            
            # Read first few lines to get schema
            sample = pd.read_json(file_path, lines=True, nrows=100)
        else:
            # Regular JSON
            sample = pd.read_json(file_path, nrows=100)
            total_rows = len(sample)  # Approximate
        
        column_names = sample.columns.tolist()
        column_types = {col: str(dtype) for col, dtype in sample.dtypes.items()}
        
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        estimated_memory_mb = file_size_mb * 3.0  # JSON is verbose
        
        return DatasetMetadata(
            format=format_type,
            total_rows=total_rows,
            total_columns=len(column_names),
            column_names=column_names,
            column_types=column_types,
            file_size_mb=file_size_mb,
            estimated_memory_mb=estimated_memory_mb,
            has_nulls=sample.isnull().any().any(),
            null_counts=sample.isnull().sum().to_dict()
        )
    
    def _get_generic_metadata(
        self, 
        file_path: Path, 
        format_type: DatasetFormat
    ) -> DatasetMetadata:
        """Fallback metadata extraction"""
        df = self.load_full(file_path)
        
        return DatasetMetadata(
            format=format_type,
            total_rows=len(df),
            total_columns=len(df.columns),
            column_names=df.columns.tolist(),
            column_types={col: str(dtype) for col, dtype in df.dtypes.items()},
            file_size_mb=file_path.stat().st_size / (1024 * 1024),
            estimated_memory_mb=df.memory_usage(deep=True).sum() / (1024 * 1024),
            has_nulls=df.isnull().any().any(),
            null_counts=df.isnull().sum().to_dict()
        )
    
    def _load_hdf5(self, file_path: Path) -> pd.DataFrame:
        """Load HDF5 file into DataFrame"""
        with h5py.File(file_path, 'r') as f:
            dataset_key = list(f.keys())[0]
            data = f[dataset_key][:]
            return pd.DataFrame(data)
    
    def _stream_parquet(
        self, 
        file_path: Path, 
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """Stream Parquet file in chunks"""
        parquet_file = pq.ParquetFile(file_path)
        
        for batch in parquet_file.iter_batches(batch_size=chunk_size):
            yield batch.to_pandas()
    
    def _stream_hdf5(
        self, 
        file_path: Path, 
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """Stream HDF5 file in chunks"""
        with h5py.File(file_path, 'r') as f:
            dataset_key = list(f.keys())[0]
            dataset = f[dataset_key]
            
            total_rows = dataset.shape[0]
            for i in range(0, total_rows, chunk_size):
                chunk_data = dataset[i:i+chunk_size]
                yield pd.DataFrame(chunk_data)


# Utility functions for quick access
def load_dataset(file_path: Union[str, Path]) -> pd.DataFrame:
    """Quick load dataset - convenience function"""
    loader = DatasetLoader()
    return loader.load_full(file_path)


def get_dataset_info(file_path: Union[str, Path]) -> DatasetMetadata:
    """Quick get dataset metadata - convenience function"""
    loader = DatasetLoader()
    return loader.get_metadata(file_path)


def stream_dataset(
    file_path: Union[str, Path], 
    chunk_size: int = 100000
) -> Iterator[pd.DataFrame]:
    """Quick stream dataset - convenience function"""
    loader = DatasetLoader(chunk_size=chunk_size)
    return loader.stream_chunks(file_path)
