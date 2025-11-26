import pytest
import pandas as pd
import numpy as np
import os
import tempfile
from pathlib import Path
from src.data_processors.dataset_loader import DatasetLoader, DatasetFormat, DatasetMetadata

@pytest.fixture
def sample_df():
    """Create a sample DataFrame"""
    return pd.DataFrame({
        'col1': range(100),
        'col2': np.random.randn(100),
        'col3': ['A'] * 50 + ['B'] * 50
    })

@pytest.fixture
def temp_csv(sample_df):
    """Create a temporary CSV file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_df.to_csv(f.name, index=False)
        yield f.name
    os.unlink(f.name)

@pytest.fixture
def temp_parquet(sample_df):
    """Create a temporary Parquet file"""
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        sample_df.to_parquet(f.name, index=False)
        yield f.name
    os.unlink(f.name)

@pytest.fixture
def temp_json(sample_df):
    """Create a temporary JSON file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        sample_df.to_json(f.name, orient='records')
        yield f.name
    os.unlink(f.name)

class TestDatasetLoader:
    
    def test_detect_format(self):
        loader = DatasetLoader()
        assert loader.detect_format("data.csv") == DatasetFormat.CSV
        assert loader.detect_format("data.parquet") == DatasetFormat.PARQUET
        assert loader.detect_format("data.json") == DatasetFormat.JSON
        
        with pytest.raises(ValueError):
            loader.detect_format("data.unknown")

    def test_get_metadata_csv(self, temp_csv):
        loader = DatasetLoader()
        metadata = loader.get_metadata(temp_csv)
        
        assert isinstance(metadata, DatasetMetadata)
        assert metadata.format == DatasetFormat.CSV
        assert metadata.estimated_rows == 100
        assert metadata.estimated_columns == 3
        assert 'col1' in metadata.column_names

    def test_get_metadata_parquet(self, temp_parquet):
        loader = DatasetLoader()
        metadata = loader.get_metadata(temp_parquet)
        
        assert isinstance(metadata, DatasetMetadata)
        assert metadata.format == DatasetFormat.PARQUET
        assert metadata.estimated_rows == 100
        assert metadata.estimated_columns == 3

    def test_load_full_csv(self, temp_csv):
        loader = DatasetLoader()
        df = loader.load_full(temp_csv)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100
        assert len(df.columns) == 3

    def test_load_full_parquet(self, temp_parquet):
        loader = DatasetLoader()
        df = loader.load_full(temp_parquet)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100

    def test_stream_chunks_csv(self, temp_csv):
        loader = DatasetLoader(chunk_size=20)
        chunks = list(loader.stream_chunks(temp_csv))
        
        assert len(chunks) == 5  # 100 rows / 20 per chunk
        assert len(chunks[0]) == 20
        assert pd.concat(chunks).shape == (100, 3)

    def test_stream_chunks_parquet(self, temp_parquet):
        loader = DatasetLoader(chunk_size=20)
        chunks = list(loader.stream_chunks(temp_parquet))
        
        # Parquet chunking depends on row groups, so exact chunk count might vary
        # but total rows should match
        full_df = pd.concat(chunks)
        assert len(full_df) == 100
        assert full_df.shape[1] == 3

    @pytest.mark.asyncio
    async def test_load_dataset_async(self, temp_csv):
        loader = DatasetLoader()
        df = await loader.load_dataset(temp_csv, 'csv')
        
        assert len(df) == 100
