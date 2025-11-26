import pytest
import numpy as np
import pandas as pd
from unittest.mock import MagicMock, patch, mock_open, AsyncMock
from pathlib import Path
import tempfile
import shutil
import json
import os

from src.storage.local_provider import LocalProvider
from src.storage.factory import StorageFactory


class TestLocalProvider:
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    @pytest.fixture
    def provider(self, temp_dir):
        """Create a LocalProvider instance"""
        return LocalProvider(base_path=temp_dir)
    
    def test_initialization(self, provider, temp_dir):
        """Test provider initialization"""
        assert provider.base_path == Path(temp_dir)
        assert provider.base_path.exists()
    
    def test_upload_and_download_bytes(self, provider, temp_dir):
        """Test uploading and downloading bytes"""
        data = b"test data content"
        remote_path = "test_file.bin"
        
        # Upload bytes
        uri = provider.upload_bytes(data, remote_path)
        assert uri is not None
        
        # Download bytes
        downloaded = provider.download_bytes(remote_path)
        assert downloaded == data
    
    def test_upload_and_download_file(self, provider, temp_dir):
        """Test uploading and downloading files"""
        # Create a test file
        test_content = "Hello, World!"
        local_file = Path(temp_dir) / "source.txt"
        local_file.write_text(test_content)
        
        remote_path = "test/file.txt"
        
        # Upload
        uri = provider.upload(str(local_file), remote_path)
        assert uri is not None
        
        # Download
        download_path = Path(temp_dir) / "downloaded.txt"
        result_path = provider.download(remote_path, str(download_path))
        
        assert download_path.exists()
        assert download_path.read_text() == test_content
    
    def test_exists(self, provider):
        """Test checking if file exists"""
        remote_path = "test_exists.bin"
        
        # Should not exist initially
        assert not provider.exists(remote_path)
        
        # Upload data
        provider.upload_bytes(b"data", remote_path)
        
        # Should exist now
        assert provider.exists(remote_path)
    
    def test_delete(self, provider):
        """Test deleting files"""
        remote_path = "test_delete.bin"
        
        # Upload data
        provider.upload_bytes(b"data", remote_path)
        assert provider.exists(remote_path)
        
        # Delete
        result = provider.delete(remote_path)
        assert result is True
        assert not provider.exists(remote_path)
    
    def test_delete_nonexistent(self, provider):
        """Test deleting non-existent file"""
        result = provider.delete("nonexistent.bin")
        assert result is False
    
    def test_list_files(self, provider):
        """Test listing files with prefix"""
        # Upload multiple files
        provider.upload_bytes(b"1", "dir1/file1.txt")
        provider.upload_bytes(b"2", "dir1/file2.txt")
        provider.upload_bytes(b"3", "dir2/file3.txt")
        
        # List all
        all_files = provider.list()
        assert len(all_files) >= 3
        
        # List with prefix
        dir1_files = provider.list(prefix="dir1/")
        assert len(dir1_files) == 2
        assert all('dir1' in f for f in dir1_files)
    
    def test_get_uri(self, provider):
        """Test getting file URI"""
        remote_path = "test.txt"
        uri = provider.get_uri(remote_path)
        
        # Should be a file:// URI
        assert 'file://' in uri or str(provider.base_path) in uri
    
    def test_nested_directories(self, provider):
        """Test uploading to nested directories"""
        remote_path = "deep/nested/directory/file.txt"
        data = b"nested content"
        
        # Should create all directories
        provider.upload_bytes(data, remote_path)
        assert provider.exists(remote_path)
        
        # Download back
        downloaded = provider.download_bytes(remote_path)
        assert downloaded == data
    
    def test_upload_with_metadata(self, provider):
        """Test uploading with metadata"""
        remote_path = "file_with_meta.txt"
        data = b"content"
        metadata = {"key1": "value1", "key2": "value2"}
        
        provider.upload_bytes(data, remote_path, metadata=metadata)
        
        assert provider.exists(remote_path)
    
    def test_download_nonexistent(self, provider):
        """Test downloading non-existent file raises error"""
        from src.storage.storage_provider import StorageNotFoundError
        
        with pytest.raises(StorageNotFoundError):
            provider.download_bytes("nonexistent.txt")


class TestStorageFactory:
    
    def test_create_local_provider(self):
        """Test creating local storage provider"""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = {
                'provider': 'local',
                'base_path': temp_dir
            }
            
            provider = StorageFactory.create(config)
            
            assert isinstance(provider, LocalProvider)
            assert provider.base_path == Path(temp_dir)
    
    def test_invalid_provider_type(self):
        """Test creating provider with invalid type"""
        from src.storage.storage_provider import StorageConnectionError
        
        with pytest.raises(StorageConnectionError):
            StorageFactory.create({'provider': 'invalid'})
    
    def test_missing_provider_type(self):
        """Test creating provider without type"""
        from src.storage.storage_provider import StorageConnectionError
        
        with pytest.raises(StorageConnectionError):
            StorageFactory.create({})
    
    @patch('src.storage.factory.S3Provider')
    def test_create_s3_provider_mocked(self, mock_s3_class):
        """Test S3 provider creation with mocked class"""
        mock_instance = MagicMock()
        mock_s3_class.return_value = mock_instance
        
        config = {
            'provider': 's3',
            'bucket_name': 'test-bucket',
            'region': 'us-west-2'
        }
        
        provider = StorageFactory.create(config)
        
        assert provider == mock_instance
        mock_s3_class.assert_called_once()
    
    @patch('src.storage.factory.GCSProvider')
    def test_create_gcs_provider_mocked(self, mock_gcs_class):
        """Test GCS provider creation with mocked class"""
        mock_instance = MagicMock()
        mock_gcs_class.return_value = mock_instance
        
        config = {
            'provider': 'gcs',
            'bucket_name': 'test-bucket',
            'project_id': 'test-project'
        }
        
        provider = StorageFactory.create(config)
        
        assert provider == mock_instance
        mock_gcs_class.assert_called_once()
    
    def test_from_env_local(self):
        """Test creating provider from environment variables"""
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.dict(os.environ, {
                'STORAGE_PROVIDER': 'local',
                'STORAGE_BASE_PATH': temp_dir
            }, clear=True):
                provider = StorageFactory.from_env()
                
                assert isinstance(provider, LocalProvider)
                assert provider.base_path == Path(temp_dir)

