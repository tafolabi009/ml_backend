"""
Abstract Storage Provider Interface
Supports multiple cloud storage backends (GCS, S3, Azure Blob) and local filesystem

This abstraction allows the ml_backend to work with different storage providers
without changing the core validation logic.
"""

from abc import ABC, abstractmethod
from typing import Optional, List, BinaryIO, Dict, Any
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import io


@dataclass
class StorageMetadata:
    """Metadata for stored objects"""
    path: str
    size_bytes: int
    content_type: str
    created_at: datetime
    modified_at: datetime
    etag: Optional[str] = None
    custom_metadata: Optional[Dict[str, str]] = None


class StorageProvider(ABC):
    """
    Abstract base class for storage providers.
    
    All storage providers (GCS, S3, Azure, Local) must implement these methods.
    """
    
    @abstractmethod
    def upload(
        self,
        local_path: str,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Upload a file to storage.
        
        Args:
            local_path: Path to local file to upload
            remote_path: Destination path in storage
            content_type: MIME type (e.g., 'text/csv', 'application/json')
            metadata: Custom metadata to attach to the object
            
        Returns:
            Remote path/URI of uploaded file
            
        Raises:
            StorageError: If upload fails
        """
        pass
    
    @abstractmethod
    def upload_bytes(
        self,
        data: bytes,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """
        Upload bytes directly to storage without a local file.
        
        Args:
            data: Bytes to upload
            remote_path: Destination path in storage
            content_type: MIME type
            metadata: Custom metadata
            
        Returns:
            Remote path/URI of uploaded data
            
        Raises:
            StorageError: If upload fails
        """
        pass
    
    @abstractmethod
    def download(
        self,
        remote_path: str,
        local_path: str
    ) -> str:
        """
        Download a file from storage.
        
        Args:
            remote_path: Path in storage to download
            local_path: Local destination path
            
        Returns:
            Local path where file was saved
            
        Raises:
            StorageError: If download fails or file doesn't exist
        """
        pass
    
    @abstractmethod
    def download_bytes(
        self,
        remote_path: str
    ) -> bytes:
        """
        Download file as bytes without saving to disk.
        
        Args:
            remote_path: Path in storage to download
            
        Returns:
            File contents as bytes
            
        Raises:
            StorageError: If download fails or file doesn't exist
        """
        pass
    
    @abstractmethod
    def exists(self, remote_path: str) -> bool:
        """
        Check if a file exists in storage.
        
        Args:
            remote_path: Path to check
            
        Returns:
            True if file exists, False otherwise
        """
        pass
    
    @abstractmethod
    def delete(self, remote_path: str) -> bool:
        """
        Delete a file from storage.
        
        Args:
            remote_path: Path to delete
            
        Returns:
            True if deleted successfully, False if file didn't exist
            
        Raises:
            StorageError: If deletion fails
        """
        pass
    
    @abstractmethod
    def list(
        self,
        prefix: str = "",
        max_results: Optional[int] = None
    ) -> List[str]:
        """
        List files in storage with optional prefix filter.
        
        Args:
            prefix: Filter results by prefix (folder path)
            max_results: Maximum number of results to return
            
        Returns:
            List of file paths
            
        Raises:
            StorageError: If listing fails
        """
        pass
    
    @abstractmethod
    def get_metadata(self, remote_path: str) -> StorageMetadata:
        """
        Get metadata for a stored file.
        
        Args:
            remote_path: Path to file
            
        Returns:
            Metadata object with file details
            
        Raises:
            StorageError: If file doesn't exist or metadata retrieval fails
        """
        pass
    
    @abstractmethod
    def generate_signed_url(
        self,
        remote_path: str,
        expiration_seconds: int = 3600
    ) -> str:
        """
        Generate a signed URL for temporary access to a file.
        
        Args:
            remote_path: Path to file
            expiration_seconds: How long the URL should be valid (default 1 hour)
            
        Returns:
            Signed URL string
            
        Raises:
            StorageError: If URL generation fails
        """
        pass
    
    @abstractmethod
    def get_uri(self, remote_path: str) -> str:
        """
        Get the full URI for a file in storage.
        
        Args:
            remote_path: Path to file
            
        Returns:
            Full URI (e.g., 'gs://bucket/path', 's3://bucket/path', 'file:///path')
        """
        pass


class StorageError(Exception):
    """Base exception for storage operations"""
    pass


class StorageConnectionError(StorageError):
    """Failed to connect to storage backend"""
    pass


class StorageNotFoundError(StorageError):
    """Requested file/object not found"""
    pass


class StoragePermissionError(StorageError):
    """Insufficient permissions for operation"""
    pass


class StorageQuotaError(StorageError):
    """Storage quota exceeded"""
    pass
