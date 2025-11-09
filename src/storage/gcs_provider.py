"""
Google Cloud Storage Provider Implementation
"""

import os
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime, timedelta
import logging

from .storage_provider import (
    StorageProvider,
    StorageMetadata,
    StorageError,
    StorageConnectionError,
    StorageNotFoundError,
    StoragePermissionError,
    StorageQuotaError
)

logger = logging.getLogger(__name__)


class GCSProvider(StorageProvider):
    """
    Google Cloud Storage implementation.
    
    Requires: google-cloud-storage library
    
    Authentication:
    - Service account JSON key file (set GOOGLE_APPLICATION_CREDENTIALS)
    - Default application credentials
    - Workload Identity (GKE)
    """
    
    def __init__(
        self,
        bucket_name: str,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None
    ):
        """
        Initialize GCS provider.
        
        Args:
            bucket_name: GCS bucket name
            project_id: GCP project ID (optional if using default credentials)
            credentials_path: Path to service account JSON key file
        """
        self.bucket_name = bucket_name
        self.project_id = project_id
        
        try:
            from google.cloud import storage
            from google.oauth2 import service_account
            
            # Initialize client
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path
                )
                self.client = storage.Client(
                    project=project_id,
                    credentials=credentials
                )
            else:
                self.client = storage.Client(project=project_id)
            
            self.bucket = self.client.bucket(bucket_name)
            
            # Verify bucket exists
            if not self.bucket.exists():
                raise StorageError(f"Bucket '{bucket_name}' does not exist")
            
            logger.info(f"GCS provider initialized for bucket: {bucket_name}")
            
        except ImportError:
            raise StorageConnectionError(
                "google-cloud-storage library not installed. "
                "Install with: pip install google-cloud-storage"
            )
        except Exception as e:
            raise StorageConnectionError(f"Failed to initialize GCS client: {e}")
    
    def upload(
        self,
        local_path: str,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            
            if metadata:
                blob.metadata = metadata
            
            blob.upload_from_filename(
                local_path,
                content_type=content_type
            )
            
            uri = self.get_uri(remote_path)
            logger.info(f"Uploaded {local_path} to {uri}")
            return uri
            
        except FileNotFoundError:
            raise StorageError(f"Local file not found: {local_path}")
        except Exception as e:
            raise StorageError(f"Upload failed: {e}")
    
    def upload_bytes(
        self,
        data: bytes,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload bytes to GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            
            if metadata:
                blob.metadata = metadata
            
            blob.upload_from_string(
                data,
                content_type=content_type
            )
            
            uri = self.get_uri(remote_path)
            logger.info(f"Uploaded {len(data)} bytes to {uri}")
            return uri
            
        except Exception as e:
            raise StorageError(f"Upload bytes failed: {e}")
    
    def download(
        self,
        remote_path: str,
        local_path: str
    ) -> str:
        """Download file from GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            
            if not blob.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            # Create parent directories if needed
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            blob.download_to_filename(local_path)
            
            logger.info(f"Downloaded {remote_path} to {local_path}")
            return local_path
            
        except StorageNotFoundError:
            raise
        except Exception as e:
            raise StorageError(f"Download failed: {e}")
    
    def download_bytes(
        self,
        remote_path: str
    ) -> bytes:
        """Download file as bytes from GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            
            if not blob.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            data = blob.download_as_bytes()
            logger.info(f"Downloaded {remote_path} ({len(data)} bytes)")
            return data
            
        except StorageNotFoundError:
            raise
        except Exception as e:
            raise StorageError(f"Download bytes failed: {e}")
    
    def exists(self, remote_path: str) -> bool:
        """Check if file exists in GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking existence of {remote_path}: {e}")
            return False
    
    def delete(self, remote_path: str) -> bool:
        """Delete file from GCS"""
        try:
            blob = self.bucket.blob(remote_path)
            
            if not blob.exists():
                return False
            
            blob.delete()
            logger.info(f"Deleted {remote_path}")
            return True
            
        except Exception as e:
            raise StorageError(f"Delete failed: {e}")
    
    def list(
        self,
        prefix: str = "",
        max_results: Optional[int] = None
    ) -> List[str]:
        """List files in GCS with prefix"""
        try:
            blobs = self.client.list_blobs(
                self.bucket_name,
                prefix=prefix,
                max_results=max_results
            )
            
            paths = [blob.name for blob in blobs]
            logger.info(f"Listed {len(paths)} files with prefix '{prefix}'")
            return paths
            
        except Exception as e:
            raise StorageError(f"List failed: {e}")
    
    def get_metadata(self, remote_path: str) -> StorageMetadata:
        """Get metadata for GCS file"""
        try:
            blob = self.bucket.blob(remote_path)
            blob.reload()  # Fetch metadata
            
            if not blob.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            return StorageMetadata(
                path=remote_path,
                size_bytes=blob.size,
                content_type=blob.content_type or 'application/octet-stream',
                created_at=blob.time_created,
                modified_at=blob.updated,
                etag=blob.etag,
                custom_metadata=blob.metadata
            )
            
        except StorageNotFoundError:
            raise
        except Exception as e:
            raise StorageError(f"Get metadata failed: {e}")
    
    def generate_signed_url(
        self,
        remote_path: str,
        expiration_seconds: int = 3600
    ) -> str:
        """Generate signed URL for GCS file"""
        try:
            blob = self.bucket.blob(remote_path)
            
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=expiration_seconds),
                method="GET"
            )
            
            logger.info(f"Generated signed URL for {remote_path} (expires in {expiration_seconds}s)")
            return url
            
        except Exception as e:
            raise StorageError(f"Generate signed URL failed: {e}")
    
    def get_uri(self, remote_path: str) -> str:
        """Get GCS URI"""
        return f"gs://{self.bucket_name}/{remote_path}"
