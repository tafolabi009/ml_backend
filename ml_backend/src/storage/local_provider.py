"""
Local Filesystem Storage Provider Implementation
Useful for development, testing, and on-premise deployments
"""

import os
import shutil
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
import logging

from .storage_provider import (
    StorageProvider,
    StorageMetadata,
    StorageError,
    StorageNotFoundError,
)

logger = logging.getLogger(__name__)


class LocalProvider(StorageProvider):
    """
    Local filesystem implementation.
    
    Treats a local directory as the "bucket" and provides the same interface
    as cloud storage providers. Useful for:
    - Local development
    - Testing
    - On-premise deployments
    - Air-gapped environments
    """
    
    def __init__(self, base_path: str):
        """
        Initialize local filesystem provider.
        
        Args:
            base_path: Base directory path (treated as "bucket")
        """
        self.base_path = Path(base_path).resolve()
        
        # Create base directory if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Local provider initialized at: {self.base_path}")
    
    def _get_full_path(self, remote_path: str) -> Path:
        """Convert remote path to full local path"""
        # Remove leading slashes to prevent absolute path issues
        remote_path = remote_path.lstrip('/')
        full_path = self.base_path / remote_path
        
        # Security check: ensure path is within base_path
        try:
            full_path.resolve().relative_to(self.base_path.resolve())
        except ValueError:
            raise StorageError(f"Path traversal not allowed: {remote_path}")
        
        return full_path
    
    def upload(
        self,
        local_path: str,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Copy file to local storage"""
        try:
            if not os.path.exists(local_path):
                raise StorageError(f"Local file not found: {local_path}")
            
            dest_path = self._get_full_path(remote_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(local_path, dest_path)
            
            # Store metadata in sidecar file if provided
            if metadata:
                self._save_metadata(remote_path, content_type, metadata)
            
            uri = self.get_uri(remote_path)
            logger.info(f"Uploaded {local_path} to {uri}")
            return uri
            
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"Upload failed: {e}")
    
    def upload_bytes(
        self,
        data: bytes,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Write bytes to local storage"""
        try:
            dest_path = self._get_full_path(remote_path)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            
            dest_path.write_bytes(data)
            
            # Store metadata in sidecar file if provided
            if metadata:
                self._save_metadata(remote_path, content_type, metadata)
            
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
        """Copy file from local storage"""
        try:
            src_path = self._get_full_path(remote_path)
            
            if not src_path.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            # Create parent directories for destination
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(src_path, local_path)
            
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
        """Read file as bytes from local storage"""
        try:
            src_path = self._get_full_path(remote_path)
            
            if not src_path.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            data = src_path.read_bytes()
            logger.info(f"Downloaded {remote_path} ({len(data)} bytes)")
            return data
            
        except StorageNotFoundError:
            raise
        except Exception as e:
            raise StorageError(f"Download bytes failed: {e}")
    
    def exists(self, remote_path: str) -> bool:
        """Check if file exists in local storage"""
        try:
            full_path = self._get_full_path(remote_path)
            return full_path.exists() and full_path.is_file()
        except Exception as e:
            logger.error(f"Error checking existence of {remote_path}: {e}")
            return False
    
    def delete(self, remote_path: str) -> bool:
        """Delete file from local storage"""
        try:
            full_path = self._get_full_path(remote_path)
            
            if not full_path.exists():
                return False
            
            full_path.unlink()
            
            # Also delete metadata sidecar if it exists
            metadata_path = full_path.with_suffix(full_path.suffix + '.meta')
            if metadata_path.exists():
                metadata_path.unlink()
            
            logger.info(f"Deleted {remote_path}")
            return True
            
        except Exception as e:
            raise StorageError(f"Delete failed: {e}")
    
    def list(
        self,
        prefix: str = "",
        max_results: Optional[int] = None
    ) -> List[str]:
        """List files in local storage with prefix"""
        try:
            # Remove leading slashes
            prefix = prefix.lstrip('/')
            
            if prefix:
                search_path = self.base_path / prefix
            else:
                search_path = self.base_path
            
            paths = []
            
            if search_path.exists():
                # Recursively find all files
                for file_path in search_path.rglob('*'):
                    if file_path.is_file() and not file_path.name.endswith('.meta'):
                        # Get relative path from base_path
                        relative = file_path.relative_to(self.base_path)
                        paths.append(str(relative))
                        
                        if max_results and len(paths) >= max_results:
                            break
            
            logger.info(f"Listed {len(paths)} files with prefix '{prefix}'")
            return sorted(paths)
            
        except Exception as e:
            raise StorageError(f"List failed: {e}")
    
    def get_metadata(self, remote_path: str) -> StorageMetadata:
        """Get metadata for local file"""
        try:
            full_path = self._get_full_path(remote_path)
            
            if not full_path.exists():
                raise StorageNotFoundError(f"File not found: {remote_path}")
            
            stat = full_path.stat()
            
            # Try to load custom metadata from sidecar file
            custom_metadata = self._load_metadata(remote_path)
            
            # Guess content type from extension
            import mimetypes
            content_type, _ = mimetypes.guess_type(str(full_path))
            content_type = content_type or 'application/octet-stream'
            
            return StorageMetadata(
                path=remote_path,
                size_bytes=stat.st_size,
                content_type=content_type,
                created_at=datetime.fromtimestamp(stat.st_ctime),
                modified_at=datetime.fromtimestamp(stat.st_mtime),
                etag=None,  # No etag for local files
                custom_metadata=custom_metadata
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
        """
        Generate file:// URL for local file.
        Note: expiration is not enforced for local files.
        """
        full_path = self._get_full_path(remote_path)
        
        if not full_path.exists():
            raise StorageNotFoundError(f"File not found: {remote_path}")
        
        # Return file:// URL
        url = full_path.as_uri()
        logger.info(f"Generated file URL for {remote_path}")
        return url
    
    def get_uri(self, remote_path: str) -> str:
        """Get file:// URI"""
        full_path = self._get_full_path(remote_path)
        return full_path.as_uri()
    
    def _save_metadata(
        self,
        remote_path: str,
        content_type: Optional[str],
        metadata: Dict[str, str]
    ):
        """Save metadata to sidecar JSON file"""
        try:
            import json
            
            full_path = self._get_full_path(remote_path)
            metadata_path = full_path.with_suffix(full_path.suffix + '.meta')
            
            meta_data = {
                'content_type': content_type,
                'metadata': metadata,
                'created_at': datetime.now().isoformat()
            }
            
            metadata_path.write_text(json.dumps(meta_data, indent=2))
            
        except Exception as e:
            logger.warning(f"Failed to save metadata for {remote_path}: {e}")
    
    def _load_metadata(self, remote_path: str) -> Optional[Dict[str, str]]:
        """Load metadata from sidecar JSON file"""
        try:
            import json
            
            full_path = self._get_full_path(remote_path)
            metadata_path = full_path.with_suffix(full_path.suffix + '.meta')
            
            if metadata_path.exists():
                meta_data = json.loads(metadata_path.read_text())
                return meta_data.get('metadata', {})
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to load metadata for {remote_path}: {e}")
            return None
