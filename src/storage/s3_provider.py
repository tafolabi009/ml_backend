"""
Amazon S3 Storage Provider Implementation
"""

import os
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime
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


class S3Provider(StorageProvider):
    """
    Amazon S3 implementation.
    
    Requires: boto3 library
    
    Authentication:
    - AWS credentials file (~/.aws/credentials)
    - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    - IAM role (EC2/ECS/Lambda)
    """
    
    def __init__(
        self,
        bucket_name: str,
        region: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None  # For S3-compatible services (MinIO, etc.)
    ):
        """
        Initialize S3 provider.
        
        Args:
            bucket_name: S3 bucket name
            region: AWS region (e.g., 'us-east-1')
            aws_access_key_id: AWS access key (optional)
            aws_secret_access_key: AWS secret key (optional)
            endpoint_url: Custom endpoint for S3-compatible storage
        """
        self.bucket_name = bucket_name
        self.region = region
        
        try:
            import boto3
            from botocore.exceptions import ClientError
            
            # Initialize client
            session_kwargs = {}
            if aws_access_key_id and aws_secret_access_key:
                session_kwargs['aws_access_key_id'] = aws_access_key_id
                session_kwargs['aws_secret_access_key'] = aws_secret_access_key
            if region:
                session_kwargs['region_name'] = region
            
            session = boto3.Session(**session_kwargs)
            
            client_kwargs = {}
            if endpoint_url:
                client_kwargs['endpoint_url'] = endpoint_url
            
            self.s3_client = session.client('s3', **client_kwargs)
            self.s3_resource = session.resource('s3', **client_kwargs)
            self.bucket = self.s3_resource.Bucket(bucket_name)
            
            # Verify bucket exists
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == '404':
                    raise StorageError(f"Bucket '{bucket_name}' does not exist")
                elif error_code == '403':
                    raise StoragePermissionError(f"Access denied to bucket '{bucket_name}'")
                else:
                    raise StorageConnectionError(f"Failed to access bucket: {e}")
            
            logger.info(f"S3 provider initialized for bucket: {bucket_name}")
            
        except ImportError:
            raise StorageConnectionError(
                "boto3 library not installed. "
                "Install with: pip install boto3"
            )
        except Exception as e:
            raise StorageConnectionError(f"Failed to initialize S3 client: {e}")
    
    def upload(
        self,
        local_path: str,
        remote_path: str,
        content_type: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None
    ) -> str:
        """Upload file to S3"""
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                remote_path,
                ExtraArgs=extra_args if extra_args else None
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
        """Upload bytes to S3"""
        try:
            import io
            
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_fileobj(
                io.BytesIO(data),
                self.bucket_name,
                remote_path,
                ExtraArgs=extra_args if extra_args else None
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
        """Download file from S3"""
        try:
            # Create parent directories if needed
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(
                self.bucket_name,
                remote_path,
                local_path
            )
            
            logger.info(f"Downloaded {remote_path} to {local_path}")
            return local_path
            
        except self.s3_client.exceptions.NoSuchKey:
            raise StorageNotFoundError(f"File not found: {remote_path}")
        except Exception as e:
            raise StorageError(f"Download failed: {e}")
    
    def download_bytes(
        self,
        remote_path: str
    ) -> bytes:
        """Download file as bytes from S3"""
        try:
            import io
            
            buffer = io.BytesIO()
            self.s3_client.download_fileobj(
                self.bucket_name,
                remote_path,
                buffer
            )
            
            data = buffer.getvalue()
            logger.info(f"Downloaded {remote_path} ({len(data)} bytes)")
            return data
            
        except self.s3_client.exceptions.NoSuchKey:
            raise StorageNotFoundError(f"File not found: {remote_path}")
        except Exception as e:
            raise StorageError(f"Download bytes failed: {e}")
    
    def exists(self, remote_path: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            return True
        except self.s3_client.exceptions.NoSuchKey:
            return False
        except Exception as e:
            logger.error(f"Error checking existence of {remote_path}: {e}")
            return False
    
    def delete(self, remote_path: str) -> bool:
        """Delete file from S3"""
        try:
            if not self.exists(remote_path):
                return False
            
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            
            logger.info(f"Deleted {remote_path}")
            return True
            
        except Exception as e:
            raise StorageError(f"Delete failed: {e}")
    
    def list(
        self,
        prefix: str = "",
        max_results: Optional[int] = None
    ) -> List[str]:
        """List files in S3 with prefix"""
        try:
            kwargs = {
                'Bucket': self.bucket_name,
                'Prefix': prefix
            }
            if max_results:
                kwargs['MaxKeys'] = max_results
            
            response = self.s3_client.list_objects_v2(**kwargs)
            
            paths = []
            if 'Contents' in response:
                paths = [obj['Key'] for obj in response['Contents']]
            
            logger.info(f"Listed {len(paths)} files with prefix '{prefix}'")
            return paths
            
        except Exception as e:
            raise StorageError(f"List failed: {e}")
    
    def get_metadata(self, remote_path: str) -> StorageMetadata:
        """Get metadata for S3 file"""
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=remote_path
            )
            
            return StorageMetadata(
                path=remote_path,
                size_bytes=response['ContentLength'],
                content_type=response.get('ContentType', 'application/octet-stream'),
                created_at=response['LastModified'],
                modified_at=response['LastModified'],
                etag=response.get('ETag', '').strip('"'),
                custom_metadata=response.get('Metadata', {})
            )
            
        except self.s3_client.exceptions.NoSuchKey:
            raise StorageNotFoundError(f"File not found: {remote_path}")
        except Exception as e:
            raise StorageError(f"Get metadata failed: {e}")
    
    def generate_signed_url(
        self,
        remote_path: str,
        expiration_seconds: int = 3600
    ) -> str:
        """Generate presigned URL for S3 file"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': remote_path
                },
                ExpiresIn=expiration_seconds
            )
            
            logger.info(f"Generated presigned URL for {remote_path} (expires in {expiration_seconds}s)")
            return url
            
        except Exception as e:
            raise StorageError(f"Generate presigned URL failed: {e}")
    
    def get_uri(self, remote_path: str) -> str:
        """Get S3 URI"""
        return f"s3://{self.bucket_name}/{remote_path}"
