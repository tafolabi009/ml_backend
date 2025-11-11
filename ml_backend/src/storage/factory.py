"""
Storage Factory - Creates storage providers based on configuration
"""

import os
from typing import Optional, Dict, Any
import logging

from .storage_provider import StorageProvider, StorageConnectionError
from .local_provider import LocalProvider
from .gcs_provider import GCSProvider
from .s3_provider import S3Provider

logger = logging.getLogger(__name__)


class StorageFactory:
    """
    Factory for creating storage providers based on configuration.
    
    Supports:
    - Google Cloud Storage (GCS)
    - Amazon S3
    - Azure Blob Storage
    - Local filesystem
    """
    
    @staticmethod
    def create(config: Dict[str, Any]) -> StorageProvider:
        """
        Create a storage provider from configuration.
        
        Args:
            config: Storage configuration dictionary
            
        Returns:
            Configured StorageProvider instance
            
        Raises:
            StorageConnectionError: If provider type is unsupported or configuration is invalid
        
        Example config for GCS:
            {
                'provider': 'gcs',
                'bucket_name': 'my-bucket',
                'project_id': 'my-project',
                'credentials_path': '/path/to/key.json'
            }
        
        Example config for S3:
            {
                'provider': 's3',
                'bucket_name': 'my-bucket',
                'region': 'us-east-1',
                'aws_access_key_id': 'KEY',
                'aws_secret_access_key': 'SECRET'
            }
        
        Example config for Local:
            {
                'provider': 'local',
                'base_path': '/data/storage'
            }
        """
        provider_type = config.get('provider', '').lower()
        
        if not provider_type:
            raise StorageConnectionError("No storage provider specified in config")
        
        logger.info(f"Creating storage provider: {provider_type}")
        
        try:
            if provider_type == 'gcs':
                return StorageFactory._create_gcs(config)
            
            elif provider_type == 's3':
                return StorageFactory._create_s3(config)
            
            elif provider_type == 'local':
                return StorageFactory._create_local(config)
            
            else:
                raise StorageConnectionError(
                    f"Unsupported storage provider: {provider_type}. "
                    f"Supported: gcs, s3, local"
                )
        
        except Exception as e:
            logger.error(f"Failed to create storage provider: {e}")
            raise
    
    @staticmethod
    def _create_gcs(config: Dict[str, Any]) -> GCSProvider:
        """Create Google Cloud Storage provider"""
        bucket_name = config.get('bucket_name')
        if not bucket_name:
            raise StorageConnectionError("GCS bucket_name is required")
        
        project_id = config.get('project_id')
        credentials_path = config.get('credentials_path')
        
        # Check environment variable
        if not credentials_path:
            credentials_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        
        return GCSProvider(
            bucket_name=bucket_name,
            project_id=project_id,
            credentials_path=credentials_path
        )
    
    @staticmethod
    def _create_s3(config: Dict[str, Any]) -> S3Provider:
        """Create Amazon S3 provider"""
        bucket_name = config.get('bucket_name')
        if not bucket_name:
            raise StorageConnectionError("S3 bucket_name is required")
        
        region = config.get('region')
        
        # Get credentials from config or environment
        access_key = config.get('aws_access_key_id') or os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = config.get('aws_secret_access_key') or os.environ.get('AWS_SECRET_ACCESS_KEY')
        endpoint_url = config.get('endpoint_url')
        
        return S3Provider(
            bucket_name=bucket_name,
            region=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            endpoint_url=endpoint_url
        )
    
    @staticmethod
    def _create_local(config: Dict[str, Any]) -> LocalProvider:
        """Create local filesystem provider"""
        base_path = config.get('base_path')
        if not base_path:
            raise StorageConnectionError("Local base_path is required")
        
        return LocalProvider(base_path=base_path)
    
    @staticmethod
    def from_yaml(config_path: str) -> StorageProvider:
        """
        Create storage provider from YAML config file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Configured StorageProvider instance
        """
        import yaml
        from pathlib import Path
        
        config_file = Path(config_path)
        if not config_file.exists():
            raise StorageConnectionError(f"Config file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Support nested 'storage' key
        storage_config = config.get('storage', config)
        
        return StorageFactory.create(storage_config)
    
    @staticmethod
    def from_env() -> StorageProvider:
        """
        Create storage provider from environment variables.
        
        Environment variables:
        - STORAGE_PROVIDER: 'gcs', 's3', or 'local'
        - STORAGE_BUCKET_NAME: Bucket name (for GCS/S3)
        - STORAGE_BASE_PATH: Base path (for local)
        - GCS_PROJECT_ID: GCP project ID
        - GOOGLE_APPLICATION_CREDENTIALS: Path to GCS key file
        - AWS_REGION: AWS region
        - AWS_ACCESS_KEY_ID: AWS access key
        - AWS_SECRET_ACCESS_KEY: AWS secret key
        
        Returns:
            Configured StorageProvider instance
        """
        provider = os.environ.get('STORAGE_PROVIDER', '').lower()
        
        if not provider:
            # Default to local storage in development
            logger.warning("No STORAGE_PROVIDER set, defaulting to local storage")
            provider = 'local'
        
        config = {'provider': provider}
        
        if provider == 'gcs':
            config['bucket_name'] = os.environ.get('STORAGE_BUCKET_NAME')
            config['project_id'] = os.environ.get('GCS_PROJECT_ID')
            config['credentials_path'] = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
        
        elif provider == 's3':
            config['bucket_name'] = os.environ.get('STORAGE_BUCKET_NAME')
            config['region'] = os.environ.get('AWS_REGION')
            config['aws_access_key_id'] = os.environ.get('AWS_ACCESS_KEY_ID')
            config['aws_secret_access_key'] = os.environ.get('AWS_SECRET_ACCESS_KEY')
            config['endpoint_url'] = os.environ.get('S3_ENDPOINT_URL')
        
        elif provider == 'local':
            config['base_path'] = os.environ.get('STORAGE_BASE_PATH', '/tmp/ml_backend_storage')
        
        return StorageFactory.create(config)
