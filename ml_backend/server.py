#!/usr/bin/env python3
"""
Unified gRPC Server for ML Backend
Serves both ValidationEngine and CollapseEngine services
"""

import asyncio
import logging
import signal
import sys
from concurrent import futures
from pathlib import Path

import grpc
from grpc import aio

# Import generated protobuf stubs
from src.grpc_services import validation_pb2_grpc
from src.grpc_services.validation_server_complete import (
    ValidationEngineServicer,
    CollapseEngineServicer,
    DataServiceServicer
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
import os

VALIDATION_PORT = int(os.getenv('VALIDATION_SERVICE_PORT', '50051'))
COLLAPSE_PORT = int(os.getenv('COLLAPSE_SERVICE_PORT', '50052'))
DATA_PORT = int(os.getenv('DATA_SERVICE_PORT', '50054'))
HOST = os.getenv('SERVICE_HOST', '0.0.0.0')
MAX_WORKERS = int(os.getenv('GRPC_MAX_WORKERS', '10'))
MAX_MESSAGE_SIZE = int(os.getenv('GRPC_MAX_MESSAGE_SIZE', '100000000'))  # 100MB


class GracefulKiller:
    """Handle graceful shutdown on SIGTERM/SIGINT"""
    kill_now = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    
    def exit_gracefully(self, *args):
        self.kill_now = True


async def serve():
    """Start all gRPC servers"""
    killer = GracefulKiller()
    
    # Create async servers
    validation_server = aio.server(
        futures.ThreadPoolExecutor(max_workers=MAX_WORKERS),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_SIZE),
            ('grpc.max_receive_message_length', MAX_MESSAGE_SIZE),
        ]
    )
    
    collapse_server = aio.server(
        futures.ThreadPoolExecutor(max_workers=MAX_WORKERS),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_SIZE),
            ('grpc.max_receive_message_length', MAX_MESSAGE_SIZE),
        ]
    )
    
    data_server = aio.server(
        futures.ThreadPoolExecutor(max_workers=MAX_WORKERS),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_SIZE),
            ('grpc.max_receive_message_length', MAX_MESSAGE_SIZE),
        ]
    )
    
    # Initialize service configuration
    config = {
        'database_url': os.getenv('DATABASE_URL'),
        'redis_url': os.getenv('REDIS_URL'),
        'minio_endpoint': os.getenv('MINIO_ENDPOINT'),
        'minio_access_key': os.getenv('MINIO_ACCESS_KEY'),
        'minio_secret_key': os.getenv('MINIO_SECRET_KEY'),
        'gpu_devices': os.getenv('GPU_DEVICES', '0,1,2,3'),
    }
    
    # Register servicers
    validation_pb2_grpc.add_ValidationEngineServicer_to_server(
        ValidationEngineServicer(config), validation_server
    )
    validation_pb2_grpc.add_CollapseEngineServicer_to_server(
        CollapseEngineServicer(config), collapse_server
    )
    validation_pb2_grpc.add_DataServiceServicer_to_server(
        DataServiceServicer(config), data_server
    )
    
    # Start servers
    validation_server.add_insecure_port(f'{HOST}:{VALIDATION_PORT}')
    collapse_server.add_insecure_port(f'{HOST}:{COLLAPSE_PORT}')
    data_server.add_insecure_port(f'{HOST}:{DATA_PORT}')
    
    await validation_server.start()
    await collapse_server.start()
    await data_server.start()
    
    logger.info(f"✅ Validation Service listening on {HOST}:{VALIDATION_PORT}")
    logger.info(f"✅ Collapse Service listening on {HOST}:{COLLAPSE_PORT}")
    logger.info(f"✅ Data Service listening on {HOST}:{DATA_PORT}")
    logger.info("🚀 All ML services started successfully")
    
    # Wait for termination
    try:
        while not killer.kill_now:
            await asyncio.sleep(1)
    finally:
        logger.info("Shutting down gracefully...")
        await validation_server.stop(5)
        await collapse_server.stop(5)
        await data_server.stop(5)
        logger.info("All services stopped")


if __name__ == '__main__':
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        sys.exit(1)
