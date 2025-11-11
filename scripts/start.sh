#!/bin/bash
# Quick start script for Synthos microservices

set -e

echo "🚀 Starting Synthos Microservices..."
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check Docker is running
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running. Please start Docker first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker is running${NC}"

# Check Docker Compose version
if ! docker-compose version > /dev/null 2>&1; then
    echo -e "${RED}❌ Docker Compose not found. Please install Docker Compose.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker Compose is available${NC}"
echo ""

# Create .env if it doesn't exist
if [ ! -f .env ]; then
    echo -e "${YELLOW}Creating .env file from .env.example...${NC}"
    cp .env.example .env
    echo -e "${GREEN}✓ .env file created${NC}"
fi

# Stop any existing services
echo ""
echo "🛑 Stopping existing services..."
docker-compose down

# Start infrastructure services first
echo ""
echo "🔧 Starting infrastructure services (PostgreSQL, Redis, MinIO)..."
docker-compose up -d postgres redis minio

echo "⏳ Waiting for services to be healthy..."
sleep 10

# Check if services are healthy
if docker-compose ps postgres | grep -q "healthy"; then
    echo -e "${GREEN}✓ PostgreSQL is ready${NC}"
else
    echo -e "${RED}❌ PostgreSQL is not healthy${NC}"
fi

if docker-compose ps redis | grep -q "healthy"; then
    echo -e "${GREEN}✓ Redis is ready${NC}"
else
    echo -e "${RED}❌ Redis is not healthy${NC}"
fi

if docker-compose ps minio | grep -q "healthy"; then
    echo -e "${GREEN}✓ MinIO is ready${NC}"
else
    echo -e "${RED}❌ MinIO is not healthy${NC}"
fi

# Initialize MinIO bucket
echo ""
echo "🪣 Initializing MinIO bucket..."
sleep 5
docker exec synthos-minio mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
docker exec synthos-minio mc mb local/synthos-datasets 2>/dev/null || echo "Bucket already exists"
docker exec synthos-minio mc policy set download local/synthos-datasets 2>/dev/null || true
echo -e "${GREEN}✓ MinIO bucket configured${NC}"

# Start application services
echo ""
echo "🚀 Starting application services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for all services to start..."
sleep 15

# Show service status
echo ""
echo "📊 Service Status:"
docker-compose ps

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${GREEN}✅ Synthos Microservices Started Successfully!${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "🌐 Access Points:"
echo "  • API Gateway:      http://localhost:8080"
echo "  • MinIO Console:    http://localhost:9001 (minioadmin/minioadmin)"
echo "  • PostgreSQL:       localhost:5432 (postgres/postgres)"
echo "  • Redis:            localhost:6379"
echo ""
echo "📚 Documentation:"
echo "  • Docker Setup:     DOCKER_SETUP.md"
echo "  • Architecture:     MICROSERVICES_ARCHITECTURE.md"
echo ""
echo "🔧 Useful Commands:"
echo "  • View logs:        docker-compose logs -f"
echo "  • Stop services:    docker-compose down"
echo "  • Restart service:  docker-compose restart <service-name>"
echo ""
echo "🧪 Test the API:"
echo "  curl http://localhost:8080/health"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
