#!/bin/bash

# Synthos ML Backend - Complete Setup and Verification Script

set -e

echo "🚀 Synthos ML Backend - Complete Setup"
echo "======================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check prerequisites
echo "📋 Checking prerequisites..."
echo ""

# Check Docker
if command -v docker &> /dev/null; then
    print_status "Docker installed: $(docker --version | cut -d ' ' -f3)"
else
    print_error "Docker is not installed"
    exit 1
fi

# Check Docker Compose
if command -v docker-compose &> /dev/null; then
    print_status "Docker Compose installed: $(docker-compose --version | cut -d ' ' -f4)"
else
    print_error "Docker Compose is not installed"
    exit 1
fi

# Check Go
if command -v go &> /dev/null; then
    print_status "Go installed: $(go version | cut -d ' ' -f3)"
else
    print_warning "Go is not installed (optional for development)"
fi

echo ""
echo "🛠️  Setting up services..."
echo ""

# Change to project root
cd "$(dirname "$0")"

# Create required directories
print_info "Creating required directories..."
mkdir -p monitoring/grafana/dashboards
mkdir -p monitoring/grafana/datasources
mkdir -p admin_dashboard/views

# Build services
print_info "Building Docker images..."
docker-compose build --parallel || {
    print_error "Failed to build Docker images"
    exit 1
}
print_status "Docker images built successfully"

# Start infrastructure services
print_info "Starting infrastructure services (PostgreSQL, Redis, MinIO)..."
docker-compose up -d postgres redis minio
sleep 5
print_status "Infrastructure services started"

# Wait for database to be ready
print_info "Waiting for PostgreSQL to be ready..."
timeout 60 bash -c 'until docker-compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 2; done' || {
    print_error "PostgreSQL failed to start"
    exit 1
}
print_status "PostgreSQL is ready"

# Start ML services
print_info "Starting ML services (Validation, Collapse, Data)..."
docker-compose up -d validation-service collapse-service data-service
sleep 3
print_status "ML services started"

# Start orchestrator
print_info "Starting Job Orchestrator..."
docker-compose up -d job-orchestrator
sleep 3
print_status "Job Orchestrator started"

# Start API gateway
print_info "Starting API Gateway (Go Backend)..."
docker-compose up -d api-gateway
sleep 3
print_status "API Gateway started"

# Optional: Start monitoring stack
read -p "$(echo -e ${BLUE}?${NC} Start monitoring stack \(Prometheus, Grafana, Jaeger, Admin Dashboard\)? [y/N]: )" -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_info "Starting monitoring stack..."
    docker-compose --profile monitoring up -d prometheus grafana jaeger admin-dashboard
    sleep 5
    print_status "Monitoring stack started"
fi

echo ""
echo "🔍 Verifying services..."
echo ""

# Check services
services=(
    "http://localhost:8000/health:Go Backend (API Gateway)"
    "http://localhost:8080/health:Job Orchestrator"
)

all_healthy=true
for service in "${services[@]}"; do
    IFS=':' read -r url name <<< "$service"
    if curl -sf "$url" > /dev/null 2>&1; then
        print_status "$name is healthy"
    else
        print_error "$name is not responding"
        all_healthy=false
    fi
done

echo ""
echo "📊 Service URLs:"
echo "================"
echo ""
echo "Core Services:"
echo "  🌐 API Gateway:        http://localhost:8000"
echo "  🎯 Job Orchestrator:   http://localhost:8080"
echo "  🔍 Health Check:       http://localhost:8000/health"
echo ""
echo "Monitoring (if enabled):"
echo "  📈 Prometheus:         http://localhost:9090"
echo "  📊 Grafana:            http://localhost:3000 (admin/admin)"
echo "  🔎 Jaeger:             http://localhost:16686"
echo "  🖥️  Admin Dashboard:    http://localhost:3001 (admin/admin)"
echo "  📊 Metrics:            http://localhost:8000/metrics"
echo ""
echo "Infrastructure:"
echo "  🗄️  PostgreSQL:         localhost:5432 (postgres/postgres)"
echo "  💾 Redis:              localhost:6379"
echo "  📦 MinIO Console:      http://localhost:9001 (minioadmin/minioadmin)"
echo ""

# Test API
echo "🧪 Testing API..."
echo ""

# Register test user
print_info "Registering test user..."
REGISTER_RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/auth/register \
    -H "Content-Type: application/json" \
    -d '{
        "email": "test@example.com",
        "password": "SecurePassword123!",
        "name": "Test User",
        "company": "Test Company"
    }' 2>/dev/null) || {
    print_warning "Registration failed (user may already exist)"
}

# Login
print_info "Logging in..."
LOGIN_RESPONSE=$(curl -s -X POST http://localhost:8000/api/v1/auth/login \
    -H "Content-Type: application/json" \
    -d '{
        "email": "test@example.com",
        "password": "SecurePassword123!"
    }' 2>/dev/null)

if echo "$LOGIN_RESPONSE" | grep -q "access_token"; then
    print_status "Authentication successful"
    TOKEN=$(echo "$LOGIN_RESPONSE" | jq -r '.access_token' 2>/dev/null)
else
    print_error "Authentication failed"
    TOKEN=""
fi

# Check orchestrator resources
print_info "Checking orchestrator resources..."
RESOURCES=$(curl -s http://localhost:8080/api/v1/resources/status 2>/dev/null)
if echo "$RESOURCES" | grep -q "total_workers"; then
    print_status "Orchestrator resources available"
    echo "  Workers: $(echo "$RESOURCES" | jq -r '.total_workers' 2>/dev/null || echo 'N/A')"
    echo "  CPU: $(echo "$RESOURCES" | jq -r '.total_cpu_cores' 2>/dev/null || echo 'N/A') cores"
    echo "  Memory: $(echo "$RESOURCES" | jq -r '.total_memory_gb' 2>/dev/null || echo 'N/A') GB"
else
    print_warning "Could not fetch orchestrator resources"
fi

echo ""
echo "📚 Documentation:"
echo "================="
echo "  📖 QUICKSTART.md                   - Quick start guide"
echo "  🏗️  ARCHITECTURE_COMPLETE.md        - System architecture"
echo "  🧪 TESTING_RUN_GUIDE.md            - Testing guide"
echo "  ✅ IMPLEMENTATION_COMPLETE_FINAL.md - Complete implementation summary"
echo ""

echo "🎉 Setup Complete!"
echo ""

if [ "$all_healthy" = true ]; then
    echo -e "${GREEN}All services are healthy and ready to use!${NC}"
    echo ""
    echo "Next steps:"
    echo "  1. View services: docker-compose ps"
    echo "  2. View logs: docker-compose logs -f api-gateway"
    echo "  3. Run tests: ./run_tests.sh"
    echo "  4. Check monitoring dashboards at the URLs above"
    echo ""
else
    echo -e "${YELLOW}Some services may not be healthy. Check logs:${NC}"
    echo "  docker-compose logs api-gateway"
    echo "  docker-compose logs job-orchestrator"
    echo ""
fi

# Save token for later use
if [ -n "$TOKEN" ]; then
    echo "export SYNTHOS_TOKEN='$TOKEN'" > .env.test
    print_info "Auth token saved to .env.test"
fi
