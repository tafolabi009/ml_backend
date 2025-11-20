#!/bin/bash
# Production Readiness Verification Script for ML Backend
# This script verifies all components are working correctly

echo "======================================================================="
echo "ML Backend Production Readiness Verification"
echo "======================================================================="
echo ""

# Color codes
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
PASSED=0
FAILED=0

# Function to test a condition
test_condition() {
    local description="$1"
    local command="$2"
    
    echo -n "Testing: $description... "
    if eval "$command" &> /dev/null; then
        echo -e "${GREEN}✅ PASS${NC}"
        ((PASSED++))
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}"
        ((FAILED++))
        return 1
    fi
}

echo "1. Docker Services Status"
echo "-------------------------"
test_condition "PostgreSQL is running" "docker ps | grep synthos-postgres | grep -q healthy"
test_condition "Redis is running" "docker ps | grep synthos-redis | grep -q healthy"
test_condition "MinIO is running" "docker ps | grep synthos-minio | grep -q healthy"
test_condition "ML Backend is running" "docker ps | grep synthos-ml-backend | grep -q healthy"
echo ""

echo "2. gRPC Services Connectivity"
echo "-----------------------------"
test_condition "Validation Service (50051)" "python -c 'import grpc; ch=grpc.insecure_channel(\"localhost:50051\"); grpc.channel_ready_future(ch).result(timeout=5)' 2>/dev/null"
test_condition "Collapse Service (50052)" "python -c 'import grpc; ch=grpc.insecure_channel(\"localhost:50052\"); grpc.channel_ready_future(ch).result(timeout=5)' 2>/dev/null"
echo ""

echo "3. Resonance Neural Networks"
echo "----------------------------"
test_condition "RNN package is installed" "docker exec synthos-ml-backend python -c 'import resonance_nn' 2>/dev/null"
test_condition "RNN is available in code" "docker exec synthos-ml-backend python -c 'from src.model_architectures import RESONANCE_AVAILABLE; assert RESONANCE_AVAILABLE' 2>/dev/null"
test_condition "Can create RNN models" "docker exec synthos-ml-backend python -c 'from src.model_architectures import create_resonance_model; create_resonance_model(size=\"tiny\")' 2>/dev/null"
echo ""

echo "4. Service Orchestration"
echo "-----------------------"
test_condition "Orchestrator initialized" "docker logs synthos-ml-backend 2>&1 | grep -q 'All modules initialized successfully'"
test_condition "ValidationEngine ready" "docker logs synthos-ml-backend 2>&1 | grep -q 'ValidationEngine service initialized'"
test_condition "CollapseEngine ready" "docker logs synthos-ml-backend 2>&1 | grep -q 'CollapseEngine service initialized'"
echo ""

echo "5. Resource Availability"
echo "-----------------------"
test_condition "Models directory exists" "docker exec synthos-ml-backend test -d /models"
test_condition "Cache directory exists" "docker exec synthos-ml-backend test -d /cache"
test_condition "Signatures directory exists" "docker exec synthos-ml-backend test -d /data/signatures"
echo ""

echo "======================================================================="
echo "Summary"
echo "======================================================================="
echo -e "Tests Passed: ${GREEN}$PASSED${NC}"
echo -e "Tests Failed: ${RED}$FAILED${NC}"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✅ All tests passed! ML Backend is production ready.${NC}"
    echo ""
    echo "Next steps:"
    echo "  - Deploy to GPU-enabled instance for production use"
    echo "  - Configure monitoring (Prometheus/Grafana)"
    echo "  - Set up TLS/SSL for gRPC services"
    echo "  - Run load tests with production data"
    exit 0
else
    echo -e "${RED}❌ Some tests failed. Please review the failures above.${NC}"
    echo ""
    echo "Common fixes:"
    echo "  - Ensure all services are started: docker-compose up -d ml-backend"
    echo "  - Check logs: docker logs synthos-ml-backend"
    echo "  - Verify network connectivity: docker network inspect synthos-network"
    exit 1
fi
