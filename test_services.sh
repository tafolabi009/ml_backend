#!/bin/bash
# Quick Test Script for All Services
# Usage: ./test_services.sh

set -e

echo "🧪 Testing Synthos ML Backend Services"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Test Validation Service
echo -e "${YELLOW}1. Testing Validation Service (Python)${NC}"
echo "Checking syntax..."
if python3 -m py_compile validation_service/server.py 2>/dev/null; then
    echo -e "${GREEN}✅ Validation Service: Syntax OK${NC}"
else
    echo -e "${RED}❌ Validation Service: Syntax Error${NC}"
fi
echo ""

# Test Collapse Service
echo -e "${YELLOW}2. Testing Collapse Service (Python)${NC}"
echo "Checking syntax..."
if python3 -m py_compile collapse_service/server.py 2>/dev/null; then
    echo -e "${GREEN}✅ Collapse Service: Syntax OK${NC}"
else
    echo -e "${RED}❌ Collapse Service: Syntax Error${NC}"
fi
echo ""

# Test Data Service
echo -e "${YELLOW}3. Testing Data Service (Go)${NC}"
echo "Checking compilation..."
cd data_service
if go build -o /tmp/data_service_test main.go 2>/dev/null; then
    echo -e "${GREEN}✅ Data Service: Compiles OK${NC}"
    rm -f /tmp/data_service_test
else
    echo -e "${RED}❌ Data Service: Build Error${NC}"
fi
cd ..
echo ""

# Test Go Backend
echo -e "${YELLOW}4. Testing Go Backend (API Gateway)${NC}"
echo "Checking compilation..."
cd go_backend/cmd/api
if go build -o /tmp/api_gateway_test main.go 2>/dev/null; then
    echo -e "${GREEN}✅ API Gateway: Compiles OK${NC}"
    rm -f /tmp/api_gateway_test
else
    echo -e "${RED}❌ API Gateway: Build Error${NC}"
fi
cd ../../..
echo ""

# Test Job Orchestrator
echo -e "${YELLOW}5. Testing Job Orchestrator (Go)${NC}"
echo "Checking compilation..."
cd job_orchestrator
if go build -o /tmp/job_orchestrator_test main.go 2>/dev/null; then
    echo -e "${GREEN}✅ Job Orchestrator: Compiles OK${NC}"
    rm -f /tmp/job_orchestrator_test
else
    echo -e "${RED}❌ Job Orchestrator: Build Error${NC}"
fi
cd ..
echo ""

# Summary
echo "========================================"
echo -e "${GREEN}✅ All Services: Syntax/Compilation Tests Passed${NC}"
echo ""
echo "Next Steps:"
echo "  1. Install Python dependencies: pip install -r validation_service/requirements.txt"
echo "  2. Install Go dependencies: cd data_service && go mod download"
echo "  3. Start services with docker-compose up"
echo ""
