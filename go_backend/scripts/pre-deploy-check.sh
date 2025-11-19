#!/bin/bash
# Pre-deployment validation script
# Run this before deploying to production

set -e

echo "🔍 Pre-Deployment Validation Check"
echo "=================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

WARNINGS=0
ERRORS=0

# Function to check
check() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓${NC} $1"
    else
        echo -e "${RED}✗${NC} $1"
        ((ERRORS++))
    fi
}

warn() {
    echo -e "${YELLOW}⚠${NC} $1"
    ((WARNINGS++))
}

# 1. Check Go version
echo ""
echo "1. Checking Go version..."
go version | grep -q "go1.2[1-9]" || go version | grep -q "go1.3"
check "Go version >= 1.21"

# 2. Check dependencies
echo ""
echo "2. Checking dependencies..."
cd /workspaces/ml_backend/go_backend
go mod download
check "Go modules downloaded"

go mod verify
check "Go modules verified"

# 3. Check for Gin code (should be deleted)
echo ""
echo "3. Checking for duplicate Gin code..."
if grep -r "gin.Context" internal/ --include="*.go" 2>/dev/null; then
    echo -e "${RED}✗${NC} Found Gin code (should be deleted)"
    ((ERRORS++))
else
    echo -e "${GREEN}✓${NC} No Gin code found"
fi

# 4. Check environment variables
echo ""
echo "4. Checking critical environment variables..."
check_env() {
    if [ -z "${!1}" ]; then
        echo -e "${RED}✗${NC} $1 not set"
        ((ERRORS++))
    else
        echo -e "${GREEN}✓${NC} $1 is set"
    fi
}

check_env "DATABASE_URL"
check_env "JWT_SECRET"
check_env "VALIDATION_SERVICE_ADDR"

# 5. Check for default secrets
echo ""
echo "5. Checking for insecure defaults..."
if grep -r "admin123" . --include="*.go" --include="*.yaml" --include="*.yml" 2>/dev/null; then
    echo -e "${RED}✗${NC} Found default password 'admin123'"
    ((ERRORS++))
else
    echo -e "${GREEN}✓${NC} No default passwords found"
fi

if grep -r "super-secret-key" . --include="*.go" --include="*.yaml" 2>/dev/null; then
    echo -e "${RED}✗${NC} Found default JWT secret"
    ((ERRORS++))
else
    echo -e "${GREEN}✓${NC} No default JWT secrets found"
fi

if grep -r 'origins: "\*"' . --include="*.go" 2>/dev/null; then
    warn "Found CORS wildcard - should restrict in production"
fi

# 6. Check database migrations
echo ""
echo "6. Checking database migrations..."
if [ -d "migrations" ] && [ "$(ls -A migrations/*.sql 2>/dev/null)" ]; then
    echo -e "${GREEN}✓${NC} Migration files exist"
else
    echo -e "${RED}✗${NC} No migration files found"
    ((ERRORS++))
fi

# 7. Check if migrations are applied
echo ""
echo "7. Checking database connection..."
if [ -n "$DATABASE_URL" ]; then
    # Try to connect (requires psql)
    if command -v psql &> /dev/null; then
        psql "$DATABASE_URL" -c "SELECT 1;" > /dev/null 2>&1
        check "Database connection successful"
        
        # Check if jobs table exists
        psql "$DATABASE_URL" -c "SELECT 1 FROM jobs LIMIT 1;" > /dev/null 2>&1
        check "Migrations applied (jobs table exists)"
    else
        warn "psql not installed, skipping database check"
    fi
else
    warn "DATABASE_URL not set, skipping database check"
fi

# 8. Check ML service connectivity
echo ""
echo "8. Checking ML service connectivity..."
if [ -n "$VALIDATION_SERVICE_ADDR" ]; then
    # Check if grpcurl is available
    if command -v grpcurl &> /dev/null; then
        grpcurl -plaintext $VALIDATION_SERVICE_ADDR list > /dev/null 2>&1
        check "Validation service reachable"
    else
        warn "grpcurl not installed, skipping ML service check"
    fi
else
    warn "VALIDATION_SERVICE_ADDR not set, skipping ML service check"
fi

# 9. Run tests
echo ""
echo "9. Running unit tests..."
go test ./pkg/... -short -timeout 30s
check "Unit tests passed"

# 10. Check for TODO/FIXME in critical files
echo ""
echo "10. Checking for unfinished work..."
TODO_COUNT=$(grep -r "TODO\|FIXME" internal/ pkg/ --include="*.go" 2>/dev/null | wc -l)
if [ "$TODO_COUNT" -gt 0 ]; then
    warn "Found $TODO_COUNT TODO/FIXME comments"
fi

# 11. Check logging configuration
echo ""
echo "11. Checking logging setup..."
if grep -q "logger.Init" cmd/api/main.go 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Logger initialized in main.go"
else
    echo -e "${RED}✗${NC} Logger not initialized"
    ((ERRORS++))
fi

# 12. Check middleware registration
echo ""
echo "12. Checking middleware..."
if grep -q "middleware.TraceID()" cmd/api/main.go 2>/dev/null; then
    echo -e "${GREEN}✓${NC} TraceID middleware registered"
else
    warn "TraceID middleware not registered"
fi

if grep -q "middleware.RateLimit()" cmd/api/main.go 2>/dev/null; then
    echo -e "${GREEN}✓${NC} RateLimit middleware registered"
else
    warn "RateLimit middleware not registered"
fi

# 13. Check health endpoints
echo ""
echo "13. Checking health endpoints..."
if grep -q "/health" cmd/api/main.go internal/handlers/*.go 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Health endpoints defined"
else
    warn "Health endpoints not found"
fi

# 14. Check circuit breaker usage
echo ""
echo "14. Checking circuit breaker implementation..."
if grep -q "circuitbreaker" pkg/grpcclient/*.go 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Circuit breakers implemented"
else
    warn "Circuit breakers not found in gRPC clients"
fi

# 15. Build check
echo ""
echo "15. Building application..."
go build -o /tmp/synthos-api cmd/api/main.go
check "Application builds successfully"

# Summary
echo ""
echo "=================================="
echo "Summary"
echo "=================================="
echo -e "Errors: ${RED}${ERRORS}${NC}"
echo -e "Warnings: ${YELLOW}${WARNINGS}${NC}"

if [ $ERRORS -gt 0 ]; then
    echo ""
    echo -e "${RED}❌ Pre-deployment validation FAILED${NC}"
    echo "Fix errors before deploying to production"
    exit 1
elif [ $WARNINGS -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}⚠ Pre-deployment validation passed with warnings${NC}"
    echo "Review warnings before deploying to production"
    exit 0
else
    echo ""
    echo -e "${GREEN}✅ Pre-deployment validation PASSED${NC}"
    echo "Ready for production deployment"
    exit 0
fi
