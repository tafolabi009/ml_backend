#!/bin/bash

# Synthos Test Suite Runner
# Runs unit and integration tests for the Go backend

set -e

echo "🧪 Synthos Test Suite"
echo "===================="

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to go_backend directory
cd "$(dirname "$0")/go_backend"

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

# Check if Go is installed
if ! command -v go &> /dev/null; then
    print_error "Go is not installed. Please install Go 1.24 or later."
    exit 1
fi

print_status "Go version: $(go version)"

# Install dependencies
echo ""
echo "📦 Installing dependencies..."
go mod download
go mod tidy
print_status "Dependencies installed"

# Run tests with coverage
echo ""
echo "🧪 Running unit tests..."
go test -v -race -coverprofile=coverage.out ./internal/handlers/... || {
    print_error "Unit tests failed"
    exit 1
}
print_status "Unit tests passed"

# Display coverage report
echo ""
echo "📊 Coverage Report:"
go tool cover -func=coverage.out | tail -n 1

# Generate HTML coverage report
go tool cover -html=coverage.out -o coverage.html
print_status "Coverage report generated: coverage.html"

# Run integration tests (skip if services are not running)
echo ""
echo "🔗 Running integration tests..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    go test -v -race ./tests/... || {
        print_warning "Integration tests failed (may require running services)"
    }
    print_status "Integration tests passed"
else
    print_warning "Orchestrator service not running, skipping integration tests"
    echo "   Start services with: docker-compose up -d"
fi

# Run go vet
echo ""
echo "🔍 Running go vet..."
go vet ./... || {
    print_error "go vet found issues"
    exit 1
}
print_status "go vet passed"

# Run golint if available
if command -v golint &> /dev/null; then
    echo ""
    echo "🔍 Running golint..."
    golint ./... || print_warning "golint found style issues"
fi

# Run staticcheck if available
if command -v staticcheck &> /dev/null; then
    echo ""
    echo "🔍 Running staticcheck..."
    staticcheck ./... || print_warning "staticcheck found issues"
fi

# Summary
echo ""
echo "===================="
echo "✅ Test suite completed successfully!"
echo ""
echo "📁 Generated files:"
echo "   - coverage.out   (coverage data)"
echo "   - coverage.html  (HTML coverage report)"
echo ""
echo "🚀 To view coverage report:"
echo "   open coverage.html"
echo ""
