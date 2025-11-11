#!/bin/bash
# Quick Test Runner Script
# Runs all tests and generates coverage report

set -e  # Exit on error

echo "======================================"
echo "Synthos ML Backend - Test Runner"
echo "======================================"
echo ""

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "⚠️  No virtual environment found. Creating one..."
    python3 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install -r requirements.txt
    echo "✅ Virtual environment created"
else
    source venv/bin/activate
    echo "✅ Using existing virtual environment"
fi

echo ""
echo "======================================"
echo "1. Running Code Quality Checks"
echo "======================================"
echo ""

# Linting
echo "Running ruff linter..."
ruff check src/ tests/ || echo "⚠️  Linting issues found (non-blocking)"

echo ""
echo "======================================"
echo "2. Running Unit Tests"
echo "======================================"
echo ""

pytest tests/unit/ -v --cov=src --cov-report=term-missing --cov-report=html || echo "⚠️  Some unit tests failed"

echo ""
echo "======================================"
echo "3. Running Integration Tests"
echo "======================================"
echo ""

pytest tests/integration/test_full_pipeline.py -v --timeout=300 || echo "⚠️  Some integration tests failed"

echo ""
echo "======================================"
echo "4. Coverage Summary"
echo "======================================"
echo ""

coverage report

echo ""
echo "======================================"
echo "Test Run Complete!"
echo "======================================"
echo ""
echo "📊 Coverage report available at: htmlcov/index.html"
echo "🔍 Open with: open htmlcov/index.html (Mac) or xdg-open htmlcov/index.html (Linux)"
echo ""
echo "Next steps:"
echo "  1. Review coverage report"
echo "  2. Run load tests: python tests/load/test_load.py"
echo "  3. Fix any failing tests"
echo ""
