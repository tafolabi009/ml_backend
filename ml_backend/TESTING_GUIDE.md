# 🚀 Testing Guide: Synthos ML Backend

This guide helps you run tests and verify the ML Backend functionality.

> ⚠️ **Status: Alpha** - Testing framework in place, coverage target is 70%+

---

## ⚡ Quick Test (1 minute)

```bash
cd ml_backend

# Run the automated test script
./run_tests.sh
```

This will:
- ✅ Run code quality checks (ruff, black, mypy)
- ✅ Run unit tests with coverage
- ✅ Run integration tests
- ✅ Generate coverage report

---

## 📋 Test Structure

```
tests/
├── unit/                           # Unit tests
│   ├── test_collapse_detector.py   # 13 tests - collapse detection
│   └── test_diversity_analyzer.py  # 14 tests - diversity analysis
├── integration/                    # Integration tests
│   └── test_full_pipeline.py       # 15 tests - end-to-end
└── load/                           # Load/benchmark tests
    └── test_load.py                # Performance benchmarks
```

---

## 🧪 Running Tests

### All Tests

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=src --cov-report=html

# Generate HTML coverage report
open htmlcov/index.html
```

### Unit Tests Only

```bash
pytest tests/unit/ -v

# Specific test file
pytest tests/unit/test_collapse_detector.py -v

# Specific test
pytest tests/unit/test_collapse_detector.py::TestCollapseDetector::test_healthy_data_passes -v
```

### Integration Tests

```bash
pytest tests/integration/ -v

# With timeout (for long-running tests)
pytest tests/integration/ -v --timeout=300
```

### Load/Benchmark Tests

```bash
python tests/load/test_load.py

# Results saved to: benchmark_results.json
```

---

## 📊 Code Quality Checks

```bash
# Linting
ruff check src/ tests/

# Fix linting issues automatically
ruff check src/ tests/ --fix

# Formatting check
black --check src/ tests/

# Format code
black src/ tests/

# Type checking
mypy src/ --ignore-missing-imports
```

---

## 📈 Test Coverage

### Current Status
- **Unit tests**: ~30% coverage
- **Target**: 70%+ coverage

### Generate Coverage Report

```bash
pytest tests/ --cov=src --cov-report=html
```

### View Coverage

```bash
# Open in browser
open htmlcov/index.html

# Or start local server
cd htmlcov && python -m http.server 8000
# Visit http://localhost:8000
```

**Coverage Legend:**
- 🟢 **Green**: Well-tested code
- 🟡 **Yellow**: Partially tested
- 🔴 **Red**: Untested (needs attention)

---

## 🔍 Test Scenarios

### Test Healthy Data Detection
```bash
pytest tests/unit/test_collapse_detector.py::TestCollapseDetector::test_healthy_data_passes -v
```

### Test Collapsed Data Detection
```bash
pytest tests/unit/test_collapse_detector.py::TestCollapseDetector::test_collapsed_data_fails -v
```

### Test Error Handling
```bash
pytest tests/integration/test_full_pipeline.py::TestFullPipeline::test_error_handling_invalid_path -v
```

### Test Small Dataset
```bash
pytest tests/integration/test_full_pipeline.py::TestPipelineRobustness::test_small_dataset -v
```

---

## 💡 Troubleshooting

### "Module not found" errors
```bash
pip install -r requirements.txt
pip install -e .
```

### "No tests ran"
```bash
# Make sure you're in the ml_backend directory
cd /workspaces/ml_backend/ml_backend

# Check test files exist
ls tests/unit/
ls tests/integration/
```

### Tests timeout
```bash
# Increase timeout
pytest tests/integration/ -v --timeout=600
```

### Coverage not generated
```bash
# Install coverage
pip install pytest-cov

# Run with explicit coverage
pytest --cov=src --cov-report=html
```

### GPU tests skipped
Tests will automatically skip GPU-specific tests on CPU-only environments. This is expected behavior.

---

## ✅ Success Criteria

Your tests are working if:
1. ✅ `./run_tests.sh` completes without errors
2. ✅ Coverage report generates in `htmlcov/`
3. ✅ Most unit tests pass (some may skip on CPU)
4. ✅ Integration tests complete within timeout
5. ✅ Linting runs (may show warnings)

---

## 🚀 Adding New Tests

### Unit Test Template

```python
import pytest
from src.your_module import YourClass

class TestYourClass:
    def setup_method(self):
        self.instance = YourClass()

    def test_basic_functionality(self):
        result = self.instance.method()
        assert result is not None

    def test_edge_case(self):
        with pytest.raises(ValueError):
            self.instance.method(invalid_input=True)
```

### Integration Test Template

```python
import pytest
import asyncio
from src.orchestrator import SynthosOrchestrator

class TestIntegration:
    @pytest.fixture
    def orchestrator(self):
        return SynthosOrchestrator()

    @pytest.mark.asyncio
    async def test_full_pipeline(self, orchestrator):
        result = await orchestrator.validate("test_data.csv", "csv")
        assert result.status == "completed"
```

---

**Remember**: This is ALPHA software. The goal is honest testing and steady improvement.

**Status**: Testing framework solid. Keep building! 🚀

*Last Updated: June 2026*
```bash
pytest tests/integration/test_full_pipeline.py::TestFullPipeline::test_error_handling_invalid_path -v
```

### Test Small Dataset
```bash
pytest tests/integration/test_full_pipeline.py::TestPipelineRobustness::test_small_dataset -v
```

---

## 💡 Troubleshooting

### "Module not found" errors
```bash
pip install -r requirements.txt
pip install -e .
```

### "No tests ran"
```bash
# Make sure you're in the project root
cd /workspaces/ml_backend

# Check test files exist
ls tests/unit/
ls tests/integration/
```

### Tests timeout
```bash
# Increase timeout
pytest tests/integration/ -v --timeout=600
```

### Coverage not generated
```bash
# Install coverage
pip install pytest-cov

# Run with explicit coverage
pytest --cov=src --cov-report=html
```

---

## 📝 Files Changed

### New Files
- `src/utils/error_handling.py` - Retry logic, circuit breakers
- `src/utils/metrics.py` - Prometheus metrics
- `tests/unit/test_collapse_detector.py` - Real unit tests
- `tests/unit/test_diversity_analyzer.py` - Real unit tests
- `tests/integration/test_full_pipeline.py` - Integration tests
- `tests/load/test_load.py` - Load testing framework
- `.ruff.toml` - Code quality config
- `pyproject.toml` - Package config
- `PRODUCTION_IMPROVEMENTS.md` - Detailed changelog
- `run_tests.sh` - Quick test runner

### Updated Files
- `README.md` - Honest status, removed false claims
- `docs/ARCHITECTURE.md` - Realistic assessment
- `requirements.txt` - Fixed dependencies
- `.github/workflows/ci.yml` - Comprehensive CI/CD
- `src/orchestrator.py` - Added error handling

### Removed Files
- `src/model_architectures.py.bak` - Deleted

---

## ✅ Success Criteria

Your improvements are working if:
1. ✅ Tests run and most pass
2. ✅ Coverage report generates
3. ✅ README says "Alpha" not "Production Ready"
4. ✅ No .bak files in repo
5. ✅ Linting runs (even if issues found)
6. ✅ CI/CD pipeline defined
7. ✅ Load test can run
8. ✅ Error handling tested

---

## 📞 Need Help?

1. **Check**: `PRODUCTION_IMPROVEMENTS.md` for detailed info
2. **Review**: Test output for specific error messages
3. **Run**: `pytest -v` for verbose test output
4. **Check**: `htmlcov/index.html` for coverage details

---

**Remember**: This is ALPHA software. The goal is honesty and steady improvement, not perfection overnight.

**Status**: Foundation is solid. Keep building! 🚀
