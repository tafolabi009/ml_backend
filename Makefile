.PHONY: test lint docker-build

PYTHON=python3

test:
	@echo "Running tests..."
	@$(PYTHON) -m pytest -q

lint:
	@echo "Running lint (ruff)..."
	@ruff check .

docker-build:
	@echo "Building Docker image 'ml_backend:local'..."
	docker build -t ml_backend:local .
