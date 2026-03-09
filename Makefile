.PHONY: help install test lint run clean docker-up docker-down

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

test: ## Run test suite
	python -m pytest tests/ -v --tb=short --timeout=30

test-cov: ## Run tests with coverage report
	python -m pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html --timeout=30

lint: ## Run code quality checks
	python -m py_compile main.py
	python -m py_compile src/stream/processor.py
	python -m py_compile src/stream/source.py
	python -m py_compile src/stream/sink.py
	python -m py_compile src/features/feature_pipeline.py
	python -m py_compile src/features/aggregations.py
	python -m py_compile src/features/cep.py
	python -m py_compile src/store/feature_store.py
	python -m py_compile src/monitoring/metrics.py

run: ## Run the demo pipeline
	python main.py

clean: ## Remove build artifacts and caches
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache htmlcov .coverage dist build *.egg-info

docker-up: ## Start infrastructure services
	docker-compose -f docker/docker-compose.yml up -d

docker-down: ## Stop infrastructure services
	docker-compose -f docker/docker-compose.yml down -v
