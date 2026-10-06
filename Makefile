# Define directories
SOURCE_DIR := lib
TEST_DIR := tests
BUILD_DIR := build
TEMP_DIR := output

# Default target: run build, ETL, and tests
all: build etl test 

# Help target: Lists all available commands
help:
	@echo "Available targets:"
	@echo "  all      - Build, run ETL, and execute tests"
	@echo "  build    - Install dependencies and build the project"
	@echo "  etl      - Execute the Ejari rents ETL process"
	@echo "  scrapy-rents - Scrapy rents Ejari (slow)"
	@echo "  notebook - Execute the Jupyter Notebook"
	@echo "  test     - Run tests"
	@echo "  layers   - Ingest today's CSV into the cumulative layers DuckDB"
	@echo "  layers-publish - Publish the layers DuckDB into the day's release"
	@echo "  clean    - Clean build artifacts and temporary files"

# Clean: Remove build artifacts, temporary files, and caches
clean:
	@echo "Cleaning project..."
	rm -rf $(BUILD_DIR)/*
	rm -rf $(SOURCE_DIR)/*.egg-info
	rm -rf $(SOURCE_DIR)/__pycache__
	rm -rf $(TEST_DIR)/__pycache__
	find . -type f -name '*.pyc' -delete
	find . -type f -name '*.pyo' -delete
	find . -type f -name '*~' -delete
	rm -rf $(TEMP_DIR)/*.csv
	rm -rf $(TEMP_DIR)/*.parquet

# Build: Set up the environment and install dependencies
build:
	@echo "Building project..."
	python --version
	pip install --upgrade pip
	pip install -r requirements.txt
	pip install lib/

# ETL: Ejari rents (EJARI_URL)
etl:
	@echo "Running Ejari rents ETL process..."
	python run_etl_pipeline.py

# Layers: ingest today's daily CSV into the cumulative combined DuckDB (Silver tables + Gold views)
layers:
	@echo "Ingesting today's contracts into the cumulative layers DuckDB..."
	@CSV=$$(ls -t output/rent_contracts_*.csv 2>/dev/null | head -1); \
	test -n "$$CSV" || (echo "No output/rent_contracts_*.csv to ingest; run 'make etl' first" && exit 1); \
	echo "Ingesting $$CSV"; \
	uv run python -m lib.analysis.build_layers_duckdb --db output/rents_layers.duckdb --csv "$$CSV"

layers-publish:
	@echo "Publishing the combined layers DuckDB into the day's release..."
	@DATA_THROUGH=$$(uv run python -c "import duckdb; print(duckdb.connect('output/rents_layers.duckdb', read_only=True).execute('SELECT data_through FROM _meta').fetchone()[0])"); \
	uv run python -m lib.workspace.publish_layers --artifact output/rents_layers.duckdb --data-through "$$DATA_THROUGH"

# Scrapy rents (paginated, slow ~10s/page) — must run inside rents_scraper/
scrapy-rents:
	@echo "Scraping rents via Scrapy (Ejari, slow ~10s/page)..."
	cd rents_scraper && uv run scrapy crawl rents -O ../output/rents.jsonl --nolog || (cd rents_scraper && scrapy crawl rents -O ../output/rents.jsonl)

# Test: Run all tests using pytest
test:
	@echo "Running tests..."
	pytest .

# Declare phony targets to avoid conflicts with files
.PHONY: all help clean build etl test scrapy-rents layers layers-publish
