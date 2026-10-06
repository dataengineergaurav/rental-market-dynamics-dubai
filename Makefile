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

# Weekly: 7-day Mon-Sun pooled. Builds both layer DuckDBs (Silver tables + Gold views)
weekly:
	@echo "Building weekly layers (previous complete ISO week)..."
	@WEEK=$$(python3 -c "from datetime import date,timedelta; d=date.today()-timedelta(days=7); y,w,_=d.isocalendar(); print(f'{y}W{w:02d}')"); \
	echo "Week $$WEEK"; \
	uv run python -m lib.analysis.build_weekly_duckdb --week $$WEEK

weekly-publish: weekly
	@echo "Publishing weekly Silver and Gold DuckDBs to GitHub Releases..."
	@test -n "$(WEEK)" || (echo "Usage: make weekly-publish WEEK=2026W37" && exit 1)
	uv run python -m lib.workspace.publish_layers --week $(WEEK) --silver output/silver_$(WEEK).duckdb --gold output/gold_$(WEEK).duckdb

# Scrapy rents (paginated, slow ~10s/page) — must run inside rents_scraper/
scrapy-rents:
	@echo "Scraping rents via Scrapy (Ejari, slow ~10s/page)..."
	cd rents_scraper && uv run scrapy crawl rents -O ../output/rents.jsonl --nolog || (cd rents_scraper && scrapy crawl rents -O ../output/rents.jsonl)

# Test: Run all tests using pytest
test:
	@echo "Running tests..."
	pytest .

# Declare phony targets to avoid conflicts with files
.PHONY: all help clean build etl test scrapy-rents
