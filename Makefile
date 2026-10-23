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
	@echo "  lint     - Lint and format-check with ruff"
	@echo "  fmt      - Format and lint-fix with ruff (write changes)"
	@echo "  coverage - Run tests with a coverage floor"
	@echo "  check    - Lint + coverage (the CI quality gate)"
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
	uv --version
	uv sync

# ETL: Ejari rents (EJARI_URL)
etl:
	@echo "Running Ejari rents ETL process..."
	uv run python run_etl_pipeline.py

# Weekly: 7-day Mon-Sun pooled DuckDB from Releases daily CSVs
weekly:
	@echo "Building weekly DuckDB (previous complete ISO week)..."
	@WEEK=$$(python3 -c "from datetime import date,timedelta; d=date.today()-timedelta(days=7); y,w,_=d.isocalendar(); print(f'{y}W{w:02d}')"); \
	echo "Week $$WEEK"; \
	uv run python -m lib.analysis.build_weekly_duckdb --week $$WEEK

weekly-publish: weekly
	@echo "Publishing weekly DuckDB to GitHub Release (weekly tag)..."
	@test -n "$(WEEK)" || (echo "Usage: make weekly-publish WEEK=2026W37" && exit 1)
	gh release view release-week-$(WEEK) --repo dataengineergaurav/rental-market-dynamics-dubai >/dev/null 2>&1 || gh release create release-week-$(WEEK) --repo dataengineergaurav/rental-market-dynamics-dubai --title "Weekly $(WEEK)" --notes "Weekly DuckDB $(WEEK) — 7d pooled enriched fact + gold views" --latest=false
	gh release upload release-week-$(WEEK) output/rental_analytics_weekly_$(WEEK).duckdb --repo dataengineergaurav/rental-market-dynamics-dubai --clobber

# Scrapy rents (paginated, slow ~10s/page) — must run inside rents_scraper/
scrapy-rents:
	@echo "Scraping rents via Scrapy (Ejari, slow ~10s/page)..."
	cd rents_scraper && uv run scrapy crawl rents -O ../output/rents.jsonl --nolog

# Test: Run all tests using pytest
test:
	@echo "Running tests..."
	uv run pytest .

# Lint: ruff checks (undefined names, unused imports, syntax, real-bug lints) and formatting.
lint:
	@echo "Linting with ruff..."
	uv run ruff check .
	uv run ruff format --check .

# Fmt: write formatting + lint fixes (used by the save hook).
fmt:
	@echo "Formatting and lint-fixing with ruff..."
	uv run ruff format .
	uv run ruff check --fix .

# Coverage: tests with a floor that ratchets (raise it as coverage improves, never lower to pass).
COVERAGE_FLOOR := 75
coverage:
	@echo "Running tests with coverage (floor $(COVERAGE_FLOOR))..."
	uv run pytest --cov=lib --cov-report=term-missing --cov-fail-under=$(COVERAGE_FLOOR)

# Check: the full CI quality gate.
check: lint coverage

# Declare phony targets to avoid conflicts with files
.PHONY: all help clean build etl test lint fmt coverage check scrapy-rents weekly weekly-publish
