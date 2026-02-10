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
	@echo "  etl      - Execute the ETL process"
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

# ETL: Run the extraction, transformation, and loading process
etl:
	@echo "Running ETL process..."
	python run_etl_pipeline.py

# Test: Run all tests using pytest
test:
	@echo "Running tests..."
	pytest .

# Declare phony targets to avoid conflicts with files
.PHONY: all help clean build etl test
