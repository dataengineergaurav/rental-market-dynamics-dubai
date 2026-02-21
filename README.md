# Rental Market Dynamics - Dubai

![Build Status](https://img.shields.io/github/actions/workflow/status/ggurjar333/rental-market-dynamics-dubai/build_and_deploy.yml?branch=main)
![License](https://img.shields.io/github/license/ggurjar333/rental-market-dynamics-dubai)
![Coverage](https://img.shields.io/codecov/c/github/ggurjar333/rental-market-dynamics-dubai)

This repository contains the code and rent contracts data for analyzing real estate properties in Dubai. The project automates the extraction, transformation, and analysis of rent contracts, providing insights into property usage.

## Features

- **Automated Data Extraction:** Retrieve rent contracts from the Dubai Land Department.
- **Data Transformation:** Convert CSV data to Parquet for optimized querying.
- **Property Usage Analysis:** Generate detailed property usage reports.
- **Automated Releases:** Publish processed data via GitHub releases.
- **CI/CD Integration:** Built-in workflows to test and deploy changes.

## Prerequisites

- **Python:** 3.9 or higher
- **Make:** To execute build commands
- **pip:** Python package installer

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/ggurjar333/rental-market-dynamics-dubai
   cd rental-market-dynamics-dubai

2. **Set up a virtual environment**
    ```bash
    python -m venv .venv
    source venv/bin/activate
    ```

3. **Install dependencies**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    make build
    ```

4. **Create a ``.env`` file**
    
    Copy the provided example and update the values:
    ```bash
    cp .env.example .env
    ```

## Folder Structure
```bash
.
├── .github
│   ├── workflows
│   │   ├── build_and_deploy.yml
│   │   └── cron.yml
│   └── dependabot.yml
├── docs
│   └── architecture.md
├── lib
│   ├── extract
│   ├── transform
│   ├── classes
│   ├── workspace
│   ├── assets
│   ├── logging_helpers.py
│   └── __init__.py
├── output
├── tests
├── .env.example
├── CHANGELOG.md
├── CONTRIBUTING.md
├── Makefile
├── README.md
└── requirements.txt
```

## Getting Started
- **ETL Pipeline:** 
    
    Run the complete pipeline (build, ETL, tests, and release publishing) with:
    ```bash
    make all
    ```
- **Testing:** 
    
    Run tests using:
    ```bash
    make test
    ```

## Usage Examples
- **Downloading & Transforming Data**:
    
    The ETL process downloads rent contracts, transforms the data into Parquet format, and generates a property usage report. Logs are saved in ``etl.log``.
    ```bash
    python run_etl_pipeline.py
    ```

- **Publishing Releases**:

    On successful processing, the data files are automatically published to a GitHub release.


## Historical Data
   Download the historical data from [releases](https://github.com/dataengineergaurav/rental-market-dynamics-dubai/releases

## Contributing
Contributions are welcome! Please review the [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Licenses
This project is licensed under the terms of the [MIT License](https://mit-license.org/).

## Changelog
Refer to [CHANGELOG.md](changelog.md) for a complete history of changes.

## Contact
For questions or feedback, please open an issue on GitHub.




