# DEApython Project Setup Guide

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](SETUP.pt-BR.md)

This guide provides step-by-step instructions to configure the project environment and run the data pipeline.

## Table of Contents
- [Pipeline architecture](#pipeline-architecture)
- [Quick Start](#quick-start-tldr)
- [System requirements](#system-requirements)
- [Required accounts](#required-accounts)
- [Environment variables](#environment-variables)
- [Run the pipeline](#run-the-pipeline)
- [Expected project structure](#expected-project-structure)
- [Data visualization](#data-visualization)

## Pipeline architecture
- Data pipeline flow:
    Raw Data → Bronze → Silver → Gold → Warehouse → BI
- Full project diagram:
    [View full diagram](project_diagram.md)

## Quick Start (TL;DR)
- Clone the repository and run the pipeline:
```
    git clone https://github.com/puffdapaz/DEApython.git
    cd DEApython
    pip install uv
    uv venv
# Windows:
    .venv\Scripts\activate
# macOS:
    source .venv/bin/activate

    uv pip install -r requirements.txt
    cp .env.example .env
    python main.py
```
**For full instructions see the sections below.**

## System requirements
- Install the tools below:
### Git
- **Windows**:
    https://git-scm.com/
- **macOS**:
```
    brew install git
```
- **Linux**:
```
    sudo apt install git
```
- Verify:
```
    git --version
```

### Python 3.9+
- Install Python:
    https://www.python.org/downloads/
- During installation on Windows check:
    Add Python to PATH
- Verify:
```
    python --version
    pip --version
```

### UV (dependency manager)
- Install:
```
    pip install uv
```
- Verify:
```
    uv --version
```

### Docker Desktop
Required to run **OpenMetadata**.
- Download:
    https://www.docker.com/products/docker-desktop/
- Verify:
```
    docker --version
```

### Windows - WSL2 Configuration
Docker Desktop on Windows requires WSL2 for proper performance.
- Enable WSL2:
```
    wsl --install
```
- More information:
    https://learn.microsoft.com/windows/wsl/install

## Required accounts
The project uses external services.

### Basedosdados
- Access to the Brazilian public data lake.
https://basedosdados.org/
- Documentation:
https://docs.basedosdados.org/

### Google Cloud Platform
Used for storage in **Google Cloud Storage**.
    https://cloud.google.com/
You will need to:
    - Create a project;
    - Create a GCS bucket;
    - Create a service account;
    - Download the JSON key and save it as `credentials/deapython_gcp_key.json`.

### Neon Postgres
Used as the **Data Warehouse**.
    https://neon.tech/
- Copy the database connection string.

## Environment variables
- Create the `.env` file.
```
billing_project_id = "yourproject"
gcp_bucket_name = "deapython"
google_application_credentials = credentials/deapython_gcp_key.json

NEON_USER = "neondb_owner"
NEON_PASSWORD = "password"
NEON_HOST = "xx-xxxxx-xxx-########-xxxxx.c-2.us-region-1.aws.neon.tech"
NEON_PORT = "port"
NEON_DATABASE = "neondb"

OPENMETADATA_JWT_TOKEN = "yourtoken"
OPENMETADATA_HOST = "http://###.##.###.##:8585/api"

Optional (Power BI)
POWERBI_CLIENT_ID = "client_hash"
POWERBI_CLIENT_SECRET = "client_hash"
POWERBI_TENANT_ID = "client_hash"
```

### Run and obtain OpenMetadata JWT token
- Start the metadata catalog:
```
    docker compose up -d
```
- Interface available at:
    http://localhost:8585
- Login:
    admin@open-metadata.org / admin
- Go to **Settings → Bots → ingestion-bot**
    click **Generate new token** and copy it to the `.env` file.

## Run the pipeline
```
    python main.py
```

## Expected project structure
```
DEApython/
├── configs/                  # YAML configuration files
├── credentials/              # gcp_key.json (must be created)
├── data/                     # Processed data (generated automatically)
│   ├── raw/                  # Raw data
│   └── processed/            # Processed data
│       ├── gold/             # Modeled data and results
│       └── silver/           # Aggregated data
├── docs/                     # Original article
├── etl/                      # ETL pipeline code
│   ├── diagnostics/          # Validation and diagnostics
│   ├── save_utils/           # Storage and publishing utilities
│   ├── bronze_ingestion.py   # Ingestion workflow
│   ├── silver_processing.py  # Processing workflow
│   ├── gold_modeling.py      # Modeling workflow
│   ├── dash_metrics.py       # Metrics workflow
│   └── geodata.py            # Validation and diagnostic functions
├── openmetadata/             # Metadata catalog configuration
├── .env                      # Environment variables (must be created)
├── .gitignore                # Repository ignore rules
├── CITATION.cff              # Citation information
├── main.py                   # Project entry point
├── project_diagram.md        # Pipeline diagram
├── pyproject.toml            # Project configuration
├── README.en-US.md           # Project description (en_US)
├── README.md                 # Language selector
├── README.pt-BR.md           # Project description (pt_BR)
├── requirements.txt          # Project dependencies
├── SETUP.en-US.md            # Setup instructions (en_US)
├── SETUP.md                  # Language selector
└── SETUP.pt-BR.md            # Setup instructions (pt_BR)
```

## Data visualization
Final datasets are loaded into **Neon Postgres**.
- Compatible tools:
    - Power BI
    - Metabase
    - Tableau
    - DBeaver
    - pgAdmin

See the [README](README.en-US.md) for additional information and support. Feel free to ask questions if needed.