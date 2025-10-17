# Project Setup Guide

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/puffdapaz/DEApython/blob/main/SETUP.pt-BR.md)

### This guide provides step-by-step instructions to set up the project environment and run the data pipeline.

## Prerequisites

### 1. Install Git
- **Windows**: Download at [git-scm.com](https://git-scm.com/)<br/>
- **macOS**: Already included or install via Homebrew: `brew install git`<br/>
- **Linux**: `sudo apt install git` (Ubuntu/Debian)<br/>

### 2. Install python 3.9 or newer
- Download at official site: [python.org/downloads](https://www.python.org/downloads/)<br/>
- **Important**: When installing, check the option **"Add python to PATH"**
- Check installation after completing:<br/>
  `bash
  python --version
  pip --version`

### 3. Clone the repository
In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
  `bash
  git clone https://github.com/puffdapaz/DEApython.git
  cd DEApython`

### 4. Set up a virtual environment
Open command prompt (Windows) or Terminal (macOS/Linux). <br/>
Navigate to your project folder: cd address/directory/folder/ <br/>
Create a virtual environment called venv: **_python -m venv venv_** <br/>
Activate the virtual environment: <br/>
    On Windows: **_.\venv\Scripts\activate_**<br/>
    On macOS/Linux: **_source venv/bin/activate_**<br/>

### 5. Configure Environment Variables
Create `.env` file in root directory, containing parameters for:
  GCS account for storage: billing_project_id=your_gcp_project
  GCS bucket for storage: gcp_bucket_name=your_gcs_bucket
  Reference for credentials: google_application_credentials=credentials/gcp_key.json

### 6. Install dependencies
In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`pip install .`<br/>
or `pip install -r requirements.txt`

### 7. Running the code
In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`python main.py`<br/>

### 8. Expected Project Structure
After successful execution, your folder should contain:

DEApython/
├── configs/                  # YAML files with parameters
├── credentials/              # gcp_key.json (need to create)
├── docs/                     # Original paper
├── data/                     # Organized data (automatically generated)
│   ├── bronze/               # Raw data
│   ├── silver/               # Processed data  
│   └── gold/                 # Final results
├── etl/                      # ETL pipeline code
│   ├── bronze_ingestion.py   # Ingestion Flow
│   ├── silver_processing.py  # Processing Flow
│   ├── gold_modeling.py      # Modeling Flow
│   ├── dash_metrics.py       # Metrics Flow
│   ├── diagnostics/          # Validation and Diagnostic Functions
│   └── save_utils/           # Storage Functions
├── .env                      # Environment Variables (need to create)
├── .gitignore                # Repository file filter
├── CITATION.cff              # Citations and Credits
├── main.py                   # Project entrypoint
├── project_diagram.md        # Project diagram flow
├── pyproject.toml            # Premises and conditions
├── README.en-US.md           # Project Description (en_US)
├── README.md                 # Language option for Description
├── README.pt-BR.md           # Project Description (pt_BR)
├── requirements.txt          # Project dependencies
├── SETUP.en-US.md            # Installation Instructions (en_US)
├── SETUP.md                  # Language option for Instructions
└── SETUP.pt-BR.md            # Installation Instructions (pt_BR)

There is [README](https://github.com/puffdapaz/DEApython/blob/main/README.en-US.md) file with additional support. Do not hesitate to ask for help.<br/>