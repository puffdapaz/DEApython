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
- **Important**: When installing, check the option **"Add python to PATH"**<br/>
- Check installation after completing:<br/>
In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`bash`<br/>
`python --version`<br/>
`pip --version`<br/>

### 3. Clone the repository
- In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`bash`<br/>
`git clone https://github.com/puffdapaz/DEApython.git`<br/>
`cd DEApython`<br/>

### 4. Set up a virtual environment
- In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
Navigate to your project folder:<br/>
`cd address/directory/folder/`<br/>
Create a virtual environment called venv:<br/>
`python -m venv venv` <br/>
Activate the virtual environment: <br/>
On Windows: `.\venv\Scripts\activate`<br/>
On macOS/Linux: `source venv/bin/activate`<br/>

### 5. Configure Environment Variables
- Create `.env` file in root directory, containing parameters for:<br/>
GCS account for storage: billing_project_id=your_gcp_project<br/>
GCS bucket for storage: gcp_bucket_name=your_gcs_bucket<br/>
Reference for credentials: google_application_credentials=credentials/gcp_key.json<br/>

### 6. Install dependencies
- In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`pip install .`<br/>
or<br/>
`pip install -r requirements.txt`<br/>

### 7. Running the code
- In command prompt (Windows) or Terminal (macOS/Linux) type:<br/>
`python main.py`<br/>

### 8. Expected Project Structure
After successful execution, your folder should contain:<br/>

DEApython/<br/>
├── configs/                  # YAML files with parameters<br/>
├── credentials/              # gcp_key.json (need to create)<br/>
├── docs/                     # Original paper<br/>
├── data/                     # Organized data (automatically generated)<br/>
│   ├── bronze/               # Raw data<br/>
│   ├── silver/               # Processed data<br/>
│   └── gold/                 # Final results<br/>
├── etl/                      # ETL pipeline code<br/>
│   ├── bronze_ingestion.py   # Ingestion Flow<br/>
│   ├── silver_processing.py  # Processing Flow<br/>
│   ├── gold_modeling.py      # Modeling Flow<br/>
│   ├── dash_metrics.py       # Metrics Flow<br/>
│   ├── diagnostics/          # Validation and Diagnostic Functions<br/>
│   └── save_utils/           # Storage Functions<br/>
├── .env                      # Environment Variables (need to create)<br/>
├── .gitignore                # Repository file filter<br/>
├── CITATION.cff              # Citations and Credits<br/>
├── main.py                   # Project entrypoint<br/>
├── project_diagram.md        # Project diagram flow<br/>
├── pyproject.toml            # Premises and conditions<br/>
├── README.en-US.md           # Project Description (en_US)<br/>
├── README.md                 # Language option for Description<br/>
├── README.pt-BR.md           # Project Description (pt_BR)<br/>
├── requirements.txt          # Project dependencies<br/>
├── SETUP.en-US.md            # Installation Instructions (en_US)<br/>
├── SETUP.md                  # Language option for Instructions<br/>
└── SETUP.pt-BR.md            # Installation Instructions (pt_BR)<br/>

There is [README](https://github.com/puffdapaz/DEApython/blob/main/README.en-US.md) file with additional support. Do not hesitate to ask for help.<br/>