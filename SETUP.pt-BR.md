# Guia de configuração para replicar o Projeto

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](https://github.com/puffdapaz/DEApython/blob/main/SETUP.en-US.md)

### Este guia fornece instruções passo a passo para configurar o ambiente do projeto e executar o pipeline de dados.

## Pré-requisitos

### 1. Instalar Git
- **Windows**: Baixe em [git-scm.com](https://git-scm.com/)<br/>
- **macOS**: Já incluso ou instale via Homebrew: `brew install git`<br/>
- **Linux**: `sudo apt install git` (Ubuntu/Debian)<br/>

### 2. Instalar python 3.9 ou superior
- Baixe no site oficial: [python.org/downloads](https://www.python.org/downloads/)<br/>
- **Importante**: Durante a instalação, marque a opção **"Add python to PATH"**<br/>
- Verifique a instalação após concluir:<br/>
No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`bash`<br/>
`python --version`<br/>
`pip --version`<br/>

### 3. Clonar o repositório
- No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`bash`<br/>
`git clone https://github.com/puffdapaz/DEApython.git`<br/>
`cd DEApython`<br/>

### 4. Configurar um ambiente virtual
- No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
Navegue até a pasta do seu projeto:<br/>
`cd endereço/diretório/pasta/`<br/>
Crie um ambiente virtual chamado venv:<br/>
`python -m venv venv`<br/>
Ative o ambiente virtual: <br/>
No Windows: `.\venv\Scripts\activate`<br/>
No macOS/Linux: `source venv/bin/activate`<br/>

### 5. Configurar Variáveis de Ambiente
- Criação de arquivo `.env` no diretório raiz,contendo parâmetros para:<br/>
Conta GCS para armazenamento: billing_project_id=seu_projeto_no_gcp<br/>
Bucket GCS para armazenamento: gcp_bucket_name=seu_bucket_gcs<br/>
Referência para credenciais: google_application_credentials=credentials/gcp_key.json<br/>

### 6. Instalação das dependências
- No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`pip install .`<br/>
ou<br/>
`pip install -r requirements.txt`<br/>

### 7. Executando o código
- No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`python main.py`<br/>

### 8. Estrutura Esperada do Projeto
Após a execução bem-sucedida, sua pasta deve conter:<br/>

DEApython/<br/>
├── configs/                  # Arquivos YAML com parâmetros<br/>
├── credentials/              # gcp_key.json (necessario criar)<br/>
├── docs/                     # Artigo original<br/>
├── data/                     # Dados tratados (gerados automaticamente)<br/>
│   ├── bronze/               # Dados brutos<br/>
│   ├── silver/               # Dados processados<br/>
│   └── gold/                 # Resultados finais<br/>
├── etl/                      # Código do pipeline ETL<br/>
│   ├── bronze_ingestion.py   # Fluxo de Ingestão<br/>
│   ├── silver_processing.py  # Fluxo de Processamento<br/>
│   ├── gold_modeling.py      # Fluxo de Modelagem<br/>
│   ├── dash_metrics.py       # Fluxo de Métricas<br/>
│   ├── diagnostics/          # Funções de Validação e Diagnóstico<br/>
│   └── save_utils/           # Funções de Armazenamento<br/>
├── .env                      # Variáveis de Ambiente (necessário criar)<br/>
├── .gitignore                # Filtro de arquivos repositório<br/>
├── CITATION.cff              # Citações e Créditos<br/>
├── main.py                   # Ponto de entrada do projeto<br/>
├── project_diagram.md        # Diagrama do fluxo do projeto<br/>
├── pyproject.toml            # Premissas e condições<br/>
├── README.en-US.md           # Descrição do Projeto (en_US)<br/>
├── README.md                 # Opção para linguagem da Descrição<br/>
├── README.pt-BR.md           # Descrição do Projeto (pt_BR)<br/>
├── requirements.txt          # Dependências do projeto<br/>
├── SETUP.en-US.md            # Instruções para Instalação (en_US)<br/>
├── SETUP.md                  # Opção para linguagem da Instruções<br/>
└── SETUP.pt-BR.md            # Instruções para Instalação (pt_BR)<br/>

Há arquivo [README](https://github.com/puffdapaz/DEApython/blob/main/README.pt-BR.md) com suporte adicional. Não hesite em pedir ajuda.<br/>