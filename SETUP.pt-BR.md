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
- **Importante**: Durante a instalação, marque a opção **"Add python to PATH"**
- Verifique a instalação após concluir:<br/>
  `bash
  python --version
  pip --version`

### 3. Clonar o repositório
No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
  `bash
  git clone https://github.com/puffdapaz/DEApython.git
  cd DEApython`

### 4. Configurar um ambiente virtual
Abra o prompt de comando (Windows) ou Terminal (macOS/Linux). <br/>
Navegue até a pasta do seu projeto: cd endereço/diretório/pasta/ <br/>
Crie um ambiente virtual chamado venv: **_python -m venv venv_** <br/>
Ative o ambiente virtual: <br/>
    No Windows: **_.\venv\Scripts\activate_**<br/>
    No macOS/Linux: **_source venv/bin/activate_**<br/>

### 5. Configurar Variáveis de Ambiente
Criação de arquivo `.env` no diretório raiz,contendo parâmetros para:
  Conta GCS para armazenamento: billing_project_id=seu_projeto_no_gcp
  Bucket GCS para armazenamento: gcp_bucket_name=seu_bucket_gcs
  Referência para credenciais: google_application_credentials=credentials/gcp_key.json

### 6. Instalação das dependências
No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`pip install .`<br/>
ou `pip install -r requirements.txt`

### 7. Executando o código
No prompt de comando (Windows) ou Terminal (macOS/Linux) digite:<br/>
`python main.py`<br/>

### 8. Estrutura Esperada do Projeto
Após a execução bem-sucedida, sua pasta deve conter:

DEApython/
├── configs/                  # Arquivos YAML com parâmetros
├── credentials/              # gcp_key.json (necessario criar)
├── docs/                     # Artigo original
├── data/                     # Dados tratados (gerados automaticamente)
│   ├── bronze/               # Dados brutos
│   ├── silver/               # Dados processados  
│   └── gold/                 # Resultados finais
├── etl/                      # Código do pipeline ETL
│   ├── bronze_ingestion.py   # Fluxo de Ingestão
│   ├── silver_processing.py  # Fluxo de Processamento
│   ├── gold_modeling.py      # Fluxo de Modelagem
│   ├── dash_metrics.py       # Fluxo de Métricas
│   ├── diagnostics/          # Funções de Validação e Diagnóstico
│   └── save_utils/           # Funções de Armazenamento
├── .env                      # Variáveis de Ambiente (necessário criar)
├── .gitignore                # Filtro de arquivos repositório
├── CITATION.cff              # Citações e Créditos
├── main.py                   # Ponto de entrada do projeto
├── project_diagram.md        # Diagrama do fluxo do projeto
├── pyproject.toml            # Premissas e condições
├── README.en-US.md           # Descrição do Projeto (en_US)
├── README.md                 # Opção para linguagem da Descrição
├── README.pt-BR.md           # Descrição do Projeto (pt_BR)
├── requirements.txt          # Dependências do projeto
├── SETUP.en-US.md            # Instruções para Instalação (en_US)
├── SETUP.md                  # Opção para linguagem da Instruções
└── SETUP.pt-BR.md            # Instruções para Instalação (pt_BR)

Há arquivo [README](https://github.com/puffdapaz/DEApython/blob/main/README.pt-BR.md) com suporte adicional. Não hesite em pedir ajuda.<br/>