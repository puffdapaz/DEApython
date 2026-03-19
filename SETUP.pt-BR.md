# Guia de configuração para replicar o Projeto DEApython

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](SETUP.en-US.md)

Este guia fornece instruções passo a passo para configurar o ambiente do projeto e executar o pipeline de dados.

## Sumário
- [Arquitetura do pipeline](#arquitetura-do-pipeline)
- [Inicialização rápida](#inicialização-rápida-tldr)
- [Requisitos do sistema](#requisitos-do-sistema)
- [Contas necessárias](#contas-necessárias)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Executar pipeline](#executar-pipeline)
- [Estrutura esperada](#estrutura-esperada)
- [Visualização de Dados](#visualização-de-dados)

## Arquitetura do pipeline
- Fluxo do pipeline de dados:
    Dados Brutos → Bronze → Prata → Ouro → Warehouse → BI
- Diagrama completo do projeto:
    [Ver diagrama completo](project_diagram.md)

## Inicialização rápida (TL;DR)
- Clone o repositório e execute o pipeline:
```
    git clone https://github.com/puffdapaz/DEApython.git
    cd DEApython
    pip install uv
    uv venv
    uv sync
    cp .env.example .env
    python main.py
```
**Para instruções completas veja as seções abaixo.**

## Requisitos do sistema
- Instale as ferramentas abaixo:
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
- Verificar:
```
    git --version
```

### Python 3.11+
- Instale python:
    https://www.python.org/downloads/
- Durante instalação no Windows marque:
    Add Python to PATH
- Verificar:
```
    python --version
    pip --version
```

### UV (gerenciador de dependências)
- Instalar:
```
    pip install uv
```
- Verificar:
```
    uv --version
```

### Docker Desktop
Necessário para executar **OpenMetadata**.
- Download:
    https://www.docker.com/products/docker-desktop/
- Verificar:
```
    docker --version
```

### Windows - Configuração do WSL2
O Docker Desktop no Windows requer WSL2 para performance adequada.
- Ative o WSL2:
```
    wsl --install
```
- Mais informações:
    https://learn.microsoft.com/pt-br/windows/wsl/install

## Contas necessárias
O projeto utiliza serviços externos.

### Basedosdados
- Acesso ao datalake com dados públicos do Brasil.
    https://basedosdados.org/
- Documentação:
    https://docs.basedosdados.org/

### Google Cloud Platform
Usado para armazenamento no **Google Cloud Storage**.
    https://cloud.google.com/
Você precisará:
  - Criar um projeto;
  - Criar um bucket GCS;
  - Criar uma service account;
  - Baixar a chave JSON e salvar como `credentials/deapython_gcp_key.json`.

### Neon Postgres
Usado como **Data Warehouse**.
    https://neon.tech/
- Copie a connection string do banco.

## Variáveis de ambiente
- Edit o arquivo `.env`.
```
    billing_project_id = "seuprojeto"
    gcp_bucket_name = "deapython"
    google_application_credentials = credentials/deapython_gcp_key.json

    NEON_USER = "neondb_owner"
    NEON_PASSWORD = "senha"
    NEON_HOST = "xx-xxxxx-xxx-########-xxxxx.c-2.us-region-1.aws.neon.tech"
    NEON_PORT = "porta"
    NEON_DATABASE = "neondb"

    OPENMETADATA_JWT_TOKEN = "seutoken"
    OPENMETADATA_HOST = "http://###.##.###.##:8585/api"

    Opcionais (Power BI)
    POWERBI_CLIENT_ID = "client_hash"
    POWERBI_CLIENT_SECRET = "client_hash"
    POWERBI_TENANT_ID = "client_hash"
```

### Executar e obter token JWT do OpenMetadata 
- Importante: Resolução de Conflito em Portas

O serviço de ingestão do OpenMetadata usa a porta **8080**, que pode conflitar com outros serviços (como chamadas OAuth) no seu ambiente. Caso encontre erros com a porta 8080 ocupada, siga esses passos:

#### Mudar a porta do Airflow
**Interrompa os containers do OpenMetadata**:
   ```
   docker-compose -f docker-compose-postgres.yml down
   ```

**Modifique o arquivo docker-compose**:
Abra e edite docker-compose-postgres.yml, encontre o serviço de ingestão e modifique o endereço das portas de:

```
ports:
  - "8080:8080"
```
para:
```
ports:
  - "8090:8080"
```

e

```
environment:
  - PIPELINE_SERVICE_CLIENT_ENDPOINT=http://ingestion:8080
```
para:
```
environment:
  - PIPELINE_SERVICE_CLIENT_ENDPOINT=http://ingestion:8090
```

Reinicie os containers e verifique as modificações:

Acesse Airflow em: http://localhost:8090

Acesse OpenMetadata em: http://localhost:8585

- Login: 
    admin@open-metadata.org / admin
- Vá em **Settings** → **Bots** → **ingestion-bot**
    clique em **Generate new token** e copie no arquivo .env

## Executar pipeline
```
    python main.py
```

## Estrutura esperada

```
DEApython/
├── configs/                  # Arquivos YAML com parâmetros
├── credentials/              # gcp_key.json (necessario criar)
├── data/                     # Dados tratados (gerados automaticamente)
│   ├── raw/                  # Dados brutos
│   └── processed/            # Dados processados
│       ├── gold/             # Dados modelados e resultados
│       └── silver/           # Dados agregados
├── docs/                     # Artigo original
├── etl/                      # Código do pipeline ETL
│   ├── diagnostics/          # Validação e diagnósticos
│   ├── save_utils/           # Armazenagem e disponibilização
│   ├── bronze_ingestion.py   # Fluxo de Ingestão
│   ├── silver_processing.py  # Fluxo de Processamento
│   ├── gold_modeling.py      # Fluxo de Modelagem
│   ├── dash_metrics.py       # Fluxo de Métricas
│   └── geodata.py            # Funções de Validação e Diagnóstico
├── openmetadata/             # Registro de metadata
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
```
## Visualização de dados
Os dados finais são carregados no **Neon Postgres**.
- Ferramentas compatíveis:
  - Power BI
  - Metabase
  - Tableau
  - DBeaver
  - pgAdmin

Há arquivo [README](README.pt-BR.md) com suporte adicional. Não hesite em pedir ajuda.