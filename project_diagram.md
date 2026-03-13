**Alt Text:**
"End-to-end data pipeline architecture diagram. Raw datasets extracted from 'basedosdados' BigQuery datalake and ingested into the Bronze layer of a GCS storage bucket. Data is then processed and validated into the Silver layer, followed by DEA and analytical features modeling in the Gold layer. Final outputs are loaded into a Neon PostgreSQL warehouse for visualization in a Power BI dashboard. Configuration files, environment variables, and schema validation are applied throughout the pipeline, and metadata lineage is tracked using OpenMetadata."

**Texto Alternativo:**
"Diagrama da arquitetura de um pipeline de dados de ponta a ponta. Conjuntos de dados brutos extraídos do datalake BigQuery 'basedosdados' são ingeridos na camada Bronze de armazenamento do GCS. Os dados são então processados ​​e validados na camada Silver, seguidos pela modelagem DEA e recursos analíticos na camada Gold. Os resultados finais são carregados em um warehouse Neon PostgreSQL para visualização em um painel do Power BI. Arquivos de configuração, variáveis ​​de ambiente e validação de esquema são aplicados ao longo de todo o pipeline, e a linhagem de metadados é rastreada usando o OpenMetadata."
```mermaid
flowchart TB
  %% ======================
  %% high contrast palette 
  %% ======================
classDef external fill:#E3F2FD,stroke:#0D47A1,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef bronze fill:#FFE8CC,stroke:#E65100,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef silver fill:#D4EDDA,stroke:#155724,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef gold fill:#F3E5F5,stroke:#4A148C,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef validation fill:#B2EBF2,stroke:#006064,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef storage fill:#FFF3CD,stroke:#856404,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef process fill:#F8F9FA,stroke:#495057,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef metadata fill:#ECEFF1,stroke:#263238,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef dashboard fill:#FFCDD2,stroke:#B71C1C,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
classDef persist fill:#424242,stroke:#212121,stroke-width:3px,color:#FFFFFF,font-weight:bold,text-align:center;

  %% ======================
  %% External Inputs
  %% ======================
  BD[Basedosdados<br/>BigQuery DataLake 🔍]
  CFG[Configuration<br/>YAML ⚙️]
  ENV[Environment<br/>.env 🔑]
  

  %% ======================
  %% Bronze Layer (extract -> validate -> save)
  %% ======================
  subgraph BRONZE["Ingestion - Bronze🥉"]
    B_EXTRACT[Extract raw tables via SQL]
    B_VALIDATE[Pandera schema validation]
    B_SAVE[Persist layer]
  end

  %% ======================
  %% Silver Layer (transform -> validate -> save)
  %% ======================
  subgraph SILVER["Processing - Silver🥈"]
    S_TRANSFORM[Join & Quality flags]
    S_VALIDATE[Pandera schema validation]
    S_SAVE[Persist layer]
  end

  %% ======================
  %% Gold Layer (model -> validate -> save)
  %% ======================
  subgraph GOLD["DEA Modeling - Gold🥇"]
    G_DEA[Run DEA models]
    G_VALIDATE[Pandera schema validation]
    G_SAVE[Persist layer]
  end

  %% ======================
  %% Metadata Layer
  %% ======================
  subgraph METADATA["Metadata & Lineage📋"]
    OM[OpenMetadata Catalog]
  end

  %% ======================
  %% Analytics & Storage
  %% ======================
  ANALYTICS[Analytical features<br/>📈]
  STORAGE[(Storage - Medallion layers<br/>GCS bucket<br/>💾)]
  WAREHOUSE[(Warehouse<br/>Neon Postgres Serverless<br/>🛢️)]
  MAPS[Geographic features<br/>🗺️]
  PBI[Power BI Dashboard<br/>📊]

  %% ======================
  %% Connections (data flow)
  %% ======================
  BD --> B_EXTRACT
  CFG --> B_EXTRACT
  ENV --> B_EXTRACT

  B_EXTRACT --> B_VALIDATE --> B_SAVE --> S_TRANSFORM

  CFG --> S_TRANSFORM
  ENV --> S_TRANSFORM
  S_TRANSFORM --> MAPS 
  MAPS --> S_VALIDATE 
  S_VALIDATE --> S_SAVE --> G_DEA

  CFG --> G_DEA
  ENV --> G_DEA
  G_DEA --> ANALYTICS --> 
  G_VALIDATE --> G_SAVE

  B_SAVE --> STORAGE
  S_SAVE --> STORAGE
  G_SAVE --> STORAGE
  G_SAVE --> WAREHOUSE
  WAREHOUSE --> PBI

  BD --> OM
  STORAGE --> OM
  WAREHOUSE --> OM
  PBI --> OM
  
  %% ======================
  %% Class assignments
  %% ======================
  class BD,ENV,CFG external;
  class B_EXTRACT,S_TRANSFORM,G_DEA,OM process;
  class B_VALIDATE,S_VALIDATE,G_VALIDATE validation;
  class STORAGE,WAREHOUSE storage;
  class ANALYTICS gold;
  class BRONZE bronze;
  class SILVER,MAPS silver;
  class GOLD gold;
  class PBI dashboard;
  class METADATA metadata;
  class B_SAVE,S_SAVE,G_SAVE persist;
```