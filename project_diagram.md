**Alt Text for Diagram:**
"End-to-end data pipeline diagram showing ingestion (Bronze), processing (Silver), and DEA modeling (Gold) layers. Each layer performs extract, transform, validate, and save steps, with configurations, environment variables, and schema validation integrated at each stage. Outputs are stored locally and in Google Cloud Storage and visualized via analytical dashboards."
```mermaid
flowchart TB
  %% ======================
  %% ACCESSIBLE COLOR SCHEME
  %% Colorblind-friendly palette with high contrast
  %% ======================
classDef external fill:#E6F3FF,stroke:#0066CC,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef bronze fill:#FFE8CC,stroke:#E65100,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef silver fill:#D4EDDA,stroke:#155724,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef gold fill:#F3E5F5,stroke:#4A148C,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef validation fill:#E3F2FD,stroke:#0D47A1,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef storage fill:#FFF3CD,stroke:#856404,stroke-width:3px,color:#000000,font-weight:bold,text-align:center;
    classDef process fill:#F8F9FA,stroke:#495057,stroke-width:2px,color:#000000,font-weight:bold,text-align:center;


  %% ======================
  %% External Inputs
  %% ======================
  BD[Base dos Dados<br/>📊]
  CFG[Configuration<br/>⚙️]
  ENV[Environment<br/>🔑]
  

  %% ======================
  %% Bronze Layer (extract -> validate -> save)
  %% ======================
  subgraph BRONZE["Ingestion - Bronze🥉"]
    B_EXTRACT[Extract data]
    B_VALIDATE[Validate schema]
    B_SAVE[Store Data]
  end

  %% ======================
  %% Silver Layer (transform -> validate -> save)
  %% ======================
  subgraph SILVER["Processing - Silver🥈"]
    S_TRANSFORM[Transform & join]
    S_FLAGS[Quality flags]
    S_VALIDATE[Validate schema]
    S_SAVE[Store Data]
  end

  %% ======================
  %% Gold Layer (model -> validate -> save)
  %% ======================
  subgraph GOLD["DEA Modeling - Gold🥇"]
    G_PREP[Prepare matrices]
    G_DEA[Run DEA models]
    G_VALIDATE[Validate schema]
    G_DIAG[Diagnostics]
    G_SAVE[Store Data]
  end

  %% ======================
  %% Analytics & Storage
  %% ======================
  ANALYTICS[Analytical features<br/>📈]
  STORAGE[(Storage<br/>💾)]

  %% ======================
  %% Connections (data flow)
  %% ======================
  BD --> B_EXTRACT
  CFG --> B_EXTRACT
  ENV --> B_EXTRACT

  B_EXTRACT --> B_VALIDATE --> B_SAVE --> S_TRANSFORM

  CFG --> S_TRANSFORM
  ENV --> S_TRANSFORM

  S_TRANSFORM --> S_FLAGS --> S_VALIDATE --> S_SAVE --> G_PREP

  CFG --> G_PREP
  ENV --> G_PREP
  G_DIAG --> ANALYTICS
  ANALYTICS --> G_SAVE
  G_PREP --> G_DEA --> G_VALIDATE --> G_DIAG

  B_SAVE --> STORAGE
  S_SAVE --> STORAGE
  G_SAVE --> STORAGE

  %% ======================
  %% Class assignments
  %% ======================
  class BD,ENV,CFG, external;
  class B_EXTRACT,B_SAVE,S_TRANSFORM,S_FLAGS,G_PREP,G_DEA process;
  class B_VALIDATE,S_VALIDATE,G_VALIDATE validation;
  class B_SAVE,S_SAVE,G_SAVE storage;
  class STORAGE storage;
  class ANALYTICS,G_DIAG gold;
  class BRONZE bronze;
  class SILVER silver;
  class GOLD gold;
```