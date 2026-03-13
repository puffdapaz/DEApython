# Collection and Processing of Public Elementary Education Data using Python: Reproducing a Scientific Paper Project

[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/puffdapaz/DEApython/blob/main/README.pt-BR.md)

[![App](https://custom-icon-badges.demolab.com/badge/Power%20BI-F1C912?logo=power-bi&logoColor=fff)](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9)

## Efficiency of public spending on education in Brazilian municipalities applying Data Envelopment Analysis

### [Data Source](https://basedosdados.org)

### Improvements
- Expand the study to all possible municipalities;<br/>
- Deepen the analysis;<br/>
- Include a data visualization dashboard;<br/>
- Document and publish.<br/>

## Table of Contents
- [About the Project](#project)
- [Diagram](https://github.com/puffdapaz/DEApython/blob/main/project_diagram.md)
- [Pipeline and Architecture](#code)
- [DEA Modeling](#methods)
- [Results](#results)
- [Paper and References (pt_BR)](https://github.com/puffdapaz/DEApython/blob/main/docs/Eficiência%20dos%20gastos%20públicos%20com%20educação%20nos%20municípios%20baianos.pdf)
- [How to replicate the repository](https://github.com/puffdapaz/DEApython/blob/main/SETUP.en-US.md)

## Project
The goal of this project is to improve the use of good practices in Python for data engineering, analysis, and science through the replication and enhancement of a research study conducted in 2023, published as a scientific paper based on public social data.<br/>
The study evaluates the technical and scale efficiency of Brazilian municipalities regarding the allocation of public resources to education in 2017 and 2019, applying the DEA (Data Envelopment Analysis) model.<br/>
In addition to reproducing the econometric model, the project implements a modular lakehouse data processing pipeline with:<br/>
 - Medallion architecture with parquet locally and at [GCS Cloud storage](https://cloud.google.com/storage);<br/>
 - Schema validation with [Pandera](https://github.com/unionai-oss/pandera);<br/>
 - Serverless warehousing at [Neon PostgreSQL](https://neon.com);<br/>
 - Data lineage and metadata at [OpenMetadata](https://open-metadata.org);<br/>
 - Dashboard visualization with [Power BI](www.microsoft.com/power-platform/products/power-bi).<br/>

## Code
1. **Bronze Layer**<br/>
The flow begins with data extraction using the [basedosdados BigQuery datalake](https://basedosdados.org), organization, table consistency validation, and storage in .parquet format in a local directory and in a [GCS bucket](https://cloud.google.com/storage) on bronze layer, as DataFrames in their original/raw structure, without any modification.<br/>
The data collected are at the municipal level and refer to the years 2017 and 2019 for Elementary Education:<br/>
- Population;<br/>
- GDP;<br/>
- Education Spending;<br/>
- Number of Enrollments;<br/>
- IDEB;<br/>
    - Early years;<br/>
    - Final years;<br/>
- Dropout Rate;<br/>
    - Early years;<br/>
    - Final years.<br/>

2. **Silver Layer**<br/>
The tables go through a transformation process, being combined into a single DataFrame (through the Municipality Code established by [IBGE - Instituto Brasileiro de Geografia e Estatística](https://servicodados.ibge.gov.br/api/docs/)), field renaming, and inclusion of the following fields:<br/>
- City name;<br/>
- GDP per Capita (GDP / Population);<br/>
- Spending per Student (Education Spending / Number of Enrollments);<br/>
- % of GDP in Education (Education Spending / GDP);<br/>
- Data completeness flag by city.<br/>
The DataFrame undergoes descriptive and correlation analysis and is also validated regarding fields and data types, and finally stored in .parquet format in a local directory and in a [GCS bucket](https://cloud.google.com/storage) on silver layer.<br/>

3. **Geographic Features**<br/>
The municipal geographic polygons are then obtained using [geobr](https://github.com/ipeaGIT/geobr) and again by the Municipality Code established by [IBGE](https://servicodados.ibge.gov.br/api/docs/), merged with the socioeconomic information centralized in the DataFrame saved on silver layer.<br/>

4. **Gold Layer**<br/>
At this stage, the flow starts from the DataFrame saved in the previous step (silver). The data is filtered by the data completeness field, and the relevant fields are extracted and organized into matrices to be modeled using DEA.<br/>
\* The Dropout Rate fields are converted for modeling adjustment since higher dropout rates indicate worse performance. Unlike the original study, which used the ratio '*1/rate*' for adjustment, this project converts the rate using '*100 - rate*' base.<br/>

The [dealib](https://github.com/ArtyomViryutin/dealib) model is then applied to the matrices, and additional fields are calculated based on the results:<br/>
- Scale efficiency (Constant Return Input-Oriented / Variable Return Input-Oriented);<br/>
- Classification of Returns Nature.<br/>

5. **Additional Metrics**<br/>
There is an additional step for calculating metrics used in visual interpretation of the results:<br/>
- Yearly National VRS Input Median;<br/>
- Yearly State VRS Input Average;<br/>
- Yearly VRS Input National Ranking;<br/>
- Yearly VRS Input State Ranking.<br/>
The results and additional calculated fields are aggregated into the main DataFrame, which undergoes descriptive and correlation analysis, as well as statistical tests (Normality, Distribution, and t-test), and validation regarding fields, values, and data types.<br/>
The DataFrame, the descriptive summary (both in .parquet format), and the statistical test results (in .json format) are stored in a local directory, in a [GCS bucket](https://cloud.google.com/storage) on gold layer and at a [Neon PostgreSQL serverless warehouse](https://neon.com).<br/>

6. **Metadata**<br/>
There is an additional step registering metadata of the project based on validation schemas ([Pandera](https://github.com/unionai-oss/pandera)) to ensure metadata matches the project actual data structures. [OpenMetadata](https://open-metadata.org) creates and manages all DEA project metadata entities linked:<br/>
- Datalake source tables ([BigQuery basedosdados](https://basedosdados.org));<br/>
- Storage services ([GCS bronze, silver and gold layers](https://cloud.google.com/storage));<br/>
- Gold dataset ([Neon PostgreSQL DataWarehouse](https://neon.com));<br/>
- Dashboard service ([Power BI dashboard](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9));<br/>
- Lineage relationships between all entities.<br/>

7. **Visuals**<br/>
With the completion of data processing, the DataFrame is then available for consumption in [Neon PostgreSQL serverless warehouse](https://neon.com) by a [Power BI dashboard](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9).<br/>

## Methods
### **CRS** — *Constant Returns to Scale*
- CCR Model by Charnes, Cooper, and Rhodes;<br/>
### **VRS** — *Variable Returns to Scale*
- BCC Model by Banker, Charnes, and Cooper;<br/>
### **IRS** — *Increasing returns to scale*
- BCC Model by Banker, Charnes, and Cooper;<br/>
### **DRS** — *Decreasing returns to scale*
- BCC Model by Banker, Charnes, and Cooper;<br/>
### **Input-Oriented**
- Seeks to minimize inputs for a given level of output;<br/>
### **Output-Oriented**
- Seeks to maximize outputs for a given level of input.<br/>
### Parameters:
- Period:<br/>
    - 2017;<br/>
    - 2019;<br/>
- Input Variables:<br/>
    - GDP per capita;<br/>
    - Municipal spending per student (Education spending / Enrollments);<br/>
- Output Variables:<br/>
    - IDEB early years;<br/>
    - IDEB final years;<br/>
    - Dropout rate early years;<br/>
    - Dropout rate final years.<br/>

### Model
| **DEA CRS (Input-Oriented)** | **DEA VRS (Input-Oriented)** |
|-------------------------------|-------------------------------|
| **Objective:**<br>Minimize $$\theta = \min \frac{\lambda u}{\lambda v}$$ | **Objective:**<br>Minimize $$\theta = \min \frac{\lambda}{\theta u}$$ |
| **Subject to:**<br>$$\sum_i u_i x_i \le 1$$<br>$$\sum_i u_i y_i \ge \theta y^0 \quad \forall \text{DMUs}$$<br>$$u_i \ge 0, \; v_i \ge 0$$ | **Subject to:**<br>$$\sum_i v_i x_i \le 1$$<br>$$\sum_i v_i y_i \ge \theta u \quad \forall \text{DMUs}$$<br>$$u_i \ge 0, \; v_i \ge 0$$ |

| **DEA CRS (Output-Oriented)** | **DEA VRS (Output-Oriented)** |
|-------------------------------|-------------------------------|
| **Objective:**<br>Minimize $$\theta = \min \frac{\lambda v}{\lambda u}$$ | **Objective:**<br>Minimize $$\theta = \min \frac{\theta v}{\lambda}$$ |
| **Subject to:**<br>$$\sum_i v_i x_i \ge x^0 \quad \forall \text{DMUs}$$<br>$$\sum_i v_i y_i = 1$$<br>$$u_i \ge 0, \; v_i \ge 0$$ | **Subject to:**<br>$$\sum_i u_i x_i \ge \theta v \quad \forall \text{DMUs}$$<br>$$\sum_i u_i y_i = 1$$<br>$$u_i \ge 0, \; v_i \ge 0$$ |

### Notation
- $$x_i$$: input \(i\);<br/>
- $$y_i$$: output \(i\);<br/>
- $$u_i, v_i$$: weights for outputs and inputs;<br/>
- $$\theta$$: efficiency score.<br/>

## Results
Extending the study to more municipalities reinforced the results obtained in the original research in 2023.<br/>
>"... Excellence in input management does not necessarily mean increasing investment per student or the overall budget, but rather seeking the least possible use of resources while aiming for social well-being; it is up to decision-makers to continuously apply themselves, regardless of favorable scenarios, respecting socioeconomic demands and ensuring basic access conditions for students.
><br/>
>Improving spending management involves not only investing in education but ensuring equity in opportunities."<br/>

Only 52% of the municipalities presented all the information in the time frame, composing the sample (2891 cities).<br/>
Statistical tests show three main conclusions: scale efficiency data do not follow a normal distribution (Shapiro–Wilk test, p < 10⁻⁴¹), the mean scale efficiency is significantly different from 1 (t-test, p = 0.0), and the distributions between CRS/VRS and IRS/DRS are significantly different (Kolmogorov–Smirnov test, p < 10⁻⁴³). The very low p-values (< 0.05) indicate high confidence that these differences are statistically significant.<br/>

On average, municipalities showed a slight increase in GDP, education spending, and GDP per capita by 2019, as well as improvements in IDEB scores and reduced dropout rates. DEA efficiency scores remained relatively stable, with VRS efficiency around 0.52 in 2019, indicating moderate resource-use efficiency.<br/>

Correlations show that higher ***percentages of education spending relative to GDP*** are ***strongly associated with better DEA efficiency scores***, while dropout rates correlate negatively with IDEB.<br/>
Municipalities with higher GDP per capita or spending per student tend to have better educational outcomes, ***but not necessarily higher efficiency, suggesting disparities in resource allocation.*** Scale efficiency improved slightly, although many municipalities still operate below the optimal scale.<br/>