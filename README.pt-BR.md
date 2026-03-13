# Coleta e Tratamento de dados públicos de Educação fundamental utilizando python: Refazendo projeto de Artigo Científico

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](README.en-US.md)

[![App](https://custom-icon-badges.demolab.com/badge/Power%20BI-F1C912?logo=power-bi&logoColor=fff)](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9)

## Eficiência dos gastos públicos com educação nos municípios brasileiros aplicando Análise Envoltória de Dados

### Incrementos
- Expandir estudo para todos os municípios possíveis;
- Aprofundar as análises;
- Incluir painel com visualizaçao de dados;
- Documentar e publicar.

## Sumário
- [Sobre o Projeto](#projeto)
- [Diagrama](project_diagram.md)
- [Pipeline e Arquitetura](#código)
- [Modelagem DEA](#métodos)
- [Resultados](#resultados)
- [Artigo original e Referências](https://github.com/puffdapaz/DEApython/blob/main/docs/Eficiência%20dos%20gastos%20públicos%20com%20educação%20nos%20municípios%20baianos.pdf)
- [Como replicar o repositório](SETUP.pt-BR.md)

## Projeto
O objetivo deste projeto é aprimorar o uso de boas práticas em Python para engenharia, análise e ciência de dados por meio da replicação e aprimoramento de um estudo de pesquisa realizado em 2023, publicado como artigo científico com base em dados sociais públicos.
O estudo avalia a eficiência técnica e de escala dos municípios brasileiros em relação à alocação de recursos públicos para a educação em 2017 e 2019, aplicando o modelo DEA (Análise Envoltória de Dados).
Adicionalmente ao o modelo econométrico, o projeto implementa um pipeline modular de processamento de dados em formato lakehouse com:
- Arquitetura Medallion com Parquet localmente e no [GCS Cloud Storage](https://cloud.google.com/storage);
- Validação de esquema com [Pandera](https://github.com/unionai-oss/pandera);
- Armazenamento de dados serverless no [Neon PostgreSQL](https://neon.com);
- Linhagem de dados e metadados em [OpenMetadata](https://open-metadata.org);
- Visualização do painel com [Power BI](www.microsoft.com/power-platform/products/power-bi).

## Código
1. **Camada Bronze**
O fluxo começa com a extração de dados usando o [datalake BigQuery basedosdados](https://basedosdados.org), organização, validação da consistência das tabelas e armazenamento em formato .parquet em diretório local e [bucket do GCS](https://cloud.google.com/storage) na camada bronze, como DataFrames em sua estrutura original/bruta, sem qualquer modificação.
Os dados coletados são em nível municipal e referem-se aos anos de 2017 e 2019 para o Ensino Fundamental:
- População;
- PIB;
- Gastos com Educação;
- Quantidade de Matrículas;
- IDEB;
    - Anos iniciais;
    - Anos finais;
- Taxa de Abandono;
    - Anos iniciais;
    - Anos finais.

2. **Camada Prata**
As tabelas passam por um processo de transformação, sendo combinadas em um único DataFrame (através do Código do Município estabelecido pelo [IBGE - Instituto Brasileiro de Geografia e Estatística](https://servicodados.ibge.gov.br/api/docs/)), renomeação de campos e inclusão dos seguintes campos:
- Nome da cidade;
- PIB per capita (PIB / População);
- Gasto por aluno (Gasto com Educação / Número de Matrículas);
- % do PIB em Educação (Gasto com Educação / PIB);
- Indicador de completude dos dados por cidade.
O DataFrame passa por análise descritiva e de correlação, é validado em relação aos campos e tipos de dados e, finalmente, armazenado em formato .parquet em diretório local e [bucket do GCS](https://cloud.google.com/storage) na camada Silver.

3. **Características Geográficas**
Os polígonos geográficos municipais são obtidos utilizando o pacote [geobr](https://github.com/ipeaGIT/geobr), e pelo Código Municipal estabelecido pelo [IBGE](https://servicodados.ibge.gov.br/api/docs/), combinados com as informações socioeconômicas centralizadas no DataFrame salvo na camada Silver.

4. **Camada Gold**
Nesta etapa, o fluxo parte do DataFrame salvo na etapa anterior (camada Silver). Os dados são filtrados pelo campo de completude dos dados, e os campos relevantes são extraídos e organizados em matrizes para serem modelados usando DEA.
* Os campos de Taxa de Abandono são convertidos para ajuste de modelagem, uma vez que taxas de abandono mais altas indicam pior desempenho. Ao contrário do estudo original, que usou a razão '*1/taxa*' para ajuste, este projeto converte a taxa usando '*100 - taxa*' como base.

O modelo [dealib](https://github.com/ArtyomViryutin/dealib) é então aplicado às matrizes, e campos adicionais são calculados com base nos resultados:
- Eficiência de escala (Orientado à entrada de retorno constante / Orientado à entrada de retorno variável);
- Classificação da Natureza dos Retornos.

5. **Métricas Adicionais**
Há uma etapa adicional para calcular as métricas usadas na interpretação visual dos resultados:
- Mediana Nacional Anual do VRS Input;
- Média Estadual Anual do VRS Input;
- Ranking Nacional Anual do VRS Input;
- Ranking Estadual Anual do VRS Input.
Os resultados e os campos calculados adicionais são agregados ao DataFrame principal, que passa por análises descritivas e de correlação, bem como testes estatísticos (Normalidade, Distribuição e teste t) e validação em relação aos campos, valores e tipos de dados.
O DataFrame, o resumo descritivo (ambos em formato .parquet) e os resultados dos testes estatísticos (em formato .json) são armazenados em diretório local, [bucket do GCS](https://cloud.google.com/storage) na camada Gold e [banco de dados serverless Neon PostgreSQL](https://neon.com).

6. **Metadados**
Há uma etapa adicional de registro de metadados do projeto com base em esquemas de validação ([Pandera](https://github.com/unionai-oss/pandera)) para assegurar que os metadados correspondam às estruturas de dados reais do projeto. [OpenMetadata](https://open-metadata.org) cria e gerencia todas as entidades de metadados do projeto DEA encadeadas:
- Tabelas de origem do Data Lake ([Basedosdados do BigQuery](https://basedosdados.org));
- Serviço de armazenamento ([Camadas bronze, prata e ouro do GCS](https://cloud.google.com/storage));
- Dataset Gold ([Neon PostgreSQL DataWarehouse](https://neon.com));
- Painel visual ([Painel Power BI](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9));
- Relações de linhagem entre todas as entidades.

7. **Visuais**
Com a conclusão do processamento dos dados, o DataFrame fica disponível para consumo no [banco de dados Neon PostgreSQL](https://neon.com) pelo [painel em Power BI](https://app.powerbi.com/view?r=eyJrIjoiMmU1ZDJkYjItNTM2NS00ZWFiLWFhNTAtYzE5ZjRkZTBiZjcyIiwidCI6ImFlMTJhMzE4LWQxYjgtNGQ5My04NTBmLTQ3ZWFkMzYwMmM2NiJ9).

## Métodos
### **CRS** — *Retornos Constantes de Escala*
- Modelo CCR de Charnes, Cooper e Rhodes;
### **VRS** — *Retornos Variáveis ​​de Escala*
- Modelo BCC de Banker, Charnes e Cooper;
### **IRS** — *Retornos Crescentes de Escala*
- Modelo BCC de Banker, Charnes e Cooper;
### **DRS** — *Retornos decrescentes de escala*
- Modelo BCC de Banker, Charnes e Cooper;
### **Orientado para Insumos**
- Busca minimizar os insumos para um determinado nível de produção;
### **Orientado para Produtos**
- Busca maximizar os produtos para um determinado nível de insumo.
### Parâmetros:
- Período:
    - 2017;
    - 2019;
- Variáveis ​​de Entrada:
    - PIB per capita;
    - Gastos municipais por aluno (Gastos com educação / Matrículas);
- Variáveis ​​de Saída:
    - Nota IDEB anos iniciais;
    - Nota IDEB anos finais;
    - Taxa de evasão escolar anos iniciais;
    - Taxa de evasão escolar anos finais.

### Modelo
| **DEA CRS (orientado a insumo)** | **DEA VRS (orientado a insumo)** |
|----------------------------------|----------------------------------|
| **Objetivo:**<br>Minimizar $$\theta = \min \frac{\lambda u}{\lambda v}$$ | **Objetivo:**<br>Minimizar $$\theta = \min \frac{\lambda}{\theta u}$$ |
| **Sujeito a:**<br>$$\sum_i u_i x_i \le 1$$<br>$$\sum_i u_i y_i \ge \theta y^0 \quad \forall \text{DMUs}$$<br>$$u_i \ge 0, \; v_i \ge 0$$ | **Sujeito a:**<br>$$\sum_i v_i x_i \le 1$$<br>$$\sum_i v_i y_i \ge \theta u \quad \forall \text{DMUs}$$<br>$$u_i \ge 0, \; v_i \ge 0$$ |

| **DEA CRS (orientado a produto)** | **DEA VRS (orientado a produto)** |
|----------------------------------|----------------------------------|
| **Objetivo:**<br>Minimizar $$\theta = \min \frac{\lambda v}{\lambda u}$$ | **Objetivo:**<br>Minimizar $$\theta = \min \frac{\theta v}{\lambda}$$ |
| **Sujeito a:**<br>$$\sum_i v_i x_i \ge x^0 \quad \forall \text{DMUs}$$<br>$$\sum_i v_i y_i = 1$$<br>$$u_i \ge 0, \; v_i \ge 0$$ | **Sujeito a:**<br>$$\sum_i u_i x_i \ge \theta v \quad \forall \text{DMUs}$$<br>$$\sum_i u_i y_i = 1$$<br>$$u_i \ge 0, \; v_i \ge 0$$ |

#### Notação
- $$x_i$$: insumo \(i\);
- $$y_i$$: produto \(i\);
- $$u_i, v_i$$: pesos associados aos produtos e insumos;
- $$\theta$$: escore de eficiência.

## Resultados
A extensão do estudo a mais municípios reforçou os resultados obtidos na pesquisa original em 2023.
>"... A excelência na gestão dos insumos, não significa em incrementar o investimento por aluno ou o orçamento como um todo, mas também na busca pela menor utilização possível de recursos, visando o bem-estar social; cabe aos responsáveis e tomadores de decisão se aplicarem continuamente, independente de cenários favoráveis, respeitando as demandas socioeconômicas e propiciando condições básicas de acesso aos estudantes.
>
>O aprimoramento no gerenciamento dos gastos passa não somente por investimento em educação, mas em assegurar equidade nas oportunidades."

Somente 52% dos municípios apresentou todas as informações no recorte temporal, compondo a amostra (2891 municípios).
Os testes estatísticos mostram três conclusões principais: os dados de eficiência de escala não seguem distribuição normal (teste de Shapiro-Wilk, p < 10⁻⁴¹), a média da eficiência de escala é significativamente diferente de 1 (teste t, p = 0.0), e as distribuições entre CRS/VRS e IRS/DRS são significativamente diferentes (teste de Kolmogorov-Smirnov, p < 10⁻⁴³). Os p-valores muito baixos (< 0,05) indicam alta confiança de que essas diferenças são estatisticamente significativas.

Em média, os municípios apresentaram leve aumento no PIB, gastos em educação e PIB per capita até 2019, bem como, melhorias nos índices do IDEB e redução nas taxas de evasão. Os escores de eficiência DEA mantiveram-se relativamente estáveis, com a eficiência VRS em torno de 0,52 em 2019, indicando eficiência moderada no uso de recursos.

As correlações mostram que maiores ***percentuais de gastos em educação em relação ao PIB*** estão ***fortemente associados a melhores escores de eficiência DEA***, enquanto taxas de evasão correlacionam-se negativamente com o IDEB. 
Municípios com maior PIB per capita ou gasto por aluno tendem a ter melhores resultados educacionais, ***mas não necessariamente maior eficiência, sugerindo disparidades na alocação de recursos.*** A eficiência de escala melhorou levemente, embora muitos municípios ainda operem abaixo da escala ideal. 