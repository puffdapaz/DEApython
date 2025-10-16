# Coleta e Tratamento de dados públicos de Educação fundamental utilizando python: Refazendo projeto de Artigo Científico

[![en-us](https://img.shields.io/badge/lang-en--us-red.svg)](https://github.com/puffdapaz/DEApython/blob/main/README.en-US.md)

[![App](https://img.shields.io/badge/Streamlit-FF4B4B.svg?style=for-the-badge&logo=Streamlit&logoColor=white)]()

## Eficiência dos gastos públicos com educação nos municípios brasileiros aplicando Análise Envoltória de Dados

### [Fonte dos Dados](https://basedosdados.org)

### Incrementos
- Expandir estudo para todos os municípios possíveis;<br/>
- Aprofundar as análises;<br/>
- Incluir painel com visualizaçao de dados;<br/>
- Documentar e publicar.<br/>

## Sumário
- [Sobre o Projeto](#projeto)
- [Diagrama](https://github.com/puffdapaz/DEApython/blob/main/project_diagram.md)
- [Pipeline e Arquitetura](#código)
- [Modelagem DEA](#métodos)
- [Resultados](#resultados)
- [Artigo original e Referências](https://github.com/puffdapaz/DEApython/blob/main/docs/Eficiência%20dos%20gastos%20públicos%20com%20educação%20nos%20municípios%20baianos.pdf)
- [Como replicar o repositório](https://github.com/puffdapaz/DEApython/blob/main/SETUP.pt-BR.md)

## Projeto
O intuito do projeto é aperfeiçoar a utilização de boas práticas em python para engenharia, análise e ciência de dados através de réplica e aprimoramento de pesquisa realizada em 2023 em artigo científico utilizando dados públicos sociais, como referência.<br/>
O estudo avalia a eficiência técnica e de escala dos municípios brasileiros quanto à alocação de recursos públicos na educação em 2017 e 2019, aplicando o modelo DEA (Análise Envoltória de Dados).<br/>
Além de reproduzir o modelo econométrico, o projeto implementa uma pipeline modular de tratamento dos dados (Arquitetura Medallion), garantindo rastreabilidade, validação do esquema, e armazenamento local e em nuvem.<br/>

## Código
1. **Camada Bronze**<br/>
O fluxo inicia com a extração dos dados em [basedosdados SDK](https://basedosdados.org), organização, [validação de consistencia das tabelas](https://www.union.ai/pandera) e, armazenamento em extensão .parquet em diretório local e [GCS](https://cloud.google.com/storage) na camada bronze, como DataFrames em sua estrutura original/integral, sem qualquer modificação.<br/>
Os dados coletados são da esfera municipal e se referem aos anos de 2017 e 2019 do Ensino Fundamental:<br/>
- População;<br/>
- PIB;<br/>
- Gastos com Educação;<br/>
- Quantidade de Matrículas;<br/>
- IDEB;<br/>
    - Anos iniciais;<br/>
    - Anos finais;<br/>
- Taxa de Abandono;<br/>
    - Anos iniciais;<br/>
    - Anos finais.<br/>

2. **Camada Prata**<br/>
As tabelas passam por processo de transformação combinadas em um único DataFrame (através do Código de Município estabelecido pelo [IBGE - Instituto Brasileiro de Geografia e Estatística](https://servicodados.ibge.gov.br/api/docs/)), renomeação de campos, e inclusão dos campos:<br/>
- Nome dos municípios;<br/>
- PIB per Capita (PIB / População);<br/>
- Gasto por Aluno (Gastos com Educação / Quantidade de Matrículas);<br/>
- % do PIB em Educação (Gastos com Educação / PIB);<br/>
- Verificador de totalidade dos dados do Município.<br/>
O DataFrame passa por análise descritiva e de correlações, e também é [validado](https://www.union.ai/pandera) quanto aos campos e tipagem de dados, e por fim, armazenado em extensão .parquet em diretório local e [GCS](https://cloud.google.com/storage) na camada prata.<br/>

3. **Camada Ouro**<br/>
Nessa etapa, o fluxo se inicia a partir do DataFrame salvo na etapa anterior (prata). Os dados são filtrados pelo campo de totalidade dos dados, e extraem-se os campos a serem organizados como matrizes que serão modeladas.<br/>
\* Os campos de Taxa de Abandono têm uma conversão para ajuste na modelagem, uma vez que quanto maior o valor (abandono), pior é o índice. Diferente da pesquisa original, que utilizou a razão '*1/taxa*' para ajuste, este projeto converte a taxa nas bases '*100 - taxa*'.<br/>

O modelo [dealib](https://github.com/ArtyomViryutin/dealib) é então aplicado nas matrizes; sobre os resultados são calculados campos:<br/>
- Eficiência de escala (Ret. Constante Orient. Input / Ret. Variável Orient. Input);<br/>
- Classificação de Natureza dos Retornos.<br/>

4. **Métricas Adicionais**<br/>
Há uma etapa adicional de cálculo de métricas a serem utilizadas na interpretação gráfica dos resultados:<br/>
- Ranking anual de eficiência;<br/>
- Índice de variação percentual entre períodos;<br/>
- Classificação:<br/>
    - Classes de eficiência técnica;<br/>
    - Classes de eficiência de escala;<br/>
- Variação:<br/>
    - Comparação com a eficiência técnica mediana do ano;<br/>
    - Comparação com a eficiência de escala mediana do ano;<br/>
    - Comparação com a eficiência técnica média estadual do ano;<br/>
    - Comparação com a eficiência de escala média estadual do ano;<br/>
- Clusterização por características.<br/>
Os resultados e campos adicionais calculados são agregados ao DataFrame inicial, que passa por análise descritiva e de correlações, além de testes estatísticos (Normalidade, Distribuição e teste t), e [validação](https://www.union.ai/pandera) quanto aos campos, valores e tipagem de dados.<br/>
O Dataframe, o sumário descritivo (ambos em extensão .parquet) e os resultados dos testes estatísticos (em extensão .json) são armazenado em diretório local e [GCS](https://cloud.google.com/storage) na camada ouro.<br/>

5. ****<br/>
Há então a obtenção dos polígonos geográficos municipais através do [geobr](https://pypi.org/project/geobr/) e novamente mediante o Código de Município estabelecido pelo [IBGE](https://servicodados.ibge.gov.br/api/docs/), a consolidação das informações socioeconomicas centralizadas no DataFrame salvo na camada Gold, com as coordenadas geográficas.<br/>

6. ****<br/>
Com a finalização do tratamento dos dados, o DataFrame é disponibilizado para consumo em ferramentas de Inteligência de Negócio. Para [ilustração](link powerbi), são exibidos /histogramas das variáveis selecionadas, um gráfico de dispersão, entre IDHM e Carga Tributária, contendo uma linha de tendência, um diagrama de correlação de calor, e o mapa/.<br/>

## Métodos
### **CRS** — *Retornos Constantes de Escala*
- Modelo CCR de Charnes, Cooper e Rhodes;<br/>
### **VRS** — *Retornos Variáveis de Escala*
- Modelo BCC de Banker, Charnes e Cooper;<br/>
### **Orientado a insumo** 
- Busca minimizar os insumos mantendo o nível de produto constante;<br/>
### **Orientado a produto**
- Busca maximizar os produtos mantendo o nível de insumo constante.<br/>
### Parâmetros:
- Período:<br/>
    - 2017;<br/>
    - 2019;<br/>
- Variáveis Insumo:<br/>
    - PIB per capita;<br/>
    - Gasto municipal por aluno (Gasto em educação / Nº matrículas);<br/>
- Variáveis Produto:<br/>
    - IDEB anos iniciais;<br/>
    - IDEB anos finais;<br/>
    - Taxa de Abandono anos iniciais;<br/>
    - Taxa de Abandono anos finais.<br/>

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
- $$x_i$$: insumo \(i\);<br/>
- $$y_i$$: produto \(i\);<br/>
- $$u_i, v_i$$: pesos associados aos produtos e insumos;<br/>
- $$\theta$$: escore de eficiência.<br/>

## Resultados
A extensão do estudo a mais municípios reforçou os resultados obtidos na pesquisa original em 2023. <br/>
>"... A excelência na gestão dos insumos, não significa em incrementar o investimento por aluno ou o orçamento como um todo, mas também na busca pela menor utilização possível de recursos, visando o bem-estar social; cabe aos responsáveis e tomadores de decisão se aplicarem continuamente, independente de cenários favoráveis, respeitando as demandas socioeconômicas e propiciando condições básicas de acesso aos estudantes.
><br/>
>O aprimoramento no gerenciamento dos gastos passa não somente por investimento em educação, mas em assegurar equidade nas oportunidades."

Os testes estatísticos mostram três conclusões principais: os dados de eficiência de escala não seguem distribuição normal (teste de Shapiro-Wilk, p < 10⁻⁴¹), a média da eficiência de escala é significativamente diferente de 1 (teste t, p = 0.0), e as distribuições entre CRS/VRS e IRS/DRS são significativamente diferentes (teste de Kolmogorov-Smirnov, p < 10⁻⁴³). Os p-valores muito baixos (< 0,05) indicam alta confiança de que essas diferenças são estatisticamente significativas.<br/>

Em média, os municípios apresentaram leve aumento no PIB, gastos em educação e PIB per capita até 2019, bem como, melhorias nos índices do IDEB e redução nas taxas de evasão. Os escores de eficiência DEA mantiveram-se relativamente estáveis, com a eficiência VRS em torno de 0,52 em 2019, indicando eficiência moderada no uso de recursos. <br/>

As correlações mostram que maiores ***percentuais de gastos em educação em relação ao PIB*** estão ***fortemente associados a melhores escores de eficiência DEA***, enquanto taxas de evasão correlacionam-se negativamente com o IDEB. <br/>
Municípios com maior PIB per capita ou gasto por aluno tendem a ter melhores resultados educacionais, ***mas não necessariamente maior eficiência, sugerindo disparidades na alocação de recursos.*** A eficiência de escala melhorou levemente, embora muitos municípios ainda operem abaixo da escala ideal. <br/>