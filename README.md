# MVP – Construção de um Pipeline de Dados na Nuvem

Projeto desenvolvido como MVP acadêmico da Pós-Graduação em Ciência de Dados e Analytics da PUC-Rio, utilizando o Databricks para construção de um pipeline de Engenharia de Dados aplicado ao mercado imobiliário.

## Tecnologias utilizadas

- Databricks Free Edition
- Apache Spark (PySpark)
- Spark SQL
- Delta Lake
- Unity Catalog
- Python

## 1. Contexto de Negócio e Perguntas

### 1.1 Contexto de Negócio

O mercado imobiliário apresenta diferenças significativas de preços, oferta e dinâmica de comercialização entre diferentes localidades. A análise dessas informações pode auxiliar empresas do setor imobiliário, incorporadoras, investidores e demais agentes do mercado na compreensão do comportamento de diferentes regiões e na identificação de padrões relevantes para a tomada de decisão.

Este projeto tem como objetivo construir um pipeline de dados em ambiente de nuvem para coletar, organizar, transformar e analisar dados do mercado imobiliário de cinco cidades brasileiras: Salvador, São Paulo, Rio de Janeiro, Recife e Curitiba.

Os dados utilizados contêm informações mensais por bairro relacionadas aos preços por metro quadrado, quantidade de anúncios, entrada de novos anúncios e tempo médio de permanência dos imóveis no mercado.

A solução foi estruturada utilizando a arquitetura Medalhão, composta pelas camadas Bronze, Silver e Gold. A camada Bronze preserva os dados provenientes da fonte; a camada Silver realiza limpeza, padronização e tratamento; e a camada Gold disponibiliza os dados por meio de um modelo dimensional destinado ao consumo analítico.

O resultado esperado é disponibilizar uma estrutura analítica capaz de comparar os mercados imobiliários estudados e identificar padrões relacionados a preços, oferta, permanência dos imóveis no mercado e evolução temporal.

### 1.2 Perguntas do Projeto

1. Como o preço mediano por metro quadrado varia entre as cinco cidades analisadas?
2. Quais cidades apresentam os maiores e os menores preços medianos por metro quadrado para imóveis destinados à venda e ao aluguel?
3. Como o preço mediano por metro quadrado evoluiu ao longo do período analisado em cada cidade?
4. Como se comporta a oferta de imóveis nas cidades analisadas, considerando a quantidade média de anúncios ativos e a entrada de novos anúncios?
5. Existem diferenças no tempo médio de permanência dos imóveis no mercado entre as cidades e entre os tipos de transação?
6. Quais bairros apresentam os maiores preços medianos por metro quadrado em cada cidade?

## 2. Carga dos Dados

### 2.1 Fonte dos Dados

Os dados utilizados neste projeto foram obtidos a partir da base pública disponibilizada pelo **Guru dos Imóveis**, contendo informações do mercado imobiliário brasileiro.

Foram utilizados arquivos no formato CSV referentes a cinco cidades:

- Salvador - BA;
- São Paulo - SP;
- Rio de Janeiro - RJ;
- Recife - PE;
- Curitiba - PR.

Os arquivos contêm observações mensais por bairro e tipo de transação, contemplando informações relacionadas a preço por metro quadrado, quantidade de anúncios ativos, novos anúncios e tempo médio de permanência dos imóveis no mercado.

A fonte disponibiliza os dados sob a licença **CC BY 4.0 (Creative Commons Attribution 4.0)**.

### 2.2 Ingestão dos Dados

Os arquivos CSV foram carregados para um **Volume do Unity Catalog no Databricks**, utilizado como área de armazenamento dos arquivos de origem.

A ingestão foi implementada utilizando **Apache Spark (PySpark)**. Inicialmente, os arquivos foram inspecionados individualmente para identificação da estrutura, esquema e possíveis particularidades de leitura.

Após a validação, foi definido um esquema explícito para a ingestão, evitando dependência exclusiva da inferência automática de tipos.

Durante o processo também foram adicionados metadados para permitir a identificação e rastreabilidade dos registros:

- `arquivo_origem`: identifica o arquivo CSV de origem;
- `cidade`: identifica a cidade correspondente ao arquivo;
- `uf`: identifica a Unidade Federativa;
- `data_ingestao`: registra o momento da ingestão no ambiente de dados.

### 2.3 Camada Bronze

Após a leitura e consolidação dos cinco arquivos, os dados foram armazenados na camada **Bronze** da arquitetura Medalhão.

A camada Bronze tem como objetivo preservar os dados provenientes da fonte, acrescentando apenas os metadados necessários para identificação e rastreabilidade.

A tabela criada foi:

`mvp_imobiliario.bronze.mercado_imobiliario_raw`

Ao final da ingestão, foram consolidados **36.933 registros**.

A tabela foi persistida utilizando o formato **Delta**, permitindo que os dados armazenados no ambiente Databricks sejam utilizados pelas etapas posteriores do pipeline.

## 3. Modelagem e Catálogo de Dados

### 3.1 Modelagem dos Dados

Após o processo de tratamento realizado na camada Silver, os dados foram organizados na camada Gold utilizando um **modelo dimensional**, com separação entre dimensões e tabela fato.

O modelo foi estruturado com três dimensões e uma tabela fato:

- `dim_tempo`: dimensão responsável pela representação temporal das observações;
- `dim_localidade`: dimensão contendo cidade, UF e bairro;
- `dim_transacao`: dimensão responsável pela classificação do tipo de transação imobiliária;
- `fato_mercado_imobiliario`: tabela fato contendo as métricas utilizadas nas análises do mercado imobiliário.

O grão definido para a tabela fato corresponde a **uma observação mensal para cada combinação de período, localidade e tipo de transação**.

Foram utilizadas chaves substitutas para realizar os relacionamentos entre a tabela fato e as dimensões:

- `id_tempo`;
- `id_localidade`;
- `id_transacao`.

A tabela fato contém as seguintes métricas:

- `mediana_m2`;
- `p25_m2`;
- `p75_m2`;
- `anuncios_ativos_media_dia`;
- `novos_anuncios`;
- `dias_no_mercado_medio`.

A construção do modelo preservou os **36.933 registros** existentes na camada Silver. A integridade dos relacionamentos também foi validada, não sendo identificadas chaves dimensionais nulas após os relacionamentos entre a tabela fato e as dimensões.

As tabelas resultantes da camada Gold foram:

- `mvp_imobiliario.gold.dim_tempo` — 13 registros;
- `mvp_imobiliario.gold.dim_localidade` — 2.129 registros;
- `mvp_imobiliario.gold.dim_transacao` — 2 registros;
- `mvp_imobiliario.gold.fato_mercado_imobiliario` — 36.933 registros.

### 3.2 Catálogo de Dados

O catálogo técnico das tabelas foi implementado no **Unity Catalog do Databricks**, onde foram adicionadas descrições às tabelas e aos seus respectivos campos.

A documentação do catálogo contempla a finalidade das tabelas e o significado dos atributos utilizados no pipeline. Além da documentação registrada no Unity Catalog, o projeto apresenta a seguir a estrutura lógica dos principais campos utilizados no modelo dimensional.

#### Dimensão Tempo — `dim_tempo`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_tempo` | BIGINT | Chave substituta da dimensão tempo | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `mes` | DATE | Mês de referência da observação | Datas mensais existentes na base | Proveniente do campo `mes` da camada Silver |
| `ano` | INT | Ano da observação | Ano correspondente ao campo `mes` | Derivado de `mes` |
| `numero_mes` | INT | Número do mês | Valores de 1 a 12 | Derivado de `mes` |

#### Dimensão Localidade — `dim_localidade`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_localidade` | BIGINT | Chave substituta da dimensão localidade | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `cidade` | STRING | Cidade da observação | Salvador, São Paulo, Rio de Janeiro, Recife ou Curitiba | Proveniente da camada Silver |
| `uf` | STRING | Unidade Federativa | BA, SP, RJ, PE ou PR | Proveniente da camada Silver |
| `bairro` | STRING | Bairro associado à observação | Bairros existentes na base | Proveniente da camada Silver |

#### Dimensão Transação — `dim_transacao`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_transacao` | BIGINT | Chave substituta da dimensão transação | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `transacao` | STRING | Tipo de transação imobiliária | `venda` ou `aluguel` | Campo padronizado na camada Silver |

#### Tabela Fato — `fato_mercado_imobiliario`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_tempo` | BIGINT | Chave de relacionamento com `dim_tempo` | Chave válida da dimensão tempo | Relacionamento com `dim_tempo` |
| `id_localidade` | BIGINT | Chave de relacionamento com `dim_localidade` | Chave válida da dimensão localidade | Relacionamento com `dim_localidade` |
| `id_transacao` | BIGINT | Chave de relacionamento com `dim_transacao` | Chave válida da dimensão transação | Relacionamento com `dim_transacao` |
| `mediana_m2` | INT | Preço mediano por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `p25_m2` | INT | Percentil 25 do preço por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `p75_m2` | INT | Percentil 75 do preço por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `anuncios_ativos_media_dia` | INT | Média diária de anúncios ativos | Valor igual ou superior a zero | Proveniente da camada Silver |
| `novos_anuncios` | INT | Quantidade de novos anúncios | Valor igual ou superior a zero | Proveniente da camada Silver |
| `dias_no_mercado_medio` | INT | Tempo médio de permanência dos imóveis no mercado | Valor igual ou superior a zero ou nulo | Proveniente da camada Silver |

## 4. Pipeline de Dados

### 4.1 Arquitetura do Pipeline

O pipeline foi desenvolvido integralmente no **Databricks Free Edition**, utilizando Apache Spark (PySpark), Spark SQL, Delta Lake e Unity Catalog.

A arquitetura adotada segue o padrão Medalhão, organizando o processamento dos dados em três camadas principais:

**Fonte de Dados → Bronze → Silver → Gold → Qualidade → Análise**

Cada etapa do pipeline foi implementada em um notebook específico, permitindo separar as responsabilidades de ingestão, transformação, modelagem, avaliação da qualidade e análise dos dados.

Os notebooks foram organizados na seguinte sequência:

1. `01_ingestao_bronze` — ingestão e consolidação dos arquivos de origem na camada Bronze;
2. `02_transformacao_silver` — avaliação inicial, limpeza, padronização e tratamento dos dados;
3. `03_modelagem_gold` — construção do modelo dimensional e persistência das dimensões e tabela fato;
4. `04_qualidade_dados` — avaliação da qualidade dos dados;
5. `05_analise_final` — realização das análises destinadas a responder às perguntas definidas no início do projeto.

### 4.2 Fluxo Bronze → Silver

A primeira etapa do pipeline realiza a leitura dos arquivos CSV armazenados no Volume do Unity Catalog e consolida os dados na tabela:

`mvp_imobiliario.bronze.mercado_imobiliario_raw`

A camada Bronze preserva os dados provenientes dos arquivos de origem e acrescenta metadados para garantir sua rastreabilidade.

Na etapa seguinte, a tabela Bronze é utilizada como origem para a construção da camada Silver. Durante essa transformação foram realizadas verificações e tratamentos relacionados a:

- valores nulos;
- registros duplicados;
- padronização de campos categóricos;
- validação de campos numéricos;
- identificação de valores iguais a zero;
- investigação de valores extremos;
- adequação dos tipos e estrutura dos dados.

Foi identificado um conjunto de 11 registros com valor igual a zero simultaneamente nos campos de preço por metro quadrado. Esses valores foram considerados inadequados para representar preços imobiliários e foram convertidos para valores nulos.

Valores iguais a zero em métricas nas quais esse valor possui interpretação válida, como quantidade de novos anúncios, foram preservados.

Após os tratamentos, os dados foram persistidos na tabela:

`mvp_imobiliario.silver.mercado_imobiliario_tratado`

A transformação preservou os **36.933 registros** existentes na camada Bronze.

### 4.3 Fluxo Silver → Gold

A camada Silver é utilizada como origem para a construção do modelo dimensional da camada Gold.

A partir dos dados tratados foram construídas as dimensões:

- `dim_tempo`;
- `dim_localidade`;
- `dim_transacao`;

e a tabela fato:

- `fato_mercado_imobiliario`.

Os relacionamentos foram realizados utilizando as chaves `id_tempo`, `id_localidade` e `id_transacao`.

Após a construção do modelo, foram realizadas validações para verificar a integridade dos relacionamentos e a preservação do grão definido para a tabela fato.

A camada Gold passou a representar a estrutura destinada ao consumo analítico, sendo utilizada nas etapas posteriores de avaliação da qualidade e análise das perguntas de negócio.

### 4.4 Persistência dos Dados

As tabelas das camadas Bronze, Silver e Gold foram persistidas no ambiente Databricks utilizando **Delta Lake** e organizadas no catálogo `mvp_imobiliario`.

A estrutura lógica utilizada foi:

- `mvp_imobiliario.bronze`
- `mvp_imobiliario.silver`
- `mvp_imobiliario.gold`

Essa organização permite separar os diferentes estágios de processamento e manter a rastreabilidade entre os dados de origem, os dados tratados e as estruturas destinadas à análise.
