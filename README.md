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

A documentação contempla a finalidade de cada tabela, o significado dos atributos, os tipos de dados, os domínios ou valores esperados e a origem ou transformação associada aos campos utilizados no modelo dimensional.

#### Dimensão Tempo — `dim_tempo`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_tempo` | INT | Chave substituta da dimensão tempo | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `mes` | DATE | Mês de referência da observação | Datas mensais existentes na base | Proveniente do campo `mes` da camada Silver |
| `ano` | INT | Ano da observação | Ano correspondente ao campo `mes` | Derivado de `mes` |
| `numero_mes` | INT | Número do mês | Valores de 1 a 12 | Derivado de `mes` |

#### Dimensão Localidade — `dim_localidade`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_localidade` | INT | Chave substituta da dimensão localidade | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `cidade` | STRING | Cidade associada à observação | Salvador, São Paulo, Rio de Janeiro, Recife ou Curitiba | Proveniente do campo `cidade` da camada Silver |
| `uf` | STRING | Unidade Federativa associada à cidade | BA, SP, RJ, PE ou PR | Proveniente do campo `uf` da camada Silver |
| `bairro` | STRING | Bairro associado à observação | Bairros existentes no conjunto de dados | Proveniente do campo `bairro` da camada Silver |

#### Dimensão Transação — `dim_transacao`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_transacao` | INT | Chave substituta da dimensão transação | Valores inteiros únicos e não nulos | Gerada durante a construção da dimensão |
| `transacao` | STRING | Tipo de transação imobiliária | `venda` ou `aluguel` | Proveniente do campo `transacao`, padronizado na camada Silver |

#### Tabela Fato — `fato_mercado_imobiliario`

| Campo | Tipo | Descrição | Domínio / Valores esperados | Origem / Transformação |
|---|---|---|---|---|
| `id_tempo` | INT | Chave de relacionamento com a dimensão tempo | Chave válida existente em `dim_tempo` | Obtida pelo relacionamento com `dim_tempo` |
| `id_localidade` | INT | Chave de relacionamento com a dimensão localidade | Chave válida existente em `dim_localidade` | Obtida pelo relacionamento com `dim_localidade` |
| `id_transacao` | INT | Chave de relacionamento com a dimensão transação | Chave válida existente em `dim_transacao` | Obtida pelo relacionamento com `dim_transacao` |
| `mediana_m2` | INT | Valor mediano do preço por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `p25_m2` | INT | Percentil 25 do preço por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `p75_m2` | INT | Percentil 75 do preço por metro quadrado | Valor positivo ou nulo | Proveniente da camada Silver |
| `anuncios_ativos_media_dia` | INT | Quantidade média diária de anúncios ativos | Valor igual ou superior a zero | Proveniente da camada Silver |
| `novos_anuncios` | INT | Quantidade de novos anúncios registrados no período | Valor igual ou superior a zero | Proveniente da camada Silver |
| `dias_no_mercado_medio` | INT | Quantidade média de dias dos imóveis no mercado | Valor igual ou superior a zero ou nulo | Proveniente da camada Silver |

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

## 5. Qualidade de Dados

A avaliação da qualidade foi realizada sobre os dados tratados da camada Silver e sobre as estruturas dimensionais da camada Gold. Foram analisados aspectos de completude, consistência, unicidade, plausibilidade dos valores e presença de valores extremos.

### 5.1 Completude

A análise de completude identificou valores nulos principalmente nas métricas relacionadas ao preço por metro quadrado.

Após o tratamento realizado na camada Silver, os resultados foram:

| Campo | Valores nulos | Percentual aproximado |
|---|---:|---:|
| `mediana_m2` | 3.767 | 10,20% |
| `p25_m2` | 3.767 | 10,20% |
| `p75_m2` | 3.767 | 10,20% |
| `dias_no_mercado_medio` | 22 | 0,06% |

Os demais campos avaliados não apresentaram valores nulos.

Os valores ausentes nas métricas de preço foram preservados como nulos, evitando a utilização de valores artificiais que poderiam distorcer as análises estatísticas.

### 5.2 Consistência

Foram realizadas verificações sobre os principais campos numéricos e categóricos.

Não foram identificados valores negativos nas métricas:

- `mediana_m2`;
- `p25_m2`;
- `p75_m2`;
- `anuncios_ativos_media_dia`;
- `novos_anuncios`;
- `dias_no_mercado_medio`.

Também foi verificada a relação esperada entre os percentis de preço:

`p25_m2 ≤ mediana_m2 ≤ p75_m2`

Não foram encontradas violações dessa regra nos registros em que as três métricas estavam disponíveis.

Os campos categóricos também foram avaliados após a padronização realizada na camada Silver. O campo `transacao` apresentou apenas as categorias `venda` e `aluguel`, enquanto as UFs encontradas foram BA, PE, PR, RJ e SP, coerentes com as cinco cidades utilizadas no projeto.

### 5.3 Unicidade

A unicidade foi avaliada considerando o grão dos dados:

`mes + cidade + uf + bairro + transacao`

Foram encontrados **36.933 registros e 36.933 combinações distintas** desse conjunto de atributos.

Portanto, não foram identificados registros duplicados no grão definido para os dados.

Essa verificação também contribui para garantir que a construção posterior da tabela fato não provoque duplicidade das observações analíticas.

### 5.4 Plausibilidade e Acurácia

Como o projeto utiliza uma única fonte de dados e não dispõe de uma segunda base independente para comparação, não é possível comprovar a acurácia absoluta dos valores de mercado.

Por esse motivo, a avaliação foi realizada sob a perspectiva de **plausibilidade e coerência interna**.

Foram verificadas regras como:

- inexistência de preços negativos;
- inexistência de quantidades negativas de anúncios;
- coerência entre os percentis de preço;
- categorias válidas de transação;
- correspondência das UFs com as cidades analisadas;
- comportamento dos valores extremos.

Essas verificações permitem identificar inconsistências estruturais ou valores incompatíveis com o contexto dos dados, sem assumir que a base representa um indicador oficial do mercado imobiliário.

### 5.5 Valores Extremos

A presença de valores extremos foi investigada utilizando o método do **Intervalo Interquartil (IQR)**.

Foram identificadas observações acima dos limites calculados para diferentes métricas, incluindo preços por metro quadrado, quantidade de anúncios e tempo médio de permanência no mercado.

Entretanto, a identificação estatística de um outlier não implica necessariamente erro nos dados. No mercado imobiliário, diferenças expressivas podem ocorrer em razão de características específicas das localidades, baixa quantidade de observações, imóveis de alto padrão ou particularidades dos bairros analisados.

Por esse motivo, os valores extremos **não foram removidos automaticamente**. Eles foram preservados na base e considerados durante a interpretação dos resultados.

Essa decisão evita eliminar observações potencialmente legítimas apenas por apresentarem comportamento estatisticamente distante da maior parte dos dados.

### 5.6 Conclusão da Avaliação de Qualidade

A avaliação demonstrou que o conjunto de dados apresenta estrutura adequada para as análises propostas, após os tratamentos realizados na camada Silver.

Os principais problemas identificados foram a presença de valores ausentes nas métricas de preço, os 11 registros originalmente identificados com preços iguais a zero e a existência de valores extremos.

Os preços iguais a zero foram convertidos para valores nulos, enquanto os demais valores ausentes foram mantidos sem imputação. Os valores extremos foram preservados, pois não havia evidência suficiente para classificá-los automaticamente como erros.

Além disso, não foram identificadas duplicidades no grão dos dados, valores negativos nas métricas analisadas ou violações na relação entre os percentis de preço.

Dessa forma, as decisões de tratamento buscaram preservar a informação original sempre que possível e evitar transformações que pudessem introduzir distorções artificiais nas análises.

## 6. Análise de Dados

As análises foram realizadas utilizando as tabelas dimensionais da camada Gold. Os resultados apresentados nesta seção correspondem exclusivamente ao conjunto de dados e ao período analisado, não devendo ser interpretados como indicadores oficiais de todo o mercado imobiliário das cidades.

### 6.1 Preço mediano por metro quadrado entre as cidades

**Pergunta:** Como o preço mediano por metro quadrado varia entre as cinco cidades analisadas?

A comparação dos valores medianos encontrados apresentou o seguinte resultado:

| Cidade | Preço mediano por m² |
|---|---:|
| Curitiba | R$ 5.543 |
| São Paulo | R$ 4.133 |
| Rio de Janeiro | R$ 3.333 |
| Recife | R$ 3.285 |
| Salvador | R$ 2.875 |

No conjunto de dados analisado, Curitiba apresentou o maior preço mediano por metro quadrado, enquanto Salvador apresentou o menor.

O valor observado em Curitiba foi aproximadamente **93% superior** ao encontrado em Salvador, evidenciando diferenças relevantes entre os mercados das cidades analisadas.

### 6.2 Preço por tipo de transação

**Pergunta:** Quais cidades apresentam os maiores e os menores preços medianos por metro quadrado para imóveis destinados à venda e ao aluguel?

Para imóveis destinados à **venda**, foram observados:

| Cidade | Preço mediano por m² |
|---|---:|
| Curitiba | R$ 8.137 |
| Recife | R$ 7.236 |
| São Paulo | R$ 5.556 |
| Salvador | R$ 4.616 |
| Rio de Janeiro | R$ 4.379 |

No segmento de venda, Curitiba apresentou o maior valor mediano entre as cidades analisadas, enquanto o Rio de Janeiro apresentou o menor.

Para imóveis destinados ao **aluguel**, os resultados foram:

| Cidade | Preço mediano por m² |
|---|---:|
| Recife | R$ 54 |
| Curitiba | R$ 42 |
| Salvador | R$ 42 |
| São Paulo | R$ 37 |
| Rio de Janeiro | R$ 31 |

No segmento de aluguel, Recife apresentou o maior valor mediano, enquanto o Rio de Janeiro apresentou o menor.

Os resultados demonstram que a posição relativa das cidades varia de acordo com o tipo de transação, reforçando a importância de analisar venda e aluguel separadamente.

### 6.3 Evolução temporal dos preços

**Pergunta:** Como o preço mediano por metro quadrado evoluiu ao longo do período analisado em cada cidade?

A análise temporal mostrou comportamentos distintos entre as cinco cidades.

Curitiba permaneceu com os maiores valores durante todo o período e apresentou crescimento nos meses finais da série. São Paulo apresentou comportamento relativamente estável, com valores próximos de R$ 4 mil por metro quadrado durante grande parte do período.

O Rio de Janeiro apresentou tendência geral de crescimento do preço mediano por metro quadrado no conjunto de dados analisado. Salvador permaneceu em patamar inferior às demais cidades durante grande parte da série, também apresentando elevação nos meses finais.

Recife apresentou comportamento diferente das demais localidades, com valores mais elevados no início da série e redução ao longo dos meses analisados.

Esses resultados mostram que a evolução dos preços não ocorreu de maneira uniforme entre as cidades.

### 6.4 Oferta de imóveis

**Pergunta:** Como se comporta a oferta de imóveis nas cidades analisadas, considerando a quantidade média de anúncios ativos e a entrada de novos anúncios?

A mediana da quantidade média diária de anúncios ativos apresentou os seguintes resultados:

| Cidade | Mediana de anúncios ativos |
|---|---:|
| Curitiba | 15 |
| São Paulo | 5 |
| Rio de Janeiro | 5 |
| Recife | 4 |
| Salvador | 4 |

Curitiba apresentou o maior nível mediano de anúncios ativos no conjunto analisado.

Para a entrada de novos anúncios, foram observadas as seguintes medianas:

| Cidade | Mediana de novos anúncios |
|---|---:|
| Curitiba | 1 |
| Recife | 1 |
| São Paulo | 0 |
| Rio de Janeiro | 0 |
| Salvador | 0 |

A mediana igual a zero não significa ausência total de novos anúncios durante o período. Ela indica que, considerando as observações disponíveis por bairro e período, pelo menos metade apresentou valor igual a zero para essa métrica.

### 6.5 Tempo de permanência no mercado

**Pergunta:** Existem diferenças no tempo médio de permanência dos imóveis no mercado entre as cidades e entre os tipos de transação?

Os valores medianos encontrados foram:

| Cidade | Aluguel (dias) | Venda (dias) |
|---|---:|---:|
| Curitiba | 285 | 441 |
| Recife | 170 | 291 |
| Rio de Janeiro | 225 | 651 |
| Salvador | 328 | 543 |
| São Paulo | 324 | 583 |

Em todas as cinco cidades analisadas, os imóveis destinados à venda apresentaram maior tempo mediano de permanência no mercado do que os imóveis destinados ao aluguel.

Entre os registros de venda, o Rio de Janeiro apresentou o maior valor mediano, com 651 dias, enquanto Recife apresentou o menor, com 291 dias.

No aluguel, Salvador apresentou 328 dias e São Paulo 324 dias, enquanto Recife apresentou o menor valor, com 170 dias.

Os resultados indicam diferenças tanto entre cidades quanto entre os tipos de transação.

### 6.6 Bairros com maiores preços medianos

**Pergunta:** Quais bairros apresentam os maiores preços medianos por metro quadrado em cada cidade?

A análise dos bairros revelou diferenças expressivas dentro das próprias cidades.

Os cinco bairros com maiores valores medianos encontrados em cada cidade foram:

| Cidade | Bairro | Preço mediano por m² |
|---|---|---:|
| Curitiba | Champagnat | R$ 18.304 |
| Curitiba | Batel | R$ 16.486 |
| Curitiba | Barigui | R$ 15.813 |
| Curitiba | Hugo Lange | R$ 12.846 |
| Curitiba | Cabral | R$ 12.338 |
| Recife | Ilha Joana Bezerra | R$ 20.704 |
| Recife | Cabanga | R$ 16.791 |
| Recife | Brasília Teimosa | R$ 13.203 |
| Recife | Monteiro | R$ 9.886 |
| Recife | Poço | R$ 9.528 |
| Rio de Janeiro | Ricardo de Albuquerque | R$ 183.246 |
| Rio de Janeiro | Magalhães Bastos | R$ 46.078 |
| Rio de Janeiro | Vidigal | R$ 39.004 |
| Rio de Janeiro | Urca | R$ 16.917 |
| Rio de Janeiro | Península-Barra | R$ 16.456 |
| Salvador | Corredor da Vitória | R$ 14.842 |
| Salvador | Aquarius | R$ 10.138 |
| Salvador | Alphaville 2 | R$ 10.072 |
| Salvador | Loteamento Aquarius | R$ 8.750 |
| Salvador | Jardim Armação | R$ 8.489 |
| São Paulo | Vila São Luís (Zona Oeste) | R$ 520.000 |
| São Paulo | Jardim Adutora | R$ 408.739 |
| São Paulo | Rural | R$ 153.226 |
| São Paulo | Av. Paulista | R$ 74.691 |
| São Paulo | Jardim Everest | R$ 63.714 |

Alguns bairros apresentam valores muito superiores ao comportamento predominante do conjunto de dados. Conforme discutido na avaliação de qualidade, esses registros foram identificados como valores extremos, mas não foram automaticamente removidos por não existir evidência suficiente para classificá-los como erros.

Portanto, esses resultados devem ser interpretados considerando as características e limitações da fonte utilizada.

### 6.7 Discussão Geral dos Resultados

As análises demonstram que os mercados imobiliários das cinco cidades apresentam comportamentos distintos em relação a preço, oferta e tempo de permanência dos imóveis.

Curitiba apresentou o maior preço mediano por metro quadrado quando as observações foram analisadas de forma agregada e também apresentou maior mediana de anúncios ativos. Entretanto, a segmentação por tipo de transação mostrou que a posição relativa das cidades muda entre venda e aluguel.

A análise temporal também demonstrou trajetórias diferentes entre as cidades, indicando que não existe um comportamento único de evolução dos preços no período analisado.

O tempo de permanência apresentou diferenças relevantes entre venda e aluguel, sendo superior para venda em todas as cidades analisadas.

Por fim, a análise por bairro revelou elevada heterogeneidade dentro das próprias cidades e também evidenciou a presença de valores extremos. Esses resultados reforçam a importância das etapas de avaliação da qualidade e contextualização dos dados antes da interpretação analítica.

Considerando as seis perguntas definidas no início do projeto, o pipeline construído permitiu transformar os arquivos de origem em uma estrutura organizada e analítica, possibilitando responder às questões propostas a partir das tabelas da camada Gold.

## 7. Autoavaliação

O desenvolvimento deste MVP permitiu atingir os objetivos definidos inicialmente, com a construção de um pipeline de dados em ambiente de nuvem capaz de realizar a ingestão, o tratamento, a modelagem, a avaliação da qualidade e a análise de dados do mercado imobiliário.

A utilização da arquitetura Medalhão permitiu organizar o fluxo de processamento em diferentes níveis de tratamento. A camada Bronze preservou os dados provenientes da fonte, a camada Silver concentrou os processos de limpeza e padronização e a camada Gold disponibilizou um modelo dimensional adequado ao consumo analítico.

As seis perguntas definidas no início do projeto puderam ser analisadas a partir das tabelas construídas na camada Gold, permitindo comparar preços entre cidades, analisar diferenças entre venda e aluguel, observar a evolução temporal dos preços, avaliar indicadores de oferta, comparar o tempo de permanência dos imóveis no mercado e identificar bairros com maiores preços medianos por metro quadrado.

Entre as principais dificuldades encontradas durante o desenvolvimento estiveram a compreensão da estrutura dos arquivos de origem, a definição dos tratamentos adequados para valores ausentes e valores iguais a zero e a interpretação dos valores extremos presentes na base.

A avaliação da qualidade foi importante para evitar decisões automáticas que pudessem alterar indevidamente os dados. Em especial, optou-se por preservar valores extremos quando não havia evidência suficiente para classificá-los como erros e por não realizar imputação artificial dos valores ausentes nas métricas de preço.

Outro ponto relevante foi a construção do modelo dimensional e a definição do grão da tabela fato, garantindo que as análises fossem realizadas sobre uma estrutura consistente e que os relacionamentos com as dimensões não provocassem perda ou multiplicação de registros.

Como evolução futura, o pipeline poderia incorporar novas cidades, ampliar o período histórico analisado e utilizar fontes adicionais para comparação e validação dos valores observados. Também seria possível implementar mecanismos de atualização periódica dos dados e desenvolver painéis analíticos para acompanhamento dos principais indicadores do mercado imobiliário.

De forma geral, o MVP possibilitou aplicar de maneira integrada os principais conceitos trabalhados na disciplina, incluindo ingestão de dados, arquitetura Medalhão, processamento com Apache Spark, armazenamento em Delta Lake, modelagem dimensional, catálogo de dados, avaliação de qualidade e análise de dados em ambiente de nuvem.
