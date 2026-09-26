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
