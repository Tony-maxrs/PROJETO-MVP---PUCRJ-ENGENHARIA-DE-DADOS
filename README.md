# MVP – Pipeline de Dados no Databricks

Projeto desenvolvido como MVP acadêmico utilizando a plataforma Databricks para ingestão, tratamento e análise de dados financeiros (Receita e Despesa).

## Tecnologias utilizadas
- Databricks
- Apache Spark (PySpark)
- SQL

## Objetivo
Analisar o fluxo de caixa da empresa, calculando totais de receita, despesa, resultado financeiro e rankings de clientes e fornecedores.

## Estrutura do repositório
- notebooks/: códigos do pipeline
- dados/: arquivos CSV utilizados
- 
## Contexto de Negócio

O mercado imobiliário apresenta diferenças significativas de preços, oferta e dinâmica de comercialização entre diferentes localidades. A análise dessas informações pode auxiliar empresas do setor imobiliário, incorporadoras, investidores e demais agentes do mercado na compreensão do comportamento de diferentes regiões e na identificação de padrões relevantes para a tomada de decisão.

Este projeto tem como objetivo construir um pipeline de dados em ambiente de nuvem para coletar, organizar, transformar e analisar dados do mercado imobiliário de cinco cidades brasileiras: Salvador, São Paulo, Rio de Janeiro, Recife e Curitiba.

Os dados utilizados contêm informações mensais por bairro relacionadas aos preços por metro quadrado, quantidade de anúncios, entrada de novos anúncios e tempo médio de permanência dos imóveis no mercado.

A partir desses dados, será implementada uma arquitetura de dados em camadas Bronze, Silver e Gold, utilizando o Databricks Free Edition. A camada Bronze armazenará os dados em seu estado original; a Silver será responsável pela limpeza, padronização, integração e validação; e a Gold disponibilizará os dados estruturados para análises por meio de um modelo dimensional.

O resultado esperado é disponibilizar uma estrutura analítica capaz de comparar diferentes mercados imobiliários e identificar padrões relacionados a preços, oferta, liquidez e evolução temporal.

## Perguntas do Projeto 
Quais cidades e bairros apresentam os maiores e menores preços medianos por metro quadrado para venda e aluguel?
Como o preço mediano por metro quadrado evoluiu ao longo do período analisado nas cinco cidades?
Quais cidades e bairros apresentam maior volume de novos anúncios e maior oferta média de imóveis?
Quais localidades apresentam maior e menor tempo médio de permanência dos imóveis no mercado?
Existe relação entre o preço mediano por metro quadrado e o tempo médio de permanência dos imóveis no mercado?
Quais localidades apresentam simultaneamente crescimento do preço mediano por metro quadrado e aumento da oferta de imóveis ao longo do período?
