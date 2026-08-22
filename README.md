### Escopo

O MVP analisará somente informações públicas e agregadas sobre estudos clínicos. Não serão utilizados dados pessoais de pacientes nem será realizada recomendação médica.

## Etapa 1 — Objetivo do MVP

### Título

**Análise da evolução e distribuição das pesquisas clínicas sobre câncer de mama**

### Descricão do Problema

Os estudos clínicos sobre câncer de mama estão distribuídos entre diferentes países, instituições, fases e tipos de intervenção. Essa dispersão dificulta identificar:

* Onde as pesquisas estão concentradas.
* Quais tratamentos estão sendo mais investigados.
* Como os estudos evoluíram ao longo do tempo.
* Quantos estudos foram concluídos, interrompidos ou ainda estão recrutando.
* Quais regiões possuem menor participação nas pesquisas.

### Objetivo geral

Construir uma plataforma analítica no Databricks para coletar, organizar e analisar dados públicos de estudos clínicos relacionados ao câncer de mama.

O MVP permitirá acompanhar a evolução das pesquisas, identificar padrões nos tratamentos estudados e analisar a distribuição geográfica e operacional dos estudos.

> **Observação:** os dados não permitem afirmar que determinado tratamento cura a doença. O MVP analisará o cenário das pesquisas clínicas e seus resultados cadastrais, sem produzir conclusões médicas.

### Perguntas que o MVP deverá responder

1. Quantos estudos sobre câncer de mama foram registrados por ano?
2. Como os estudos estão distribuídos por país?
3. Quais países concentram a maior quantidade de pesquisas?
4. Quais são as fases clínicas mais frequentes?
5. Quais tipos de intervenção são mais estudados?
6. Quais tratamentos ou medicamentos aparecem com maior frequência?
7. Como os estudos estão distribuídos por situação: recrutando, concluído, suspenso ou encerrado?
8. Qual é a duração média dos estudos?
9. Qual é o número médio de participantes por fase clínica?
10. Quais organizações patrocinam mais estudos?
11. Existe relação entre fase clínica, quantidade de participantes e duração do estudo?
12. Como a participação do Brasil se compara à de outros países?

### Resultado esperado

* Pipeline de ingestão e transformação no Databricks.
* Dados organizados nas camadas Bronze, Silver e Gold.
* Modelo dimensional em esquema estrela.
* Catálogo e linhagem dos dados.
* Relatório de qualidade.
* Consultas SQL e visualizações respondendo às perguntas.
* Documentação completa do MVP.

Etapa 2 — Busca e seleção dos dados

Fontes públicas e gratuitas.

1. ClinicalTrials.gov

Fonte principal para os estudos clínicos sobre câncer de mama.

Responsável: U.S. National Library of Medicine.
Formato: JSON.
Coleta: API REST v2.
Autenticação: não necessária.
Documentação: ClinicalTrials.gov API
Estrutura dos dados: Study Data Structure
