# Adventure Works Solution Project: AI & Data Platform (Lakehouse)

## Introduction

Este repositório contém o código-fonte e as configurações da Plataforma de Dados e IA da Adventure Works, uma solução end-to-end que utiliza o dataset da AdventureWorks para simular um projeto real de modernização de dados. Este projeto resolve o desafio do "caos de dados" causado pela rápida expansão global da empresa, estabelecendo uma Fonte Única de Verdade (Single Source of Truth) e habilitando análises avançadas.

O projeto implementa uma arquitetura Lakehouse escalável na nuvem Azure baseada na abordagem Medallion (Bronze, Silver e Gold), integrando múltiplas fontes de dados, pipelines de ingestão, processamento com PySpark/SQL, modelagem dimensional, governança, Business Intelligence, Feature Engineering, Machine Learning e consumo analítico com Databricks Dashboards e Power BI.

## Valor de Negócio

O projeto materializa o valor dos dados através de três pilares principais:
1. **Business Intelligence (BI):** Um pipeline automatizado que limpa, cruza e modela dados transacionais, fornecendo métricas confiáveis e padronizadas em painéis executivos para análises de vendas, compras, receita, custos, rentabilidade, produtos, clientes, regiões e territórios.
2. **Inteligência Preditiva (Machine Learning):**
   * **Previsão de Vendas:** Um modelo de Séries Temporais (SARIMAX) para prever as receitas mensais futuras.
   * **Classificação de Novos Clientes:** Um modelo de Classificação (Random Forest) que analisa os primeiros 90 dias de um cliente para prever seu *Lifetime Value* (LTV), classificando-o como OURO, PRATA ou BRONZE.
3. **GenAI e Consumo (Databricks Apps):** Criação de um ambiente interativo (Streamlit) que hospeda os dashboards, integra chamadas de API dos modelos de ML e disponibiliza o Databricks Genie, um assistente conversacional (LLM) para consultas em linguagem natural diretamente no banco de dados.

## Arquitetura e Stack Tecnológico
A solução utiliza uma stack moderna de dados centralizada no ecossistema Azure e Databricks:

* **Orquestração e Ingestão:**
  * **Azure Data Factory (ADF):** Responsável por extrair dados do Azure SQL DB (via queries parametrizadas), CSVs e de APIs (Clima, Moeda, Geolocalização), carregando-os na Landing Zone.
  * **Azure Data Lake Storage Gen2 (ADLS Gen2):** Armazenamento de dados escalável e seguro para os dados brutos.
* **Processamento e Lakehouse:**
  * **Azure Databricks (PySpark e SQL):** O motor unificado de processamento analítico que executa a arquitetura Medallion (Camadas Bronze, Silver e Gold). Realiza desde a limpeza e modelagem dimensional (*Star Schema*) até Feature Engineering.
  * **Delta Lake:** Camada de armazenamento *open-source* que traz transações ACID para o Data Lake.
* **Governança e MLOps:**
  * **Unity Catalog:** Solução unificada de governança para dados e IA (controle de acesso, tabelas e linhagem).
  * **Databricks Feature Store:** Centralização de variáveis de comportamento de clientes para treinamento de modelos.
  * **MLflow:** Plataforma *open-source* para gestão do ciclo de vida de ML (rastreamento de experimentos, versionamento de modelos e Model Registry).
* **Consumo e Visualização:**
  * **Power BI Desktop:** Utilizado para relatórios corporativos avançados (projeto versionado via formato PBIP).
  * **Databricks Dashboards:** Painéis nativos no Databricks.
* **Manejo e UI:**
  * **Streamlit:** Aplicação web para consumo dos produtos de dados criados (chatbot, Dashboards e modelos de ML).

---

## Arquitetura Medallion

### Bronze

A camada Bronze recebe dados de diferentes fontes com o objetivo de preservar os dados originais e disponibilizá-los para o processamento posterior.
* **APIs:** `AutoLoaderAPIToBronze.py`, `AutoLoaderGeocodingAPIToBronze.py`, `AutoLoaderWeatherAPIToBronze.py`
* **CSV:** `AutoLoaderCSVSalesOrderAndSalesHeaderToBronze.py`
* **SQL:** `AutoLoaderSQLToBronze.py`

O objetivo da Bronze é manter os dados em uma estrutura próxima à origem, servindo como primeira camada persistente do processamento no Lakehouse.

### Silver

A Silver está dividida em duas etapas:

1. **Silver Cleaning:** Responsável pela limpeza e padronização dos dados provenientes da Bronze. Inclui entidades relacionadas a Recursos Humanos, Pessoas, Produtos, Compras, Vendas e APIs.
2. **Silver Business:** Responsável pela construção do modelo dimensional. O resultado é um modelo analítico baseado em **Star Schema**, adequado para consultas e ferramentas de BI:
   * **Dimensões:** `Dim_Currency`, `Dim_Customer`, `Dim_Date`, `Dim_Location`, `Dim_Products`, `Dim_Supplier`, `Dim_Territory`.
   * **Fatos:** `FactSalesOrderDetail`, `FactSalesOrderHeader`, `FactPurchases`.

### Gold

A camada Gold transforma o modelo Silver em estruturas orientadas aos casos de uso do negócio. Essas estruturas servem como camada de consumo para análises de vendas, compras e rentabilidade. Entre as visões existentes estão:
* `SalesViewForBI.py`
* `PurchasesViewForBI.py`
* `ProfitabilityViewForDash.py`
* `ProfitabilityRevenueXCostViewForDash.py`
* `ProfitailityNotProfProductsViewForDash.py`

---

## Componentes Analíticos e IA

### Dashboards

O projeto possui um dashboard desenvolvido nativamente no ambiente Databricks (`ADB/4. dashboards/Profitability-GO-LIVE.lvdash.json`). Também existe um projeto Power BI versionado em `PBI/AdventureWorks.pbip`. O modelo semântico do Power BI contém tabelas dimensionais e fatos perfeitamente alinhados ao modelo construído no Lakehouse.

### Feature Engineering e Machine Learning

As *features* utilizadas pelos modelos são organizadas em `ADB/5. features store/`. Os scripts incluem variáveis relacionadas a demanda, estoque, vendas, clientes, produtos e regiões.

Os componentes de Machine Learning estão em `ADB/MachineLearning/` (`GenerateSinteticData.py`, `PredictionModel.py`). Os casos de uso descritos no projeto incluem:
- Previsão de receita utilizando **SARIMAX**.
- Classificação de clientes (*LTV Tier*) utilizando **Random Forest**.

### Databricks Genie

O projeto possui views específicas otimizadas para consumo pela IA generativa do Databricks Genie em `ADB/Genie/` (ex: `vwcustomergenie.py`, `vwsalesgenie.py`). Essas *views* organizam e desnormalizam os dados de forma mais adequada para consultas analíticas realizadas em linguagem natural pelo usuário.

### Governança

Os componentes e logs de governança estão em `ADB/Governance/` (`ADF_Execution.py`, `GovernanceTables.sql`). 
O projeto também possui um relatório específico de governança em Power BI (`Governance_report/`), cujo modelo semântico monitora informações relacionadas a:
- Execução dos pipelines do Azure Data Factory;
- Qualidade dos dados e Linhagem;
- Métricas de governança.

---

# Estrutura do repositório
```text
├── ADB/                     # Scripts executados no Azure Databricks (PySpark/SQL)
│   ├── 1. bronze schema/    # Ingestão via AutoLoader (APIs, CSV, SQL)
│   ├── 2. silver schema/    # Limpeza e Modelagem Dimensional (Fatos e Dimensões)
│   ├── 3. gold schema/      # Visões de negócio finais e agregações
│   ├── 4. dashboards/       # Arquivos de dashboards nativos do Databricks
│   ├── 5. features store/   # Construção de features consumidas pelos modelos de ML
│   ├── Genie/               # Views otimizadas para alimentar a IA (Databricks Genie)
│   ├── Governance/          # Logs de execução e tabelas de governança via ADF
│   ├── Jobs/                # Notebooks orquestradores (Master)
│   ├── MachineLearning/     # Treinamento de modelos (SARIMAX, RF) integrados ao MLflow
│   └── Module/              # Funções reaproveitáveis e utilitários
├── ADF/                     # Orquestração via Azure Data Factory (Infraestrutura como Código)
│   ├── dataset/             # Definições das estruturas de dados nas pontas (Source/Sink)
│   ├── linkedService/       # Conectores para ADLS, SQL, Databricks e APIs
│   ├── pipeline/            # Fluxo visual de execução das cargas
│   └── trigger/             # Gatilhos de agendamento automático
├── PBI/                     # Projetos do Power BI (PBIP) com modelos semânticos e relatórios
│   ├── AdventureWorks.Report/
│   └── AdventureWorks.SemanticModel/
├── publish_config.json      # Arquivo de configuração de publicação
├── TechnicalDocumentation.md# Documentação técnica aprofundada
└── README.md
```
---

# Tecnologias

### Cloud & Data Platform

- Microsoft Azure
- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Delta Lake

### Data Engineering

- Python
- PySpark
- SQL
- Databricks Auto Loader
- Medallion Architecture
- Dimensional Modeling / Star Schema

### Analytics

- Databricks Dashboards
- Power BI
- Power BI Semantic Model

### Machine Learning

- SARIMAX
- Random Forest
- Feature Engineering
- Feature Store

### Governance

- Data Quality
- Data Lineage
- Pipeline Logging
- Governance Reporting

### GenAI

- Databricks Genie
- Views específicas para consumo por linguagem natural

---

# Requisitos

Para executar ou implantar a solução são necessários, dependendo do componente utilizado:

- Uma assinatura Azure com permissões para gerenciar recursos.;
- Um workspace Azure Databricks provisionado e com o Unity Catalog habilitado;
- Uma conta Azure Data Lake Storage Gen2;
- Acesso às fontes de dados (SQL Server e APIs) utilizadas;
- Power BI Desktop para trabalhar com os projetos `.pbip`.

Nota: Como os recursos Azure e suas credenciais não fazem parte do repositório, a execução completa depende da configuração e provisionamento da infraestrutura correspondente.

A pasta `ADF/` contém as definições de infraestrutura e configuração do Azure Data Factory em formato JSON (datasets, linked services, pipelines e triggers). Os scripts Python da pasta `ADB/` devem ser sincronizados/clonados no ambiente Azure Databricks correspondente (via Databricks Repos/Git Folders).

---

# Objetivo do projeto

Este projeto foi desenvolvido como uma demonstração de uma plataforma moderna de dados, mostrando como diferentes componentes de um ecossistema cloud podem ser integrados para construir um fluxo completo:

Ingestão → Armazenamento → Processamento → Modelagem → Governança → BI → Machine Learning → GenAI

O foco não está apenas na transformação dos dados, mas na construção de uma arquitetura que permita que os mesmos dados sejam utilizados por diferentes consumidores e casos de uso de negócio.
