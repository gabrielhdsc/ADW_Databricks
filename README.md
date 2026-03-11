# Adventure Works Solution Project: AI & Data Platform (Lakehouse)

## Introduction

This repository contains the source code and configurations for the **Adventure Works AI & Data Platform**, an end-to-end solution utilizing the **AdventureWorks dataset** to simulate a real-world data platform modernization project. This project solves the "data chaos" challenge caused by the company's rapid global expansion by establishing a Single Source of Truth and enabling advanced analytics.

The solution implements a scalable **Data Lakehouse** architecture on the Azure cloud, covering the entire data lifecycle: from the automated extraction of the transactional database (SQL) to delivering actionable data for dashboards, Machine Learning models, and Generative AI (LLM) interfaces.

## Key Use Cases & Business Value

The project materializes the value of data through three main pillars:
1. **Business Intelligence (BI):** An automated pipeline that cleanses, joins, and models transactional data, providing reliable metrics for executive dashboards.
2. **Predictive Intelligence (Machine Learning):**
   * **Sales Forecasting:** A Time Series model (SARIMAX) to predict future monthly revenue.
   * **New Customer Ranking:** A Classification model (Random Forest) that analyzes a customer's first 90 days to predict their Lifetime Value (LTV) tier as GOLD, SILVER, or BRONZE.
3. **GenAI & Consumption (Databricks Apps):** An interactive UI built with **Streamlit** that hosts the dashboards, integrates ML model API calls, and provides **Databricks Genie**—a conversational LLM assistant for natural language queries directly against the database.

## Architecture & Tech Stack

The solution leverages a modern data stack centered around Azure and Databricks:

* **Orchestration & Ingestion:** * **Azure Data Factory (ADF):** The orchestrator of the operation, responsible for extracting data from the Azure SQL Database via dynamic parameterized queries and loading it into the Landing Zone.
  * **Azure Data Lake Storage Gen2 (ADLS Gen2):** Scalable and secure data lake storage for raw data.
* **Processing & Lakehouse:** * **Azure Databricks (PySpark & SQL):** The unified analytics processing engine executing the Medallion architecture (Bronze, Silver, and Gold layers). Used for data cleansing, dimensional modeling (Star Schema), and Feature Engineering.
  * **Delta Lake:** The open-source storage layer that brings ACID transactions to the data lake.
  * **Databricks Lakeflow:** Utilized for building reliable, maintainable, and testable data pipelines.
* **Governance & MLOps:** * **Unity Catalog:** Unified governance solution for data and AI, managing access control, tables, and data lineage.
  * **Databricks Feature Store:** Centralization of customer behavior variables for model training.
  * **MLflow:** An open-source platform for managing the end-to-end machine learning lifecycle (experiment tracking, model versioning, and Model Registry).
  * **Model Serving:** Exposing ML models via REST API endpoints for real-time consumption.
* **Application Lifecycle Management & UI:**
  * **Streamlit (Databricks Apps):** A web application hosted natively within Databricks for secure business consumption.
  * **Azure DevOps:** The ALM platform (Azure Boards for tracking, Azure Pipelines for CI/CD, and Azure Repos for Git version control).
  * **Power BI Desktop (Optional):** Used for advanced data visualization and corporate reporting.


# Repository Structure

The code is organized to reflect the data pipeline lifecycle:

```text
├── ADB/
│   ├── 1. bronze schema/    # AutoLoader and Delta ingestion scripts (Landing -> Bronze)
│   ├── 2. silver schema/    # Data cleansing, standardization, and dimensional modeling
│   ├── 3. gold schema/      # Business views, aggregations, and Feature Engineering
|   ├── 4. dashboards/       # GO-LIVE profitable dashboard
|   ├── 5. features store/   # Features created to be consumed by the ML models
|   ├── Genie/               # Views used to improve the chat model
│   ├── Governance/          # Audit and logging notebooks triggered by ADF
│   ├── Jobs/                # Databricks Jobs code notebooks
│   ├── MachineLearning/     # Model training (SARIMAX, Random Forest) and MLflow integration
│   └── Module/              # Functions notebooks
├── ADF/                     # JSON definitions for ADF pipelines, Linked Services, and Triggers
│   ├── dataset/             # Define the data structure in a dataset
│   ├── factory/             # JSON identifier file
│   ├── linkedService/       # Connectors between the sources and sinks
│   ├── pipeline/            # JSON files describing the pipeline structure
│   └── trigger/             # Pipeline starter configuration
│── PBI/                     # Reports developed in Power BI Plataform
│── .gitignore  
│── publish_config.json
└── README.md
```

## Getting Started
Follow the steps below to set up your environment and interact with the code in this repository.

### 1. Prerequisites

Before you begin, ensure you have access to the following:

* **Azure Subscription:** With permissions to create and manage Azure resources.
* **Azure DevOps Organization & Project:** Access to this specific Azure DevOps project (adventureworks-databricks-training).
* **Azure Databricks Workspace:** A Databricks workspace provisioned within your Azure subscription.
* **Git Client (Optional):** e.g., Git Bash, GitHub Desktop, or built-in Git in VS Code for local repository operations.


### 2. Setting up your Databricks Environment

To work with the notebooks and code, you must connect your Azure Databricks workspace to this Azure DevOps repository.

**Step A: Generate a Personal Access Token (PAT) in Azure DevOps**

1. Navigate to your **Azure DevOps Organization settings.**
2. Click on **Personal access tokens** and select **New Token.**
3. Name: `DatabricksRepoIntegration-<YourName>` (e.g., DatabricksRepoIntegration-JohnJoe).
4. **Organization:** Select your Azure DevOps organization.
5. **Expiration:** Set an appropriate expiration date.
6. **Scopes:** Under "Code", select Read & write.
7. Click **Create** and **COPY THE GENERATED TOKEN IMMEDIATELY!** (You will not be able to see it again).


**Step B: Configure Git Integration in Azure Databricks**

1. Log in to your Azure Databricks workspace.
2. In the bottom left corner, click on your **User Icon** (or initials) and select **User Settings.**
3. Go to the **Git integration** tab.
4. **Git provider:** Select Azure DevOps.
5. **Azure DevOps organization URL:** Enter your organization's URL (e.g., https://dev.azure.com/your-organization).
6. **Personal Access Token:** Paste the PAT you copied from Azure DevOps.
7. Click Save.


**Step C: Clone this Repository into Databricks Repos**

1. In your Azure Databricks workspace, navigate to **Repos** in the left sidebar.
2. Click **Add Repo**.
3. **Git repository URL:** Paste the HTTPS clone URL of this Azure DevOps repository. (You can get this URL by going to Repos > Files in Azure DevOps, clicking the "Clone" button, and copying the HTTPS URL).
4. **Git provider:** Azure DevOps (should be pre-selected).
5. Click **Create Repo.**
6. You should now see the repository's file structure (including `ADB/`, `ADF_Templates/`, etc.) within your Databricks workspace.


### 3. Executing the Data Pipeline (Azure Data Factory)

The entire end-to-end data workflow—from on-premises SQL ingestion to the final Lakehouse Gold layer—is centrally orchestrated by Azure Data Factory.

To run the project in your environment:
1. Open your **Azure Data Factory Studio**.
2. Navigate to the **Author** tab (pencil icon) on the left sidebar.
3. Under the **Pipelines** section, locate and open the master pipeline named `ETLMasterPipeline`.
4. To run the process manually, click on **Add trigger** at the top menu of the pipeline canvas and select **Trigger now**. Click **OK** to confirm the run parameters.
5. To monitor the execution, go to the **Monitor** tab on the left sidebar. Here you can track the step-by-step progress of the SQL data ingestion, the Databricks Medallion processing (Bronze, Silver, Gold), and the governance logs.
6. Alternatively, the pipeline is configured with a `ScheduleTrigger` to run automatically every day at 00:01. Ensure the trigger is activated in the **Manage** tab if you want automated daily runs.

Once the `ETLMasterPipeline` succeeds, the processed data and ML models will be fully updated in the Databricks Lakehouse, ready to be consumed by the dashboards and ML endpoints and the Streamlit App!