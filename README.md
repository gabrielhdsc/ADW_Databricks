# AdventureWorks Lakehouse Migration & Databricks Certification Prep

## Introduction

This repository serves as the central hub for our **Databricks Certification Training**, leveraging the familiar **AdventureWorks dataset** to simulate a real-world data platform modernization project. The primary objective of this training is to equip participants with the practical skills and theoretical knowledge required to successfully implement a modern Data Lakehouse architecture on Azure Databricks, ultimately preparing them for the official Databricks certifications (e.g., Data Engineer Associate, Machine Learning Associate).

Through a series of hands-on labs and practical exercises, we will cover the entire data lifecycle, from raw data ingestion to advanced analytics and machine learning, ensuring a deep understanding of Databricks best practices and key concepts such as Delta Lake, Delta Live Tables (DLT), Unity Catalog, and MLflow.

## Getting Started

This section guides you through setting up your environment to participate in the training labs.

### 1. Prerequisites

Before you begin, ensure you have access to the following:

* **Azure Subscription:** With permissions to create and manage Azure resources.
* **Azure DevOps Organization & Project:** Access to this specific Azure DevOps project (`adventureworks-databricks-training`).
* **Azure Databricks Workspace:** A Databricks workspace provisioned within your Azure subscription.

### 2. Software Dependencies

While most of the development will happen directly within the Azure Databricks workspace, some local tools might be beneficial:

* **Web Browser:** (e.g., Google Chrome, Microsoft Edge) for accessing Azure Portal, Azure DevOps, and Databricks Workspace UI.
* **Git Client:** (e.g., Git Bash, GitHub Desktop, GitKraken, built-in Git in VS Code) for local repository operations, if you choose to work outside Databricks Repos.
* **Python (Optional):** If you plan to run local scripts or use Databricks Connect. Recommended version: Python 3.8+.
* **Databricks CLI (Optional):** Useful for advanced automation, though not strictly required for most labs. Installation via `pip install databricks-cli`.
* **Power BI Desktop (Optional):** For labs focused on data visualization and reporting.

### 3. Setting up your Databricks Environment

To work with the notebooks and code in this repository, you must connect your Azure Databricks workspace to this Azure DevOps repository.

1.  **Generate a Personal Access Token (PAT) in Azure DevOps:**
    * Navigate to your **Azure DevOps Organization settings**.
    * Click on **"Personal access tokens"**.
    * Click **"New Token"**.
    * **Name:** `DatabricksRepoIntegration-<YourName>` (e.g., `DatabricksRepoIntegration-JohnDoe`).
    * **Organization:** Select your Azure DevOps organization.
    * **Expiration:** Set an appropriate expiration date (e.g., end of the training + buffer).
    * **Scopes:** Under "Code", select **"Read & write"**.
    * Click **"Create"** and **COPY THE GENERATED TOKEN IMMEDIATELY!** You will not be able to see it again.

2.  **Configure Git Integration in Azure Databricks:**
    * Log in to your Azure Databricks workspace.
    * In the bottom left corner, click on your **User Icon** (or initials) and select **"User Settings"**.
    * Go to the **"Git integration"** tab.
    * **Git provider:** Select `Azure DevOps`.
    * **Azure DevOps organization URL:** Enter your organization's URL (e.g., `https://dev.azure.com/your-organization`).
    * **Personal Access Token:** Paste the PAT you copied from Azure DevOps.
    * Click **"Save"**.

3.  **Clone this Repository into Databricks Repos:**
    * In your Azure Databricks workspace, navigate to **"Repos"** in the left sidebar.
    * Click **"Add Repo"**.
    * **Git repository URL:** Paste the HTTPS clone URL of *this* Azure DevOps repository.
        * You can get this URL by going to **Repos** > **Files** in Azure DevOps, clicking the **"Clone"** button (top right), and copying the HTTPS URL.
        * Example: `https://<YourOrgName>@dev.azure.com/<YourOrgName>/<YourProjectName>/_git/<ThisRepoName>`
    * **Git provider:** `Azure DevOps` (should be pre-selected).
    * Click **"Create Repo"**.
    * You should now see the repository's file structure (including `notebooks/`, `src/`, etc.) within your Databricks workspace.

### 4. Working with Labs

Labs are organized within the `notebooks/` directory, typically structured by module (e.g., `notebooks/module1_data_ingestion/`).

* **Branching Strategy:** For each lab assignment, please create a new feature branch from `main`. A recommended naming convention is `feature/<your-name>/<lab-id>` (e.g., `feature/john-doe/lab-1.1`).
* **Committing Changes:** As you work on your notebooks within Databricks Repos, frequently commit and push your changes to your feature branch.
* **Submitting Labs:** Once a lab is complete, create a **Pull Request (PR)** from your feature branch back into the `main` branch (or a designated `submission` branch, as per instructor guidance) within Azure DevOps. Ensure your PR description is clear and links to any relevant Work Items.

## Project Structure & Tools

This project leverages a modern data stack centered around Azure and Databricks.

### Core Components:

* **Azure Databricks:** The unified analytics platform for data engineering, machine learning, and data warehousing.
    * **Databricks Repos:** For Git integration and collaborative notebook development.
    * **Delta Lake:** The open-source storage layer that brings ACID transactions to data lakes.
    * **Delta Live Tables (DLT):** A framework for building reliable, maintainable, and testable data pipelines.
    * **Unity Catalog:** A unified governance solution for data and AI on the Lakehouse.
    * **MLflow:** An open-source platform for managing the end-to-end machine learning lifecycle (experiment tracking, model management, model deployment).
    * **Databricks SQL:** For running SQL queries and creating dashboards directly on your Lakehouse data.
* **Azure Data Lake Storage Gen2 (ADLS Gen2):** Scalable and secure data lake storage for raw and processed data.
* **Azure DevOps:** Our Application Lifecycle Management (ALM) platform.
    * **Azure Boards:** For tracking Epics, Features, and User Stories (lab assignments).
    * **Azure Repos (Git):** The version control system for all project code and notebooks.
    * **Azure Pipelines:** For Continuous Integration (CI) and Continuous Delivery (CD) of data pipelines and ML workflows (e.g., notebook deployment, job orchestration).

### Dataset:

* **AdventureWorks:** A well-known sample OLTP database from Microsoft, adapted for our Lakehouse environment. It provides realistic scenarios for data ingestion, transformation, and analysis.

## Build and Test

* **Notebook Execution:** Most of the "build" process involves executing Databricks notebooks, which will perform data transformations, model training, etc.
* **Delta Live Tables:** DLT pipelines are defined within notebooks and are executed via the DLT runtime in Databricks. Their health and status can be monitored directly in the Databricks UI.
* **Azure Pipelines:** CI/CD pipelines defined in YAML (`azure-pipelines.yml`) will automate the deployment of notebooks to Databricks Repos or specific workspace paths, and can trigger Databricks Jobs for automated testing or full pipeline runs.
* **Testing:**
    * **Data Quality Checks:** Implemented directly within notebooks or DLT pipelines (as defined in Feature 1.2).
    * **Unit Tests (Optional):** For reusable Python/Scala code in the `src/` directory, standard unit testing frameworks (e.g., `pytest`) can be used and integrated into Azure Pipelines.
    * **Model Evaluation:** MLflow is used to track model metrics, allowing for programmatic and visual comparison of model performance.

## Contribute

This training project is designed for hands-on learning and collaboration.

* **Lab Completion:** Your primary contribution will be completing the lab assignments and submitting your work via Pull Requests in Azure DevOps.
* **Feedback:** We encourage you to provide feedback on the labs, instructions, and overall training experience. Use Azure Boards to create new Work Items (e.g., "Bug" for issues, "User Story" for suggestions for improvement).
* **Best Practices:** Please adhere to the following guidelines:
    * **Branching:** Always create a new feature branch for your work.
    * **Commit Messages:** Write clear, concise commit messages that explain what changes were made and why.
    * **Pull Requests:** Create well-described Pull Requests, linking to the relevant Work Item (lab assignment) in Azure Boards.
    * **Code Quality:** Strive for clean, readable, and well-commented code in your notebooks.

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)