# Databricks notebook source
# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


LoadSilverClean = Job.from_dict(
    {
        "name": "LoadSilverClean",
        "tasks": [
            {
                "task_key": "HR_BronzeToSilver",
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/SQLBronzeToSilverSchemas/HumanResources",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Person_BronzeToSilver",
                "depends_on": [
                    {
                        "task_key": "HR_BronzeToSilver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/SQLBronzeToSilverSchemas/Person",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Products_BronzeToSilver",
                "depends_on": [
                    {
                        "task_key": "Person_BronzeToSilver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/SQLBronzeToSilverSchemas/Products",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Purchasing_BronzeToSilver",
                "depends_on": [
                    {
                        "task_key": "Products_BronzeToSilver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/SQLBronzeToSilverSchemas/Purchasing",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Sales_BronzeToSilver",
                "depends_on": [
                    {
                        "task_key": "Purchasing_BronzeToSilver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/SQLBronzeToSilverSchemas/Sales",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "CurrencyAPI_BronzeToSilverClean",
                "depends_on": [
                    {
                        "task_key": "Sales_BronzeToSilver",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/APIBronzeToSilver/APIBronzeToSilver",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "WeatherAPI_BronzeToSilverClean",
                "depends_on": [
                    {
                        "task_key": "CurrencyAPI_BronzeToSilverClean",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.1 SilverCleaning/APIBronzeToSilver/WeatherAPIToSilver",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "17.3.x-scala2.13",
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "spot_bid_max_price": -1,
                    },
                    "node_type_id": "Standard_D4s_v3",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                    },
                    "data_security_mode": "DATA_SECURITY_MODE_DEDICATED",
                    "runtime_engine": "PHOTON",
                    "kind": "CLASSIC_PREVIEW",
                    "is_single_node": True,
                },
            },
        ],
        "git_source": {
            "git_url": "https://programmersitts.visualstudio.com/Programmers.Internals.Data/_git/Programmers.Internals.Data",
            "git_provider": "azureDevOpsServices",
            "git_branch": "gabriel.santos/PipelineOrchestrationV2",
        },
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=LoadSilverClean, job_id=119354050049484)
# or create a new job using: w.jobs.create(**LoadSilverClean.as_shallow_dict())

