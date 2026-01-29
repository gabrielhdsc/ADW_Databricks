# Databricks notebook source
# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


LoadSilverDataModel = Job.from_dict(
    {
        "name": "LoadSilverDataModel",
        "tasks": [
            {
                "task_key": "Dim_Currency_SilverCleanToSilverBusiness",
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.2 SilverBusiness/SilverCleanToSilverDims/Dim_Currency",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Dim_Customer_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Currency_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/SilverBusiness/SilverCleanToSilverDims/Dim_Customer",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Dim_Date_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Customer_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/SilverBusiness/SilverCleanToSilverDims/Dim_Date",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Dim_Location_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Date_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/SilverBusiness/SilverCleanToSilverDims/Dim_Location",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Dim_Products_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Location_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/SilverBusiness/SilverCleanToSilverDims/Dim_Products",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Dim_Supplier_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Products_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/SilverBusiness/SilverCleanToSilverDims/Dim_Supplier",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Fact_Purchase_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Dim_Supplier_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.2 SilverBusiness/SilverCleanToSilverFacts/SilverCleanToSilverFactPurchases",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Fact_SalesOrderHeader_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Fact_Purchase_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.2 SilverBusiness/SilverCleanToSilverFacts/SilverCleanToFactSalesOrderHeader",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Fact_SalesOrderDetail_SilverCleanToSilverBusiness",
                "depends_on": [
                    {
                        "task_key": "Fact_SalesOrderHeader_SilverCleanToSilverBusiness",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/2. silver schema/2.2 SilverBusiness/SilverCleanToSilverFacts/SilverCleanToFactSalesOrderDetail",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "spark_version": "17.3.x-scala2.13",
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "spot_bid_max_price": -1,
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
                    },
                    "enable_elastic_disk": True,
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
w.jobs.reset(new_settings=LoadSilverDataModel, job_id=1025727506202528)
# or create a new job using: w.jobs.create(**LoadSilverDataModel.as_shallow_dict())

