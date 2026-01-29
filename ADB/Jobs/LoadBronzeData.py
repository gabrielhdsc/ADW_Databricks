# Databricks notebook source
# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


LoadBronzeData = Job.from_dict(
    {
        "name": "LoadBronzeData",
        "tasks": [
            {
                "task_key": "Load_SQL_LandingToBronze",
                "notebook_task": {
                    "notebook_path": "ADB/1. bronze schema/SQL/AutoLoaderSQLToBronze",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Load_APIcurrency_LandingToBronze",
                "depends_on": [
                    {
                        "task_key": "Load_SQL_LandingToBronze",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/1. bronze schema/API/AutoLoaderAPIToBronze",
                    "source": "GIT",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "Load_APIGeocoding_LandingBronze",
                "depends_on": [
                    {
                        "task_key": "Load_APIcurrency_LandingToBronze",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "ADB/1. bronze schema/API/AutoLoaderGeocodingAPIToBronze",
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
w.jobs.reset(new_settings=LoadBronzeData, job_id=1010199442938023)
# or create a new job using: w.jobs.create(**LoadBronzeData.as_shallow_dict())

