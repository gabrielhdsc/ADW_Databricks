# Databricks notebook source
# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.70.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


Master = Job.from_dict(
    {
        "name": "Master",
        "tasks": [
            {
                "task_key": "Landing_To_Bronze",
                "run_job_task": {
                    "job_id": 302037461749897,
                },
            },
            {
                "task_key": "Bronze_To_Silver",
                "depends_on": [
                    {
                        "task_key": "Landing_To_Bronze",
                    },
                ],
                "run_job_task": {
                    "job_id": 1044128056778887,
                },
            },
            {
                "task_key": "Data_Modelling",
                "depends_on": [
                    {
                        "task_key": "Bronze_To_Silver",
                    },
                ],
                "run_job_task": {
                    "job_id": 511893068242503,
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=Master, job_id=304147148057247)
# or create a new job using: w.jobs.create(**Master.as_shallow_dict())

