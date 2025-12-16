from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os


with DAG(
    dag_id="dbt_run_test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "test"]
) as dag:

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select models/staging/stg_sample.sql --project-dir /usr/app",
        docker_url="unix:///var/run/docker.sock",  # 명시적으로 지정
        tls_hostname=False,
        tls_verify=False,
        network_mode="bridge",
        mount_tmp_dir=False,  # 임시 디렉토리 마운트 비활성화
        mounts=[
            Mount(
                source=os.getenv("DBT_PROJECT_PATH"),
                target="/usr/app",
                type="bind"
            )
        ],
        environment={
            "DBT_PROFILES_DIR": "/usr/app",
            "PGHOST": os.getenv("PGHOST"),
            "PGPORT": os.getenv("PGPORT", "5432"),
            "PGUSER": os.getenv("PGUSER"),
            "PGPASSWORD": os.getenv("PGPASSWORD"),
            "PGDATABASE": os.getenv("PGDATABASE"),
        }
    )