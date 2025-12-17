from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os
from db_utils import PostgreSqlManager

POSTGRES_CONN_ID = "conn_postgres"
POSTGRES_ENV = PostgreSqlManager.get_postgres_connection_env(POSTGRES_CONN_ID)

def check_db_connection():
    is_connected = PostgreSqlManager.test_connection(POSTGRES_ENV)
    if not is_connected:
        raise Exception("DB 연결 테스트 실패")
    print("DB 연결 테스트 성공")

with DAG(
    dag_id="test_dbt_run",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "test"]
) as dag:
    
    # 연결 테스트
    test_db_conn = PythonOperator(
        task_id="test_db_connection",
        python_callable=check_db_connection
    )

    # dbt 실행 테스트 (stg_sample.sql 실행)
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select models/staging/stg_sample.sql --project-dir /usr/app",
        docker_url="unix:///var/run/docker.sock",
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
            **POSTGRES_ENV,
        }
    )

    # 순서 보장: 연결 테스트 성공 후 dbt 실행
    test_db_conn >> dbt_run
