from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
import os
from db_utils import PostgreSqlManager

POSTGRES_CONN_ID = "conn_postgres"
pg_manager = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)

def get_postgres_env():
    return pg_manager.get_postgres_connection_env()

def check_db_connection():
    is_connected = pg_manager.test_connection()
    if not is_connected:
        raise Exception(f"DB 연결 테스트 실패: {POSTGRES_CONN_ID}")
    print("DB 연결 테스트 성공")

with DAG(
    dag_id="dbt_run_traffic",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["dbt", "traffic"]
) as dag:

    # 1. DB 연결 테스트
    test_db_conn = PythonOperator(
        task_id="test_db_connection",
        python_callable=check_db_connection
    )

    # 2. traffic 관련 dbt 모델만 실행
    dbt_run_traffic = DockerOperator(
        task_id="dbt_run_traffic",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select *traffic* --project-dir /usr/app",
        docker_url="unix:///var/run/docker.sock",
        tls_hostname=False,
        tls_verify=False,
        network_mode="bridge",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=os.getenv("DBT_PROJECT_PATH"),
                target="/usr/app",
                type="bind"
            )
        ],
        environment={
            "DBT_PROFILES_DIR": "/usr/app",
            **get_postgres_env(),
        }
    )

    test_db_conn >> dbt_run_traffic
