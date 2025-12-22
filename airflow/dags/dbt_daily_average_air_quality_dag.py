from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os
from slack import on_failure_callback
from db_utils import PostgreSqlManager

POSTGRES_CONN_ID = "conn_postgres"
pg_manager = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)

def get_postgres_env():
    return pg_manager.get_postgres_connection_env()


with DAG(
    dag_id="dbt_daily_average_air_quality_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 10 * * *",   # daily_average_air_quality_dag 와 동일
    catchup=False,
    tags=["seoul", "weather", "dbt"],
    on_failure_callback=on_failure_callback,
    default_args={
        "on_failure_callback": on_failure_callback
    }
) as dag:
    # postgres 적재가 끝날 때까지 기다림
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="daily_average_air_quality_dag",
        external_task_ids=["insert_to_db"],
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
        check_existence=True,
        execution_delta=timedelta(minutes=0),
    )

    # stg_daily_air_quality 실행
    stg_dbt_run = DockerOperator(
        task_id="stg_dbt_run",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select stg_daily_air_quality --project-dir /usr/app",
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

    # int_air_quality과 그 하위(marts)를 한꺼번에 실행
    int_dbt_run = DockerOperator(
        task_id="int_dbt_run",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select int_air_quality+ --project-dir /usr/app",
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

    wait_for_ingest >> stg_dbt_run >> int_dbt_run