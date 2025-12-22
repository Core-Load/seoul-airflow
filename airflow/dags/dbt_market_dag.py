from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import ShortCircuitOperator
from docker.types import Mount
import os
from db_utils import PostgreSqlManager
from slack import on_failure_callback
from common_utils import skip_at_kst_21

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
    dag_id = "dbt_run_market",
    start_date = datetime(2025,12,19),
    schedule_interval = "*/30 * * * *",
    catchup=False,
    tags=["dbt", "market", "seoul_city_data"],
    on_failure_callback=on_failure_callback,
    default_args={
        "on_failure_callback": on_failure_callback
    }
) as dag:
    db_conn = PythonOperator(
        task_id = "db_connection",
        python_callable=check_db_connection
    )
    
    check_time = ShortCircuitOperator(
        task_id="check_not_21",
        python_callable=skip_at_kst_21
    )
    
    wait_for_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="seoul_citydata_to_s3_postgres",
        external_task_id="insert_to_postgres",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",  # worker 점유 방지
        poke_interval=60,   # 1분마다 체크
        timeout=60 * 60,    # 최대 1시간 대기
        check_existence=True,
    )
    
    # 2. dbt 실행 테스트 (파일명에 sample이 포함된 모든 모델 실행)
    dbt_run = DockerOperator(
        task_id="dbt_run",
        image="dbt-runner:latest",
        api_version="auto",
        auto_remove="success",
        command="run --select +fact_market_ppl +fact_market_store --project-dir /usr/app",
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
            **get_postgres_env(),
        }
    )

    # 순서 보장: 연결 테스트 성공 후 dbt 실행
    check_time >> db_conn >> wait_for_ingest >> dbt_run