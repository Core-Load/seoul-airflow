from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import os
import sys

# 커스텀 플러그인 import
sys.path.insert(0, '/opt/airflow/plugins')
from seoul_api import SeoulAPI
from s3_utils import S3Manager
from db_utils import PostgreSqlManager


@task
def test():
    PGUSER = os.getenv('PGUSER')
    print("Hello, World!")
    print(SeoulAPI.get_yesterday())
    print('pguser: ' + PGUSER[:3])
    

with DAG(
    dag_id="test",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["test"]
) as dag:
    
    test()