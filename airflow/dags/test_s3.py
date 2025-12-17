from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from s3_utils import S3Manager

AWS_CONN_ID = "conn_aws"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")

def check_s3_connection():
    is_connected = S3Manager.test_connection(
        conn_id=AWS_CONN_ID, 
        bucket_name=S3_BUCKET_NAME
    )
    
    if not is_connected:
        raise Exception(f"S3 연결 실패! Connection ID: {AWS_CONN_ID}, Bucket: {S3_BUCKET_NAME}")
    print(f"S3 연결 및 버킷({S3_BUCKET_NAME}) 접근 성공!")

def list_s3_objects():
    files = S3Manager.get_list_s3_objects(AWS_CONN_ID, S3_BUCKET_NAME)
    
    if files:
        print(f"{S3_BUCKET_NAME} 버킷 내 조회된 파일 수: {len(files)}")
        for f in files[:10]: # 최대 10개 출력
            print(f"- {f}")
    else:
        print(f"{S3_BUCKET_NAME} 버킷이 비어있거나 접근 가능한 파일이 없습니다.")

with DAG(
    dag_id="test_s3",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["s3", "test", "variable"]
) as dag:

    # 연결 테스트 태스크
    test_conn = PythonOperator(
        task_id="test_connection",
        python_callable=check_s3_connection
    )

    # 파일 목록 조회 태스크
    list_files = PythonOperator(
        task_id="get_list_s3_objects",
        python_callable=list_s3_objects
    )

    # 순서 보장: 연결 테스트 성공 후 파일 목록 조회
    test_conn >> list_files