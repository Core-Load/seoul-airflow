from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from s3_utils import S3Manager
from seoul_utils import SeoulAPI
from plugins import slack

AWS_CONN_ID = "conn_aws"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")


def list_s3_objects():
    # S3 버킷의 오늘 날짜 폴더 내 파일 목록 조회 (KST 기준)
    s3 = S3Manager(
        conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )
    
    # 오늘 날짜 폴더 내의 파일들 조회 (KST 기준)
    today = SeoulAPI.get_kst_now().strftime('%Y-%m-%d')
    prefix = f"{today}/"
    
    files = s3.get_list_s3_objects(prefix=prefix)
    
    if files:
        print(f"{S3_BUCKET_AME}/{today} 폴더 내 조회된 파일 수: {len(files)}")
        for f in files[:10]: # 최대 10개 출력
            print(f"  - {f}")
    else:
        print(f"{S3_BUCKET_AME}/{today} 폴더가 비어있거나 접근 가능한 파일이 없습니다.")

default_args= {
    'on_failure_callback': slack.on_failure_callback
}

with DAG(
    dag_id="test_s3",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["s3", "test"]
) as dag:

    # 파일 목록 조회 (연결은 자동으로 처리됨)
    list_files = PythonOperator(
        task_id="list_s3_objects",
        python_callable=list_s3_objects
    )

    list_files