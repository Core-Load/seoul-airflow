import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.models import Variable
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, date, timedelta
from s3_utils import S3Manager
from db_utils  import PostgreSqlManager
import boto3

AWS_CONN_ID = "conn_aws"
POSTGRES_CONN_ID = "conn_postgres"
S3_BUCKET_NAME = Variable.get("s3_bucket_name")
SCHEMA_NAME = "raw_data"
TABLE_NAME = "3Q_market_info"
API_KEY = Variable.get("seoul_api_key")

@task
def get_market_info_by_quarter():
    SERVICE = 'VwsmTrdarSelngQq'
    TOTAL_COUNT = 21520
    BATCH_SIZE = 1000

    all_data = []

    for start_idx in range(1, TOTAL_COUNT + 1, BATCH_SIZE):
        end_idx = min(start_idx + BATCH_SIZE - 1, TOTAL_COUNT)
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/json/{SERVICE}/{start_idx}/{end_idx}/20253"
        
        try:
            response = requests.get(url, timeout=10) 
            response.raise_for_status() 
            
            data = response.json()
            
            if 'RESULT' in data and data['RESULT']['CODE'] != 'INFO-000':
                logging.info(f"API 서버 에러 ({start_idx}~{end_idx}): {data['RESULT']['MESSAGE']} ({data['RESULT']['CODE']})")
                continue
                
            if SERVICE in data and 'row' in data[SERVICE]:
                all_data.extend(data[SERVICE]['row'])
                logging.info(f"성공: {start_idx} ~ {end_idx} 수집 중...")
            
        except Exception as e:
            logging.info(f"에러 발생: {e}")
    return all_data

@task
def save_to_s3(all_data):
    
    s3_key='one-off/market/2025_3Q_서울시_상권_추정매출.json'
    s3 = S3Manager(
        conn_id=AWS_CONN_ID,
        bucket_name=S3_BUCKET_NAME
    )
    
    s3.upload_json(key=s3_key, data=all_data)
    logging.info("s3에 저장을 완료했습니다. s3://S3_BUCKET_NAME/{s3_key}")
    return s3_key

def create_insert_in_postgres(all_data):
    db = PostgreSqlManager(conn_id=POSTGRES_CONN_ID)        # postgres 연동
    conn = db.hook.get_conn()
    curr = conn.cursor()
    
    db.create_schema_if_not_exists(SCHEMA_NAME)             # 스키마 생성

    columns = list(all_data[0].keys())
    quoted_columns = [f'"{col}"' for col in columns]
    column_names_str = ", ".join(quoted_columns)
    
    try:
        create_columns_with_types = ", ".join([f'{col} TEXT' for col in quoted_columns])
        create_query = f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{TABLE_NAME}" (
                {create_columns_with_types}
            );
        """
        curr.execute("BEGIN;")
        curr.execute(create_query) 
        logging.info(f"테이블 확인, 테이블 생성 완료")
        
        curr.execute(f'TRUNCATE TABLE "{SCHEMA_NAME}"."{TABLE_NAME}";')
        logging.info(f"테이블 내부 기존 데이터 삭제 (Truncated)")
        
        # insert
        sql_template= f"""
            INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}" ({column_names_str})
            VALUES %s
        """
        
        data_to_insert=[
            tuple(record.get(col) for col in columns)
            for record in all_data
        ]
        
        execute_values(curr, sql_template, data_to_insert)
        curr.execute("COMMIT;")
        logging.info(f"{len(all_data)}건의 데이터 저장 완료")
        
    except Exception as e:
        # curr.execute("ROLLBACK;")
        conn.rollback()
        logging.error(f"오류로 인해 롤백 수행: {e}")
        raise e
    finally:
        curr.close()
        conn.close()
@task
def s3_to_postgres(s3_key):
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
    all_data = json.loads(response['Body'].read().decode('utf-8'))

    create_insert_in_postgres(all_data)


with DAG (
    dag_id='market_analysis_by_quarter',
    start_date=datetime(2025,12,18),
    schedule_interval='@once',
    catchup=False,
    default_args={
        'retries':1,
        'retry_delay': timedelta(minutes=1)
    }
) as dag:
    market_data=get_market_info_by_quarter()
    s3key_path=save_to_s3(market_data)
    s3_to_postgres(s3key_path)