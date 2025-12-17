from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from datetime import datetime
import requests

def test_seoul_api_key():
    # 1️⃣ Variable 로드
    api_key = Variable.get("SEOUL_API_KEY", default_var=None)

    if not api_key:
        raise AirflowFailException("❌ FAIL: SEOUL_API_KEY Airflow Variable이 없습니다")

    print("✅ SEOUL_API_KEY Variable 로드 성공")

    # 2️⃣ API 호출
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/citydata/1/5/광화문·덕수궁"
    response = requests.get(url, timeout=10)

    print(f"HTTP STATUS: {response.status_code}")

    if response.status_code != 200:
        raise AirflowFailException(f"❌ FAIL: HTTP {response.status_code}")

    # 3️⃣ 응답 내용 검사 (서울시 API 성공 코드)
    if "INFO-000" not in response.text:
        raise AirflowFailException("❌ FAIL: API 응답에 INFO-000이 없습니다")

    print("🎉 SUCCESS: 서울시 OpenAPI 인증 및 호출 성공")

with DAG(
    dag_id="test_api_key_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "api", "seoul"],
) as dag:

    test_task = PythonOperator(
        task_id="test_seoul_api_key",
        python_callable=test_seoul_api_key,
    )
