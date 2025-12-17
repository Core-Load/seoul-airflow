from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import os
import requests


def test_seoul_api_key():
    api_key = os.getenv("SEOUL_KEY")

    # 1️⃣ ENV 체크
    if not api_key:
        raise AirflowFailException("❌ FAIL: SEOUL_KEY 환경변수가 없습니다")

    print("✅ SUCCESS: SEOUL_KEY 환경변수 로드 성공")

    # 2️⃣ 실제 API 호출
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/citydata/1/5/광화문·덕수궁"
    response = requests.get(url, timeout=10)

    # 3️⃣ HTTP 상태 코드 체크
    if response.status_code != 200:
        raise AirflowFailException(
            f"❌ FAIL: HTTP {response.status_code}"
        )

    # 4️⃣ 응답 내용 체크 (서울 API는 RESULT 태그로 판단)
    if "INFO-000" in response.text:
        print("🎉 SUCCESS: 서울시 OpenAPI 인증 성공 (INFO-000)")
    else:
        raise AirflowFailException(
            "❌ FAIL: API 응답은 받았으나 인증 실패"
        )


with DAG(
    dag_id="test_api_key_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "api", "seoul"],
) as dag:

    test_api = PythonOperator(
        task_id="test_seoul_api_key",
        python_callable=test_seoul_api_key,
    )
