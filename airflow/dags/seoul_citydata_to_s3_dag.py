from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from urllib.parse import quote
import requests
import json

# =============================
# 고정 설정
# =============================
DAG_ID = "seoul_citydata_to_s3"
SCHEDULE = "*/30 * * * *"

AREAS = [
    "명동 관광특구",
    "광화문·덕수궁",
    "어린이대공원",
    "여의도",
    "성수카페거리",
    "홍대입구역(2호선)",
    "강남역",
    "DMC(디지털미디어시티)",
    "광장(전통)시장",
    "서울식물원·마곡나루역",
]

BASE_URL = "http://openapi.seoul.go.kr:8088/{api_key}/json/citydata/1/5/{area}"

# =============================
# Python Callable
# =============================
def fetch_and_upload(**context):
    # 1️⃣ Seoul Open API Key
    api_key = Variable.get("seoul_api_key", default_var=None)
    if not api_key:
        raise AirflowFailException("❌ Airflow Variable 'seoul_api_key'가 없습니다")

    bucket = Variable.get("s3_bucket_name")
    prefix = Variable.get("s3_prefix", default_var="seoul/citydata")

    # 2️⃣ AWS Connection 기반 S3 Hook
    s3_hook = S3Hook(aws_conn_id="conn_aws")

    execution_time = context["data_interval_start"]

    success = []
    failed = []

    for area in AREAS:
        encoded_area = quote(area, safe="")
        url = BASE_URL.format(api_key=api_key, area=encoded_area)

        try:
            res = requests.get(url, timeout=10)

            if res.status_code != 200:
                failed.append({"area": area, "reason": f"HTTP {res.status_code}"})
                continue

            # 🔥 JSON 파싱 방어
            try:
                data = res.json()
            except Exception:
                failed.append({
                    "area": area,
                    "reason": "JSON 파싱 실패",
                    "response_sample": res.text[:200]
                })
                continue

            if "CITYDATA" not in data or not data["CITYDATA"]:
                failed.append({
                    "area": area,
                    "reason": "CITYDATA 없음",
                    "response_keys": list(data.keys())
                })
                continue

            date_part = execution_time.strftime("%Y-%m-%d")
            time_part = execution_time.strftime("%Y%m%d%H%M")

            key = (
                f"de7-team4/{date_part}/city_data/"
                f"{time_part}-서울시_실시간_도시데이터-{area}.json"
            )

            s3_hook.load_string(
                string_data=json.dumps(data, ensure_ascii=False),
                key=key,
                bucket_name=bucket,
                replace=True,
            )

            success.append(area)

        except Exception as e:
            failed.append({"area": area, "reason": str(e)})

    print("✅ SUCCESS AREAS:", success)
    print("❌ FAILED AREAS:", failed)

    if failed:
        raise AirflowFailException(f"❌ 일부 지역 수집 실패: {failed}")

    return {"success": success, "failed": failed}


# =============================
# DAG 정의
# =============================
default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    tags=["seoul", "openapi", "s3", "citydata"],
) as dag:

    fetch_citydata_to_s3 = PythonOperator(
        task_id="fetch_citydata_to_s3",
        python_callable=fetch_and_upload,
    )

    fetch_citydata_to_s3
