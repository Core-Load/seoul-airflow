import json
import requests
import logging
from datetime import datetime, timedelta
from typing import Dict, Union
from airflow.models import Variable
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

class SeoulAPI:
    BASE_URL = "http://openapi.seoul.go.kr:8088"
    KST = ZoneInfo("Asia/Seoul")
    
    def __init__(self):
        try:
            self.api_key = Variable.get("seoul_api_key")
        except KeyError as e:
            logger.error("Airflow Variable 'seoul_api_key' not found", exc_info=e)
            raise
            
        self.session = requests.Session()
    
    @staticmethod
    def get_kst_now() -> datetime:
        # 현재 KST 시간을 반환
        return datetime.now(SeoulAPI.KST)
    
    @staticmethod
    def format_date(date_obj: datetime) -> str:
        # datetime 객체를 YYYYMMDDHHmm 형식으로 변환
        return date_obj.strftime('%Y%m%d%H%M')
    

    @staticmethod
    def generate_s3_key(
        api_name: str
        , folder_name: str
        , target_date: Union[datetime, str, None] = None
    ) -> str:
        # 결과 예시: "2025-12-17/weather/202512171130-202512171130-서울시_권역별_실시간_대기환경_현황.json"

        current_time = SeoulAPI.get_kst_now()
        folder_date = current_time.strftime('%Y-%m-%d')
        load_date = current_time.strftime('%Y%m%d%H%M')
        
        # target_date 처리
        if target_date is None:
            # 실시간 데이터: 현재 시간 사용 (KST)
            target_date_str = load_date
        elif isinstance(target_date, datetime):
            # datetime 객체를 문자열로 변환
            target_date_str = SeoulAPI.format_date(target_date)
        else:
            # 이미 문자열인 경우 그대로 사용
            target_date_str = target_date
        
        # S3 키 생성: 날짜폴더/적재날짜-대상날짜-API명.json
        s3_key = (
            f"{folder_date}/{folder_name}/"
            f"{load_date}-{target_date_str}-{api_name}.json"
        )
        
        return s3_key
    

    def api_request(self, request_url: str) -> Dict:
        url = f"{self.BASE_URL}/{self.api_key}/{request_url}"

        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("Seoul API 요청 실패", exc_info=e)
            raise

        try:
            data = response.json()
        except ValueError as e:
            logger.error("Seoul API 응답 JSON 파싱 실패", exc_info=e)
            raise

        # 서울시 OpenAPI 표준 에러 포맷 처리
        result = data.get("RESULT")
        if result and result.get("CODE") != "INFO-000":
            logger.error(
                "Seoul API 오류 - code=%s, message=%s",
                result.get("CODE"),
                result.get("MESSAGE")
            )
            raise RuntimeError(result.get("MESSAGE"))

        return data