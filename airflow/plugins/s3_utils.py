import json
import logging
from typing import List
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)


class S3Manager:
    def __init__(self, conn_id: str, bucket_name: str):
        self.conn_id = conn_id
        self.bucket_name = bucket_name
        self.hook = S3Hook(aws_conn_id=conn_id)

    def upload_json(
        self,
        key: str,
        data: dict,
        replace: bool = True
    ) -> None:
        try:
            json_str = json.dumps(
                data,
                ensure_ascii=False,
                indent=2
            )

            self.hook.load_string(
                string_data=json_str,
                key=key,
                bucket_name=self.bucket_name,
                replace=replace
            )

            logger.info(
                "S3 업로드 성공: s3://%s/%s",
                self.bucket_name,
                key
            )

        except Exception as e:
            logger.error(
                "S3 JSON 업로드 실패: s3://%s/%s",
                self.bucket_name,
                key,
                exc_info=e
            )
            raise

    def get_list_s3_objects(self, prefix: str = "") -> List[str]:
        try:
            keys = self.hook.list_keys(
                bucket_name=self.bucket_name,
                prefix=prefix
            )
            return keys or []
        except Exception as e:
            logger.error(
                "S3 객체 목록 조회 실패: bucket=%s, prefix=%s",
                self.bucket_name,
                prefix,
                exc_info=e
            )
            raise
