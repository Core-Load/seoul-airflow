import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

class S3Manager:
    @staticmethod
    def test_connection(conn_id: str, bucket_name: str = None) -> bool:
        try:
            hook = S3Hook(aws_conn_id=conn_id)
            
            hook.get_conn()
            logger.info(f"AWS Connection '{conn_id}' 인증 성공")

            if bucket_name:
                exists = hook.check_for_bucket(bucket_name)
                if exists:
                    logger.info(f"S3 버킷 '{bucket_name}' 접근 가능")
                    return True
                else:
                    logger.error(f"S3 버킷 '{bucket_name}'을 찾을 수 없습니다.")
                    return False
            
            return True
        except Exception as e:
            logger.error(f"S3 연결 테스트 실패: {e}")
            return False

    @staticmethod
    def get_list_s3_objects(conn_id: str, bucket_name: str, prefix: str = "") -> list:
        hook = S3Hook(aws_conn_id=conn_id)
        return hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    