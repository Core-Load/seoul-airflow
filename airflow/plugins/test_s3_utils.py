import os
import logging
from s3_utils import S3Manager
import boto3
from botocore.exceptions import ClientError as BotocoreClientError

s3_BUCKET_NAME=''

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_s3_operation():
    logger.info("S3 연결 시작")
    
    try:
        s3_manager = S3Manager(
            bucket_name=s3_BUCKET_NAME
        )
        logger.info("✅ S3 인스턴스 생성 성공!")
    except Exception as e:
        logger.error(f"S3Manager 인스턴스 생성 실패 : {e}")
        return
    
    try:
        logger.info(f"s3 버킷 {s3_BUCKET_NAME}에 대한 접근 시도")
        s3_manager.s3_client.head_bucket(Bucket=s3_BUCKET_NAME)
        
        logger.info(f"✅ S3 연결 및 버킷 '{s3_BUCKET_NAME}' 접근 권한 확인 성공!")
    
    except BotocoreClientError as e:
        error_code = e.response.get('Error',{}).get('Code')
        if error_code == '403':
            logger.error(f"❌ 권한 오류 (403 Forbidden): IAM Role에 버킷 접근 권한이 없습니다.")
        elif error_code == '404':
            logger.error(f"❌ 버킷 오류 (404 Not Found): 버킷 '{s3_BUCKET_NAME}'이 존재하지 않습니다.")
        else:
            logger.error(f"❌ S3 API 호출 중 알 수 없는 오류 발생: {e}")
        return
    except Exception as e:
        logger.error(f"S3Manager 인스턴스 생성 또는 기타 오류 발생: {e}")
        return
        
    
    
if __name__ == "__main__":
    test_s3_operation()