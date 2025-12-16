import os
import sys
from db_utils import PostgreSqlManager

DB_HOST = ""
DB_PORT = 5432
DB_DATABASE = ""
DB_USER = ""
DB_PASSWORD = ""

import logging
logging.basicConfig(level=logging.INFO)

def main():
    print("--- PostgreSqlManager 연결 테스트 시작 ---")

    # 1. PostgreSqlManager 인스턴스 생성
    try:
        db_manager = PostgreSqlManager(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
    except Exception as e:
        print(f"PostgreSqlManager 인스턴스 생성 실패: {e}")
        return
    
    # 2. test_connection 메서드 호출
    if db_manager.test_connection():
        print("✅ DB 연결 테스트 성공!")

        # 3. execute_query 테스트 (예: 버전 조회)
        try:
            result = db_manager.execute_query("SELECT version()", fetch=True)
            print(f"✅ 쿼리 실행 테스트 성공. PostgreSQL 버전: {result[0][0]}")
        except Exception as e:
            print(f"❌ 쿼리 실행 테스트 실패: {e}")
    else:
        print("❌ DB 연결 테스트 실패!")
        print("호스트, 포트, 인증 정보를 확인하세요.")

    print("--- 테스트 종료 ---")

if __name__ == "__main__":
    main()