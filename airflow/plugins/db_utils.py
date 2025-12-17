import logging
from typing import List, Optional, Dict, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)


class PostgreSqlManager:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=conn_id)
    
    def get_postgres_connection_env(self) -> Dict[str, str]:
        conn = self.hook.get_connection(self.conn_id)
        
        # database name 결정 (schema 필드 우선, 없을 시 extra에서 추출)
        db_name = conn.schema or conn.extra_dejson.get("database")

        return {
            "PGHOST": conn.host,
            "PGPORT": str(conn.port or 5432),
            "PGUSER": conn.login,
            "PGPASSWORD": conn.password,
            "PGDATABASE": db_name,
        }
    
    def test_connection(self) -> bool:
        try:
            # Hook의 get_conn()을 사용하여 연결 가능 여부 확인
            with self.hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
            
            logger.info("PostgreSQL 연결 테스트 성공: %s", self.conn_id)
            return result is not None and result[0] == 1
        except Exception as e:
            logger.error("PostgreSQL 연결 테스트 실패: %s", self.conn_id, exc_info=e)
            return False
    
    def execute_query(
        self,
        query: str,
        parameters: Optional[tuple] = None,
        autocommit: bool = True
    ) -> None:
        try:
            self.hook.run(
                sql=query,
                parameters=parameters,
                autocommit=autocommit
            )
            logger.info("쿼리 실행 성공")
        except Exception as e:
            logger.error("쿼리 실행 실패", exc_info=e)
            raise
    
    def create_schema_if_not_exists(self, schema_name: str) -> None:
        try:
            create_query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
            self.execute_query(create_query)
            logger.info("스키마 생성 또는 확인 완료: %s", schema_name)
        except Exception as e:
            logger.error("스키마 생성 실패: %s", schema_name, exc_info=e)
            raise
    
    def create_table(self, create_query: str) -> None:
        try:
            self.execute_query(create_query)
            logger.info("테이블 생성 성공")
        except Exception as e:
            logger.error("테이블 생성 실패", exc_info=e)
            raise
    
    def insert_rows(
        self,
        table_name: str,
        columns: List[str],
        values: List[tuple],
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None
    ) -> int:
        # 데이터 INSERT (UPSERT 지원)
        try:
            if not values:
                logger.warning("INSERT할 데이터가 없습니다")
                return 0
            
            placeholders = ", ".join(["%s"] * len(columns))
            columns_str = ", ".join(columns)
            
            # 기본 INSERT 쿼리
            insert_query = f"""
                INSERT INTO {table_name} ({columns_str})
                VALUES ({placeholders})
            """
            
            # UPSERT 처리
            if conflict_columns:
                conflict_str = ", ".join(conflict_columns)
                insert_query += f"\nON CONFLICT ({conflict_str})"
                
                if update_columns:
                    # 지정된 컬럼만 업데이트
                    update_set = ", ".join([
                        f"{col} = EXCLUDED.{col}" 
                        for col in update_columns
                    ])
                    insert_query += f"\nDO UPDATE SET {update_set}"
                else:
                    # 모든 컬럼 업데이트 (conflict_columns 제외)
                    update_cols = [col for col in columns if col not in conflict_columns]
                    update_set = ", ".join([
                        f"{col} = EXCLUDED.{col}" 
                        for col in update_cols
                    ])
                    insert_query += f"\nDO UPDATE SET {update_set}"
            
            # 배치 INSERT 실행
            conn = self.hook.get_conn()
            cursor = conn.cursor()
            
            try:
                for value in values:
                    cursor.execute(insert_query, value)
                conn.commit()
                
                logger.info(
                    "데이터 INSERT 성공: table=%s, rows=%d",
                    table_name,
                    len(values)
                )
                return len(values)
                
            except Exception as e:
                conn.rollback()
                logger.error(
                    "데이터 INSERT 실패: table=%s",
                    table_name,
                    exc_info=e
                )
                raise
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            logger.error("INSERT 작업 실패", exc_info=e)
            raise
    
    def insert_from_json(
        self,
        table_name: str,
        json_data: Dict[str, Any],
        json_path: Optional[List[str]] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        filter_func: Optional[callable] = None,
        conflict_columns: Optional[List[str]] = None,
        update_columns: Optional[List[str]] = None
    ) -> int:
        try:
            # JSON 경로를 따라 데이터 추출
            data = json_data
            if json_path:
                for key in json_path:
                    data = data.get(key, {})
            
            # 리스트가 아니면 리스트로 변환
            if not isinstance(data, list):
                data = [data]
            
            if not data:
                logger.warning("INSERT할 데이터가 없습니다")
                return 0
            
            # 필터링 적용
            if filter_func:
                data = [row for row in data if filter_func(row)]
            
            if not data:
                logger.warning("필터링 후 데이터가 없습니다")
                return 0
            
            # 첫 번째 row에서 컬럼 목록 추출
            first_row = data[0]
            json_columns = list(first_row.keys())
            
            # 컬럼 매핑 적용
            if column_mapping:
                db_columns = [
                    column_mapping.get(col, col) 
                    for col in json_columns
                ]
            else:
                db_columns = json_columns
            
            # 데이터 변환
            values = []
            for row in data:
                value_tuple = tuple(row.get(col) for col in json_columns)
                values.append(value_tuple)
            
            # INSERT 실행
            return self.insert_rows(
                table_name=table_name,
                columns=db_columns,
                values=values,
                conflict_columns=conflict_columns,
                update_columns=update_columns
            )
            
        except Exception as e:
            logger.error("JSON 데이터 INSERT 실패", exc_info=e)
            raise
    
    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        try:
            query = """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s
                )
            """
            result = self.hook.get_first(
                sql=query,
                parameters=(schema, table_name)
            )
            return result[0] if result else False
            
        except Exception as e:
            logger.error(
                "테이블 존재 확인 실패: %s.%s",
                schema,
                table_name,
                exc_info=e
            )
            raise
    
    def truncate_table(self, table_name: str) -> None:
        try:
            truncate_query = f"TRUNCATE TABLE {table_name}"
            self.execute_query(truncate_query)
            logger.info("테이블 TRUNCATE 완료: %s", table_name)
        except Exception as e:
            logger.error("테이블 TRUNCATE 실패: %s", table_name, exc_info=e)
            raise