import psycopg2
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class PostgreSqlManager:
    @staticmethod
    def get_postgres_connection_env(conn_id: str) -> dict:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)

        return {
            "PGHOST": conn.host,
            "PGPORT": str(conn.port or 5432),
            "PGUSER": conn.login,
            "PGPASSWORD": conn.password,
            "PGDATABASE": conn.schema or conn.extra_dict.get("database"),
        }
    
    @staticmethod
    def test_connection(env_vars: dict) -> bool:
        # get_postgres_connection_env에서 리턴된 딕셔너리를 인자로 받아 연결이 가능한지 테스트
        try:
            conn = psycopg2.connect(
                host=env_vars['PGHOST'],
                port=env_vars['PGPORT'],
                user=env_vars['PGUSER'],
                password=env_vars['PGPASSWORD'],
                dbname=env_vars['PGDATABASE'],
                connect_timeout=5
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            result = cur.fetchone()
            cur.close()
            conn.close()
            return result[0] == 1
        except Exception as e:
            logger.error(f"연결 테스트 실패: {e}")
            return False