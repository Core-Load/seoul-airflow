from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def test_postgres_conn_safe():
    print("🚀 PostgreSQL 연결 테스트 시작")

    hook = PostgresHook(postgres_conn_id="conn_postgres")

    # 1️⃣ 커넥션 생성
    conn = hook.get_conn()
    print("✅ DB 커넥션 생성 성공")

    cur = conn.cursor()

    # 2️⃣ 최소 쿼리 (시스템 정보만)
    cur.execute("SELECT 1;")
    print("✅ SELECT 1 실행 성공")

    # 3️⃣ 현재 DB / 유저만 확인 (데이터 영향 없음)
    cur.execute("SELECT current_database(), current_user;")
    db, user = cur.fetchone()
    print(f"📌 connected_db={db}, connected_user={user}")

    cur.close()
    conn.close()

    print("🎉 PostgreSQL 연결 테스트 완료")

with DAG(
    dag_id="test_postgres_connection_safe",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test", "postgres", "safe"],
) as dag:

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn_safe,
    )
