import pendulum
from pathlib import Path

def skip_at_kst_21(**context) -> bool:
    now = pendulum.now("Asia/Seoul")
    return now.hour != 21

def load_sql(filename: str, dag_file: str, **params) -> str:
    dag_dir = Path(dag_file).parent          # /airflow/dags
    sql_dir = dag_dir / "sql"                # /airflow/dags/sql
    sql_path = sql_dir / filename

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL 파일을 찾을 수 없습니다: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    try:
        return sql.format(**params)
    except KeyError as e:
        raise KeyError(f"SQL 파라미터 누락: {e}")