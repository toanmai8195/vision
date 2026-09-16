"""DAG kiểm tra hạ tầng local: Postgres catalog đủ 4 loại dữ liệu, StarRocks sẵn sàng.

Các DAG pipeline thật (silver, gold, segment) được thêm từ P2 trở đi.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="vision_healthcheck",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vision", "p0"],
) as dag:
    catalog_has_four_data_types = BashOperator(
        task_id="catalog_has_four_data_types",
        bash_command=(
            "python -c \"import psycopg2, os;"
            "c=psycopg2.connect(os.environ['VISION_META_DSN']).cursor();"
            "c.execute('select count(distinct data_type) from meta.attribute');"
            "n=c.fetchone()[0]; print('data types:', n); assert n == 4, n\""
        ),
    )

    starrocks_ready = BashOperator(
        task_id="starrocks_ready",
        bash_command="python -c \"import socket; socket.create_connection(('starrocks', 9030), timeout=5)\"",
    )

    catalog_has_four_data_types >> starrocks_ready
