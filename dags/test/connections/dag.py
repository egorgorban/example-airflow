import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import connect as psycopg2_connect
from test.connections.config import pg_conn

default_args = {
    "owner": "admin",
    "tags": ["Test"],
    "execution_timeout": timedelta(seconds=10),
}


def check_pg_conn():
    url = PostgresHook(pg_conn).get_uri()

    with psycopg2_connect(url) as conn:
        logging.info(conn.info.dsn_parameters)


with DAG(
    dag_id="test.connections",
    schedule="@once",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
) as dag:
    check_pg_task = PythonOperator(task_id="postgres", python_callable=check_pg_conn)
