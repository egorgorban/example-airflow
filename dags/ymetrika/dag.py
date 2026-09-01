from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from ymetrika.config import accounts, dwh_conn_id, entities, source, yandex_conn_id
from ymetrika.tasks.load_api import load_metrika_data

default_args = {
    "owner": "admin",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "tags": ["Metrika", "Source", "API"],
}


for entity_config in entities:
    entity = entity_config["name"]
    dag_id = f"{source}.{entity}"
    description = f"Load {entity} from logs-api (yandex-metrika)"
    schedule = entity_config["schedule"]
    start_date = entity_config["start_date"]
    fields = entity_config["fields"]

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        default_args=default_args,
        start_date=start_date,
        end_date=datetime(2025, 1, 2),
        max_active_runs=3,
        catchup=True,
    ) as dag:
        # Delay 5 hours

        wait_for_delay__task = TimeDeltaSensor(
            task_id="wait_for_delay",
            delta=timedelta(hours=5),
            poke_interval=900,
            mode="reschedule",
        )

        # Create DDL

        create_odl_table__task = SQLExecuteQueryOperator(
            task_id="create_odl_table",
            sql=f"/sql/ddl/odl.{entity}.sql",
            autocommit=True,
            conn_id=dwh_conn_id,
        )
        wait_for_delay__task >> create_odl_table__task  # type: ignore

        # Load Data

        for account_config in accounts:
            account = account_config["name"]
            counter_id = account_config["counter_id"]
            buffer_table_pref = f"buffer_{source}_{entity}_{account}"

            logs_to_buffer__task = PythonOperator(
                task_id=f"{account}__logs_api_to_buffer",
                python_callable=load_metrika_data,
                op_kwargs={
                    "counter_id": counter_id,
                    "entity": entity,
                    "dwh_conn_id": dwh_conn_id,
                    "yandex_conn_id": yandex_conn_id,
                    "fields": fields,
                    "buffer_table_pref": buffer_table_pref,
                },
            )

            buffer_to_odl__task = SQLExecuteQueryOperator(
                task_id=f"{account}__buffer_to_odl",
                sql=f"/sql/odl.{entity}.sql",
                conn_id=dwh_conn_id,
                params={
                    "source": source,
                    "entity": entity,
                    "account_slug": account,
                    "buffer_table_pref": buffer_table_pref,
                },
            )

            drop_buffer__task = SQLExecuteQueryOperator(
                task_id=f"{account}__drop_buffer",
                conn_id=dwh_conn_id,
                sql=f'DROP TABLE stg."{buffer_table_pref}_{{{{ts_nodash}}}}";',
            )

            create_odl_table__task >> logs_to_buffer__task >> buffer_to_odl__task >> drop_buffer__task

        globals()[dag.dag_id] = dag
