from datetime import timedelta

from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from google_sheets.config import dwh_conn_id, entities, gcp_conn_id, source
from google_sheets.tasks.export import sheet_to_odl

default_args = {
    "owner": "admin",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
    "priority_weight": 10,
}


for entity_config in entities:
    entity = entity_config["name"]
    start_date = entity_config["start_date"]
    schedule = entity_config["schedule"]
    spreadsheet_id = entity_config["spreadsheet_id"]
    sheet_name = entity_config["sheet_name"]
    skip_leading_rows = entity_config["skip_leading_rows"]
    columns = [item["name"] for item in entity_config["schema"] if item["name"] not in ["_start_date", "_logical_date"]]

    with DAG(
        dag_id=f"{source}.{entity}",
        description=f"Load {entity} ({entity_config['sheet_name']}) from Google Sheets",
        start_date=start_date,
        schedule=schedule,
        tags=["GoogleSheets", "Source"],
        max_active_runs=1,
        catchup=False,
        default_args=default_args,
    ) as dag:
        latest_only__task = LatestOnlyOperator(task_id="latest_only")
        sheet_to_odl__task = PythonOperator(
            task_id="sheet_to_odl",
            python_callable=sheet_to_odl,
            op_kwargs={
                "gcp_conn_id": gcp_conn_id,
                "dwh_conn_id": dwh_conn_id,
                "odl_table": f"t_{source}_{entity}",
                "spreadsheet_id": spreadsheet_id,
                "sheet_name": sheet_name,
                "skip_leading_rows": skip_leading_rows,
                "columns_limit": len(columns),
                "columns": columns,
            },
        )

        latest_only__task >> sheet_to_odl__task

        globals()[dag.dag_id] = dag
