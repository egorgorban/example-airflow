from datetime import datetime
from importlib import import_module

source = "gsheets"
gcp_conn_id = "gcp-example"
dwh_conn_id = "dwh-postgres"

__default_params = {
    "start_date": datetime(2025, 3, 2),
    "schedule": "@daily",
    "skip_leading_rows": 1,
}

entities = [
    {
        **__default_params,
        "name": "leads_ml_contest_2025",
        "spreadsheet_id": "1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "sheet_name": "Лист1",
    },
    {
        **__default_params,
        "name": "leads_ml_contest_2024",
        "spreadsheet_id": "1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "sheet_name": "2024 год",
    },
    {
        **__default_params,
        "name": "leads_ml_contest_phones",
        "spreadsheet_id": "1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
        "sheet_name": "Лист3",
        "skip_leading_rows": 0,
    },
]

for entity in entities:
    entity["schema"] = import_module(f"{__package__}.schemas.{entity['name']}").schema
