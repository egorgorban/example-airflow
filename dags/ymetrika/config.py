from datetime import datetime

from ymetrika.schemas import hits

source = "ymetrika"
dwh_conn_id = "dwh-postgres"
yandex_conn_id = "ymetrika-logs-api"

__default_kwargs = {
    "schedule": "@daily",
    "start_date": datetime(2025, 1, 1),
}
entities = [
    {
        **__default_kwargs,
        "name": "hits",
        "fields": hits.fields,
    },
    # {
    #     **__default_kwargs,
    #     "name": "visits",
    # },
]

accounts = [
    {
        "name": "start",
        "counter_id": 12345678,
    },
    {
        "name": "ai",
        "counter_id": 87654321,
    },
]
