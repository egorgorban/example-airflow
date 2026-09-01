from io import BytesIO

from pandas import DataFrame


def _create_log_request(counter_id: int, entity, date1, date2, api_fields, url, headers) -> int:
    import logging
    import time

    import httpx

    response = httpx.post(
        url=f"{url}/{counter_id}/logrequests",
        headers=headers,
        params={
            "fields": ",".join(api_fields),
            "source": entity,
            "date1": date1,
            "date2": date2,
            "attribution": "LASTSIGN",
        },
    )
    if response.status_code != 200:
        raise ValueError(f"Ошибка при создании запроса: {response.status_code}, {response.text}")

    request_json = response.json()["log_request"]
    request_id, status = request_json["request_id"], request_json["status"]
    logging.info(f"Got request_id: `{request_id}`, {status=}")

    while status != "processed":
        time.sleep(20)
        check_status_response = httpx.get(f"{url}/{counter_id}/logrequest/{request_id}", headers=headers)
        if check_status_response.status_code != 200:
            raise ValueError(f"Ошибка при проверке статуса: {response.status_code}, {response.text}")
        status = check_status_response.json()["log_request"]["status"]
        logging.info(f"Статус запроса: {status}")

    return request_id


def _download_data(counter_id, request_id, url, headers) -> DataFrame:
    import logging

    import httpx
    import pandas as pd

    def _load_part(counter_id, request_id, url, part_number, headers) -> DataFrame:
        url = f"{url}/{counter_id}/logrequest/{request_id}/part/{part_number}/download"
        schema_overrides = {
            "ym:pv:watchID": str,
            "ym:pv:clientID": str,
            "ym:pv:counterUserIDHash": str,
        }
        response = httpx.get(url, headers=headers)
        if response.status_code != 200:
            raise ValueError(f"Ошибка при скачивании данных: {response.status_code}, {response.text}")
        logging.info(f"Часть {part_number} скачана")
        return pd.read_csv(
            BytesIO(response.content),
            sep="\t",
            dtype=schema_overrides,
        )

    part_number = 0
    df = _load_part(counter_id, request_id, url, part_number, headers)
    while True:
        try:
            part_number += 1
            df = pd.concat([df, _load_part(counter_id, request_id, url, part_number, headers)], ignore_index=True)
        except ValueError:
            logging.warning(f"Ошибка при скачивании части {part_number}")
            break
    return df


def _clean_log_request(url, counter_id, request_id, headers) -> None:
    import logging

    import httpx

    resp = httpx.post(
        f"{url}/{counter_id}/logrequest/{request_id}/clean",
        headers=headers,
    )
    if resp.status_code != 200:
        logging.warning(f"Cannot clean logs for {counter_id=}, {request_id=}")


def _transform_df(df: DataFrame, field_mapping: dict) -> DataFrame:
    df = df.rename(columns=field_mapping)
    df["device_category"] = df["device_category"].replace(
        {0: "unknown", 1: "desktop", 2: "mobile", 3: "tablet", 4: "tv"}
    )
    return df


def _get_logs_dataframe(
    counter_id: int, *, entity, date1: str, date2: str, fields: list[dict[str, str]], url: str, token: str
) -> DataFrame:
    headers = {"Authorization": f"OAuth {token}", "Content-Type": "application/json"}
    request_id = _create_log_request(counter_id, entity, date1, date2, [f["yname"] for f in fields], url, headers)
    raw_df = _download_data(counter_id, request_id, url, headers)
    _clean_log_request(url, counter_id, request_id, headers)
    return _transform_df(raw_df, field_mapping={f["yname"]: f["name"] for f in fields})


def load_metrika_data(
    counter_id,
    entity,
    dwh_conn_id,
    yandex_conn_id,
    fields,
    buffer_table_pref,
    **kwargs,
) -> None:
    import logging

    from airflow.providers.http.hooks.http import HttpHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # params
    date_from = kwargs["data_interval_start"].strftime("%Y-%m-%d")
    date_to = kwargs["data_interval_end"].strftime("%Y-%m-%d")
    logging.info(f"Running from {date_from} to {date_to}")
    #  hooks
    yandex_conn = HttpHook.get_connection(conn_id=yandex_conn_id)
    dwh_hook = PostgresHook(dwh_conn_id)
    # ------

    df = _get_logs_dataframe(
        counter_id,
        entity=entity,
        date1=date_from,
        date2=date_to,
        fields=fields,
        url=yandex_conn.host,  # type: ignore
        token=yandex_conn.password,
    )
    df.to_sql(
        name=f"{buffer_table_pref}_{kwargs['ts_nodash']}",
        schema="stg",
        con=dwh_hook.get_sqlalchemy_engine(),
        index=False,
        if_exists="replace",
    )
