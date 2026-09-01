"""This module contains a Google Sheets API hook"""

import logging
from typing import Any

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from googleapiclient.discovery import build


class GSheetsHook(GCSHook):
    """
    Interact with Google Sheets via Google Cloud connection
    Reading and writing cells in Google Sheet:
    https://developers.google.com/sheets/api/guides/values

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param api_version: API Version
    :type api_version: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(
        self,
        gcp_conn_id: str,
        api_version: str = "v4",
        delegate_to: str | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
        )
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self._conn = None

    def get_conn(self) -> Any:
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("sheets", self.api_version, http=http_authorized, cache_discovery=False)

        return self._conn

    def get_values(
        self,
        spreadsheet_id: str,
        range_: str,
        major_dimension: str = "DIMENSION_UNSPECIFIED",
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> list:
        service = self.get_conn()
        logging.info(
            "Getting values with the following params:"
            f"majorDimension={major_dimension};"
            f"value_render_option={value_render_option};"
            f"date_time_render_option={date_time_render_option};"
        )
        try:
            response = (
                service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=spreadsheet_id,
                    range=range_,
                    majorDimension=major_dimension,
                    valueRenderOption=value_render_option,
                    dateTimeRenderOption=date_time_render_option,
                )
                .execute(num_retries=self.num_retries)
            )

            # there is no the `values` field in response for empty list
            return response.get("values", [])
        finally:
            service.close()

    def get_spreadsheet(self, spreadsheet_id: str):
        return self.get_conn().spreadsheets().get(spreadsheetId=spreadsheet_id).execute(num_retries=self.num_retries)


def sheet_to_odl(
    gcp_conn_id: str,
    odl_table: str,
    dwh_conn_id: str,
    spreadsheet_id: str,
    sheet_name: str,
    columns: list[str],
    skip_leading_rows: int = 1,
    columns_limit: int | None = None,
    **kwargs,
):
    import logging
    from itertools import zip_longest

    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    sheet_hook = GSheetsHook(gcp_conn_id=gcp_conn_id)
    dwh_hook = PostgresHook(dwh_conn_id)

    logging.info(f"Getting data from the given spreadsheet ({spreadsheet_id})...")
    src_values = sheet_hook.get_values(
        spreadsheet_id=spreadsheet_id,
        range_=f"{sheet_name}!$A:$XFD",
        value_render_option="FORMATTED_VALUE",
    )
    logging.info(f"There are {len(src_values)} rows in the given sheet ({sheet_name}).")

    dst_values = [
        dict(
            zip_longest(
                columns,
                row[:columns_limit],
                fillvalue=None,
            )
        )
        for row in src_values[skip_leading_rows:]
    ]
    logging.info(f"{dst_values=}")
    df = pd.DataFrame(dst_values, columns=columns, dtype=str)
    df["_start_date"] = kwargs["ti"].start_date
    df["_logical_date"] = kwargs["logical_date"].strftime("%Y-%m-%d")

    logging.info(df)
    df.to_sql(
        name=odl_table,
        schema="odl",
        con=dwh_hook.get_sqlalchemy_engine(),
        index=False,
        if_exists="replace",
    )
