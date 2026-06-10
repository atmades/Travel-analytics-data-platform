import os

import requests


CLICKHOUSE_URL = os.getenv(
    "CLICKHOUSE_URL",
    "http://clickhouse:8123",
)

CLICKHOUSE_USER = os.getenv(
    "CLICKHOUSE_USER",
    "travel_user",
)

CLICKHOUSE_PASSWORD = os.getenv(
    "CLICKHOUSE_PASSWORD",
    "travel_password",
)


class ClickHouseQueryError(Exception):
    """Raised when a ClickHouse query fails."""


def run_clickhouse_query(
    query: str,
    data: bytes | None = None,
    timeout: int = 20,
) -> str:
    response = requests.post(
        CLICKHOUSE_URL,
        params={"query": query},
        data=data,
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        timeout=timeout,
    )

    if not response.ok:
        raise ClickHouseQueryError(response.text)

    return response.text.strip()