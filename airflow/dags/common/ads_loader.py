import json
import sys
from pathlib import Path

PROJECT_ROOT = Path("/opt/airflow/project")
if PROJECT_ROOT.exists() and str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from services.adapters.ads.google_ads_adapter import GoogleAdsAdapter
from services.adapters.ads.meta_ads_adapter import MetaAdsAdapter

from common.clickhouse_client import run_clickhouse_query


def save_raw_payload(source: str, endpoint: str, records: list[dict]) -> None:
    payload = json.dumps(records, ensure_ascii=False)

    row = json.dumps(
        {
            "source": source,
            "endpoint": endpoint,
            "payload": payload,
        },
        ensure_ascii=False,
    )

    query = """
    INSERT INTO travel.raw_ads_api_payloads
    (
        source,
        endpoint,
        payload
    )
    FORMAT JSONEachRow
    """

    run_clickhouse_query(query, data=row.encode("utf-8"))


def load_platform_ads(platform: str, table_name: str, records: list[dict]) -> None:
    if not records:
        print(f"No records received for {platform}")
        return

    save_raw_payload(
        source=platform,
        endpoint=platform.replace("_", "-"),
        records=records,
    )

    rows = "\n".join(
        json.dumps(record, ensure_ascii=False)
        for record in records
    )

    insert_query = f"""
    INSERT INTO travel.{table_name}
    (
        campaign_id,
        campaign_name,
        clicks,
        impressions,
        spend,
        platform
    )
    FORMAT JSONEachRow
    """

    run_clickhouse_query(insert_query, data=rows.encode("utf-8"))
    print(f"Loaded {len(records)} records into {table_name}")


def load_google_ads() -> None:
    records = GoogleAdsAdapter().fetch_campaign_performance()
    load_platform_ads("google_ads", "raw_google_ads", records)


def load_meta_ads() -> None:
    records = MetaAdsAdapter().fetch_campaign_performance()
    load_platform_ads("meta_ads", "raw_meta_ads", records)
