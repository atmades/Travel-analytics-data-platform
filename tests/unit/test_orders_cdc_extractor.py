import sys
from pathlib import Path
sys.modules.pop("domain", None)

SERVICE_PATH = Path(__file__).resolve().parents[2] / "services" / "orders_cdc_consumer"
sys.path.insert(0, str(SERVICE_PATH))

from domain.extractor import build_raw_cdc_row, extract_order_id  # noqa: E402


def test_extract_order_id_from_after_record():
    event = {
        "after": {"order_id": 123},
        "before": None,
    }

    assert extract_order_id(event) == 123


def test_extract_order_id_from_before_record_when_after_is_missing():
    event = {
        "after": None,
        "before": {"order_id": 456},
    }

    assert extract_order_id(event) == 456


def test_build_raw_cdc_row_preserves_before_and_after_records_as_json():
    event = {
        "op": "u",
        "before": {"order_id": 1, "status": "paid"},
        "after": {"order_id": 1, "status": "completed"},
        "ts_ms": 123456789,
    }

    row = build_raw_cdc_row(event)

    assert row["order_id"] == 1
    assert row["op"] == "u"
    assert row["source_ts_ms"] == 123456789
    assert '"status": "paid"' in row["before_record"]
    assert '"status": "completed"' in row["after_record"]