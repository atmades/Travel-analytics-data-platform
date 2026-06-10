import sys
from pathlib import Path

SERVICE_PATH = Path(__file__).resolve().parents[2] / "services" / "user_event_consumer"
sys.path.insert(0, str(SERVICE_PATH))

sys.modules.pop("domain", None)

from domain.normalizer import parse_event_time  # noqa: E402


def test_parse_event_time_converts_zulu_time_to_clickhouse_format():
    result = parse_event_time("2026-05-24T12:00:00.123456Z")

    assert result == "2026-05-24 12:00:00"


def test_parse_event_time_converts_timezone_offset_to_utc():
    result = parse_event_time("2026-05-24T12:00:00.123456-05:00")

    assert result == "2026-05-24 17:00:00"