import gzip
import json
from datetime import datetime, timedelta

import pytest
import requests

from ddareungi import realtime
from ddareungi.realtime import KST, ApiError

KEY = "SECRET-KEY-123"


def ok_page(n: int) -> dict:
    return {
        "rentBikeStatus": {
            "list_total_count": n,
            "RESULT": {"CODE": "INFO-000", "MESSAGE": "정상 처리되었습니다."},
            "row": [{"stationId": f"ST-{i}"} for i in range(n)],
        }
    }


NO_DATA = {"RESULT": {"CODE": "INFO-200", "MESSAGE": "해당하는 데이터가 없습니다."}}


class FakeResp:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.urls = []

    def get(self, url, timeout):
        self.urls.append(url)
        return FakeResp(self.payloads.pop(0))


class FakeS3:
    def __init__(self):
        self.puts = []

    def put_object(self, **kw):
        self.puts.append(kw)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    monkeypatch.setattr(realtime.time, "sleep", lambda s: None)


def test_pagination_stops_at_short_page():
    s = FakeSession([ok_page(1000), ok_page(1000), ok_page(742)])
    pages = realtime.fetch_snapshot(KEY, session=s)
    assert [len(realtime._rows(p)) for p in pages] == [1000, 1000, 742]
    assert [u.split("/")[-3:-1] for u in s.urls] == [["1", "1000"], ["1001", "2000"], ["2001", "3000"]]


def test_exact_multiple_ends_on_no_data():
    s = FakeSession([ok_page(1000), NO_DATA])
    pages = realtime.fetch_snapshot(KEY, session=s)
    assert len(pages) == 1


def test_first_page_error_raises():
    s = FakeSession([NO_DATA])
    with pytest.raises(ApiError, match="INFO-200"):
        realtime.fetch_snapshot(KEY, session=s)


def test_never_ending_pages_raise():
    s = FakeSession([ok_page(1000)] * realtime.MAX_PAGES)
    with pytest.raises(ApiError, match="끝나지 않음"):
        realtime.fetch_snapshot(KEY, session=s)


def test_api_key_is_masked_in_errors():
    class Boom:
        def get(self, url, timeout):
            raise requests.ConnectionError(f"failed for {url}")

    with pytest.raises(ApiError) as e:
        realtime.fetch_snapshot(KEY, session=Boom(), retries=2)
    assert KEY not in str(e.value)
    assert "***KEY***" in str(e.value)
    assert e.value.__suppress_context__  # 원본 예외(URL 포함)가 traceback에 출력되지 않음


def test_floor_slot_and_key():
    dt = datetime(2026, 11, 19, 10, 7, 42, 123, tzinfo=KST)
    slot = realtime.floor_slot(dt)
    assert slot == datetime(2026, 11, 19, 10, 5, tzinfo=KST)
    assert realtime.landing_key(slot) == "landing/realtime/dt=2026-11-19/1005.json.gz"


def test_slot_is_converted_to_kst():
    utc = datetime(2026, 11, 18, 15, 3, tzinfo=realtime.timezone.utc)  # KST 2026-11-19 00:03
    assert realtime.landing_key(realtime.floor_slot(utc)) == "landing/realtime/dt=2026-11-19/0000.json.gz"


def test_encode_roundtrip_is_deterministic_and_keeps_raw():
    slot = datetime(2026, 11, 19, 10, 5, tzinfo=KST)
    fetched = datetime(2026, 11, 19, 10, 5, 3, tzinfo=KST)
    pages = [ok_page(2)]
    a = realtime.encode_snapshot(slot, fetched, pages)
    assert a == realtime.encode_snapshot(slot, fetched, pages)
    data = json.loads(gzip.decompress(a))
    assert data["pages"] == pages  # 응답 원본 그대로
    assert data["fetched_at"] == "2026-11-19T10:05:03+09:00"


def test_collect_uploads_to_slot_key():
    s3 = FakeS3()
    out = realtime.collect_realtime(
        api_key=KEY, s3=s3, session=FakeSession([ok_page(1000), ok_page(5)])
    )
    assert out["stations"] == 1005 and out["pages"] == 2
    (put,) = s3.puts
    assert put["Bucket"] == "test-bucket"
    assert put["Key"] == out["key"] and put["Key"].startswith("landing/realtime/dt=")
    assert json.loads(gzip.decompress(put["Body"]))["pages"][1] == ok_page(5)


def test_old_slot_is_rejected_before_any_network_call():
    s = FakeSession([])
    old = datetime.now(KST) - timedelta(hours=1)
    with pytest.raises(ValueError, match="백필"):
        realtime.collect_realtime(api_key=KEY, slot=old, s3=FakeS3(), session=s)
    assert s.urls == []
