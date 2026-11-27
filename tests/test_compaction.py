import io
from datetime import date, datetime, timedelta

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from ddareungi import compaction
from ddareungi.compaction import CompactionError, compact_realtime
from ddareungi.realtime import KST, encode_snapshot, landing_key

DAY = date(2026, 11, 24)
NOW = datetime(2026, 11, 25, 9, 0, tzinfo=KST)


def station(i, **extra):
    row = {f: f"{f}-{i}" for f in compaction.FIELDS}
    row["shared"] = "193"
    return {**row, **extra}


def snapshot(slot, rows):
    return encode_snapshot(slot, slot + timedelta(seconds=7), [{"rentBikeStatus": {"row": rows}}])


class FakeS3:
    def __init__(self):
        self.objs = {}
        self.puts = 0
        self.gets = 0

    def add(self, hour, minute, rows=None, day=DAY):
        slot = datetime(day.year, day.month, day.day, hour, minute, tzinfo=KST)
        self.objs[landing_key(slot)] = {"Body": snapshot(slot, rows or [station(1), station(2)])}

    def get_paginator(self, name):
        objs = self.objs

        class Paginator:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [{"Key": k} for k in sorted(objs) if k.startswith(Prefix)]}]

        return Paginator()

    def get_object(self, Bucket, Key):
        self.gets += 1
        return {"Body": io.BytesIO(self.objs[Key]["Body"])}

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"Metadata": self.objs[Key]["Metadata"]}

    def put_object(self, Bucket, Key, Body, ContentType, Metadata):
        self.objs[Key] = {"Body": Body, "Metadata": Metadata}
        self.puts += 1

    def table(self, day=DAY):
        return pq.read_table(io.BytesIO(self.objs[compaction.raw_key(day)]["Body"]))


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")


def test_combines_a_day_in_time_order_keeping_values_raw():
    s3 = FakeS3()
    s3.add(9, 35)
    s3.add(9, 30)

    (result,) = compact_realtime(s3=s3, now=NOW)

    assert result["status"] == "created" and result["slots"] == 2 and result["rows"] == 4
    t = s3.table()
    assert t.column_names == ["slot", "fetched_at", *compaction.FIELDS]
    assert [x.strftime("%H:%M") for x in t.column("slot").to_pylist()] == ["09:30", "09:30", "09:35", "09:35"]
    assert t.column("shared").to_pylist()[0] == "193"


def test_rerun_skips_without_reading_landing():
    s3 = FakeS3()
    s3.add(9, 30)
    compact_realtime(s3=s3, now=NOW)
    gets = s3.gets

    (result,) = compact_realtime(s3=s3, now=NOW)

    assert result["status"] == "unchanged"
    assert s3.puts == 1 and s3.gets == gets


def test_does_not_overwrite_after_landing_partially_expired():
    s3 = FakeS3()
    s3.add(9, 30)
    s3.add(9, 35)
    compact_realtime(s3=s3, now=NOW)
    del s3.objs[landing_key(datetime(2026, 11, 24, 9, 30, tzinfo=KST))]

    (result,) = compact_realtime(s3=s3, now=NOW)

    assert result["status"] == "kept" and result["slots"] == 2
    assert s3.table().num_rows == 4


def test_updates_when_more_slots_than_before():
    s3 = FakeS3()
    s3.add(9, 30)
    compact_realtime(s3=s3, now=NOW)
    s3.add(9, 35)

    (result,) = compact_realtime(s3=s3, now=NOW)

    assert result["status"] == "updated" and s3.table().num_rows == 4


@pytest.mark.parametrize(
    "now, expected",
    [
        (datetime(2026, 11, 25, 0, 14, tzinfo=KST), 0),  # 23:55 슬롯이 아직 다시 써질 수 있음
        (datetime(2026, 11, 25, 0, 15, tzinfo=KST), 1),
        (datetime(2026, 11, 24, 23, 0, tzinfo=KST), 0),  # 진행 중인 날짜
    ],
)
def test_only_closed_days(now, expected):
    s3 = FakeS3()
    s3.add(23, 55)
    assert len(compact_realtime(s3=s3, now=now)) == expected


def test_new_api_field_is_kept_as_column():
    s3 = FakeS3()
    s3.add(9, 30)
    s3.add(9, 35, rows=[station(1, newField="x")])

    compact_realtime(s3=s3, now=NOW)

    assert s3.table().column("newField").to_pylist() == [None, None, "x"]


def test_bad_day_does_not_block_other_days_and_still_fails():
    s3 = FakeS3()
    broken = station(1)
    del broken["parkingBikeTotCnt"]
    s3.add(9, 30, rows=[broken])
    s3.add(9, 30, day=date(2026, 11, 23))
    s3.add(9, 30, day=date(2026, 11, 22), rows=[station(1)])

    with pytest.raises(CompactionError, match="2026-11-24"):
        compact_realtime(s3=s3, now=NOW)

    assert compaction.raw_key(date(2026, 11, 23)) in s3.objs
    assert compaction.raw_key(date(2026, 11, 22)) in s3.objs
    assert compaction.raw_key(DAY) not in s3.objs
