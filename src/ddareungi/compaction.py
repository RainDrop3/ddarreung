"""실시간 스냅샷 compaction: landing의 5분 JSON을 하루 하나의 Parquet로 합쳐 raw에 저장.

설계 메모
- 입력 landing/realtime/dt=D/HHMM.json.gz → 출력 raw/realtime/dt=D/snapshots.parquet
- 실행 시각이 아니라 "landing에 남아 있는, 이미 끝난 날짜 전부"를 대상으로 한다.
  PC가 꺼져 있던 날이 있어도 다음 실행이 알아서 따라잡는다.
- 멱등성: 같은 날짜는 같은 키를 덮어쓴다. 슬롯 수가 그대로면 다시 만들지 않는다.
- 덮어쓰기 방지: landing은 7일 뒤 객체가 하나씩 삭제된다. 끝난 날짜의 슬롯은 늘지 않고
  줄기만 하므로, 기존 결과보다 슬롯이 적으면 덮어쓰지 않는다.
- raw 원칙: API 필드는 이름과 값(문자열)을 그대로 둔다. 중복 제거나 형 변환은 dbt에서 한다.
  slot, fetched_at은 수집 쪽에서 만든 값이라 timestamp로 저장한다.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import re
from datetime import date, datetime, time, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from ddareungi.lake import bucket_name, get_s3_client
from ddareungi.realtime import KST

log = logging.getLogger(__name__)

LANDING_PREFIX = "landing/realtime/"
LANDING_KEY = re.compile(r"^landing/realtime/dt=(\d{4}-\d{2}-\d{2})/\d{4}\.json\.gz$")
FIELDS = (
    "stationId",
    "stationName",
    "stationLatitude",
    "stationLongitude",
    "rackTotCnt",
    "parkingBikeTotCnt",
    "shared",
)
# 23:55 슬롯은 수집 함수의 max_lag(15분)까지 재시도로 다시 써질 수 있다
SETTLE = timedelta(minutes=15)
TIMESTAMP = pa.timestamp("us", tz="Asia/Seoul")


class CompactionError(RuntimeError):
    pass


def raw_key(day: date) -> str:
    return f"raw/realtime/dt={day:%Y-%m-%d}/snapshots.parquet"


def is_closed(day: date, now: datetime) -> bool:
    return now >= datetime.combine(day + timedelta(days=1), time(), tzinfo=KST) + SETTLE


def list_landing(s3) -> dict[date, list[str]]:
    """landing에 남아 있는 날짜별 객체 키 (키 순서 = 시간 순서)."""
    days: dict[date, list[str]] = {}
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket_name(), Prefix=LANDING_PREFIX)
    for page in pages:
        for obj in page.get("Contents", []):
            m = LANDING_KEY.match(obj["Key"])
            if not m:
                log.warning("예상하지 못한 키라 건너뜀: %s", obj["Key"])
                continue
            days.setdefault(date.fromisoformat(m.group(1)), []).append(obj["Key"])
    return {d: sorted(keys) for d, keys in sorted(days.items())}


def snapshot_table(body: bytes) -> pa.Table:
    """landing 객체 하나를 대여소당 한 행인 테이블로 바꾼다."""
    snapshot = json.loads(gzip.decompress(body))
    rows = [r for page in snapshot["pages"] for r in page.get("rentBikeStatus", page).get("row", [])]
    if not rows:
        raise CompactionError(f"행이 없는 스냅샷: slot={snapshot['slot']}")

    keys = set().union(*rows)
    missing = [f for f in FIELDS if any(f not in r for r in rows)]
    if missing:
        raise CompactionError(f"응답 필드가 빠짐: {missing} (slot={snapshot['slot']})")
    # API에 필드가 새로 생기면 버리지 않고 컬럼으로 남긴다
    extra = sorted(keys - set(FIELDS))
    if extra:
        log.warning("처음 보는 응답 필드: %s", extra)

    n = len(rows)
    columns = {
        "slot": pa.array([datetime.fromisoformat(snapshot["slot"])] * n, type=TIMESTAMP),
        "fetched_at": pa.array([datetime.fromisoformat(snapshot["fetched_at"])] * n, type=TIMESTAMP),
    }
    for name in (*FIELDS, *extra):
        columns[name] = pa.array([r.get(name) for r in rows], type=pa.string())
    return pa.table(columns)


def _existing_slot_count(s3, key: str) -> int:
    try:
        head = s3.head_object(Bucket=bucket_name(), Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return 0
        raise
    return int((head.get("Metadata") or {}).get("slot-count", 0))


def compact_day(s3, day: date, keys: list[str]) -> dict:
    key = raw_key(day)
    existing = _existing_slot_count(s3, key)
    summary = {"dt": day.isoformat(), "key": key, "slots": len(keys)}
    if existing == len(keys):
        return {**summary, "status": "unchanged"}
    if existing > len(keys):
        log.info("%s: landing %s개 < 기존 결과 %s개, 덮어쓰지 않음", day, len(keys), existing)
        return {**summary, "status": "kept", "slots": existing}

    tables = [
        snapshot_table(s3.get_object(Bucket=bucket_name(), Key=k)["Body"].read()) for k in keys
    ]
    table = pa.concat_tables(tables, promote_options="default")
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    s3.put_object(
        Bucket=bucket_name(),
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/vnd.apache.parquet",
        Metadata={"slot-count": str(len(keys))},
    )
    status = "created" if existing == 0 else "updated"
    log.info(
        "%s: s3://%s/%s (슬롯 %s개, %s행, %s bytes)",
        status, bucket_name(), key, len(keys), table.num_rows, buf.tell(),
    )
    return {**summary, "status": status, "rows": table.num_rows, "bytes": buf.tell()}


def compact_realtime(s3=None, now: datetime | None = None) -> list[dict]:
    """끝난 날짜를 모두 확인해 필요한 날짜만 합친다.

    한 날짜가 실패해도 나머지는 처리하고(landing은 곧 삭제된다), 끝에서 실패를 알린다.
    """
    s3 = s3 or get_s3_client()
    now = now or datetime.now(KST)
    results, failed = [], []
    for day, keys in list_landing(s3).items():
        if not is_closed(day, now):
            continue
        try:
            results.append(compact_day(s3, day, keys))
        except Exception:
            log.exception("%s compaction 실패", day)
            failed.append(day.isoformat())
    if failed:
        raise CompactionError(f"compaction 실패 날짜: {', '.join(failed)}")
    return results
