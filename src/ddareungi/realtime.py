"""실시간 대여정보(bikeList) 스냅샷 수집 → 레이크 landing 저장.

API 명세와 관측은 docs/sources/realtime-bikelist.md 참고.

설계 메모
- 저장 위치: landing/realtime/dt=YYYY-MM-DD/HHMM.json.gz (KST, 5분 슬롯)
- 멱등성: 키를 "실제 호출 시각"이 아니라 "슬롯"에서 만든다. 같은 슬롯을 재시도하면
  같은 키를 덮어쓰고, gzip mtime을 0으로 고정해 같은 내용이면 바이트도 같다.
- 실시간 API는 과거 데이터를 주지 않는다. 오래된 슬롯을 지금 호출해 저장하면 현재 데이터에
  과거 이름표를 붙이게 되므로, 슬롯이 현재와 너무 어긋나면(max_lag) 거부한다.
  백필·catchup으로 이 함수를 돌리면 안 된다.
- raw 원칙: API 응답은 한 글자도 바꾸지 않고 pages에 그대로 넣는다. 응답에 없는
  수집 시각만 fetched_at으로 덧붙인다.
"""
from __future__ import annotations

import gzip
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from ddareungi.lake import bucket_name, get_s3_client

log = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))  # 한국은 서머타임이 없어 고정 오프셋으로 충분하다
BASE_URL = "http://openapi.seoul.go.kr:8088"
SERVICE = "bikeList"
PAGE_SIZE = 1000  # API 상한(ERROR-336)
MAX_PAGES = 10  # 무한 루프 방지. 현재 대여소는 약 2,700개(3페이지)
SLOT_MINUTES = 5
CODE_OK = "INFO-000"
CODE_NO_DATA = "INFO-200"


class ApiError(RuntimeError):
    pass


def _mask(text: str, api_key: str) -> str:
    return text.replace(api_key, "***KEY***")


def _fetch_page(session, api_key: str, start: int, end: int, retries: int) -> dict:
    # 인증키가 URL 경로에 들어간다. requests 예외 메시지에 URL이 섞이므로
    # 마스킹한 새 예외로 바꿔 던지고, 원본 예외는 from None으로 숨긴다.
    url = f"{BASE_URL}/{api_key}/json/{SERVICE}/{start}/{end}/"
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            if attempt == retries:
                raise ApiError(
                    f"{start}~{end} 호출 실패: {type(e).__name__}: {_mask(str(e), api_key)}"
                ) from None
            log.warning("%s~%s 호출 실패(%s/%s), 재시도", start, end, attempt, retries)
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def _body(payload: dict) -> dict:
    # 정상 응답은 {"rentBikeStatus": {...}}, 오류 응답은 최상위에 RESULT만 올 수 있다(미검증)
    return payload.get("rentBikeStatus", payload)


def _rows(payload: dict) -> list:
    return _body(payload).get("row", [])


def fetch_snapshot(api_key: str, session=None, retries: int = 3) -> list[dict]:
    """전 대여소를 페이지 단위로 호출해 응답 원본 리스트를 돌려준다.

    list_total_count는 전체 건수가 아니라 해당 호출의 행 수라서(관측), 반환 행이
    PAGE_SIZE 미만이면 마지막 페이지로 본다. 전체가 정확히 1000의 배수면 다음 호출이
    INFO-200(데이터 없음)으로 오므로 그것도 종료로 처리한다.
    """
    session = session or requests.Session()
    pages: list[dict] = []
    for i in range(MAX_PAGES):
        start, end = i * PAGE_SIZE + 1, (i + 1) * PAGE_SIZE
        payload = _fetch_page(session, api_key, start, end, retries)
        result = _body(payload).get("RESULT") or {}
        code = result.get("CODE")
        if code == CODE_NO_DATA and pages:
            break
        if code != CODE_OK:
            raise ApiError(f"{start}~{end} 응답 오류: {code} {result.get('MESSAGE')}")
        pages.append(payload)
        if len(_rows(payload)) < PAGE_SIZE:
            break
    else:
        raise ApiError(f"{MAX_PAGES}페이지를 넘어도 끝나지 않음")
    return pages


def floor_slot(dt: datetime) -> datetime:
    dt = dt.astimezone(KST)
    return dt.replace(minute=dt.minute - dt.minute % SLOT_MINUTES, second=0, microsecond=0)


def landing_key(slot: datetime) -> str:
    return f"landing/realtime/dt={slot:%Y-%m-%d}/{slot:%H%M}.json.gz"


def encode_snapshot(slot: datetime, fetched_at: datetime, pages: list[dict]) -> bytes:
    payload = {
        "slot": slot.isoformat(),
        "fetched_at": fetched_at.isoformat(),
        "pages": pages,
    }
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw, mtime=0)


def collect_realtime(
    api_key: str | None = None,
    slot: datetime | None = None,
    s3=None,
    session=None,
    max_lag: timedelta = timedelta(minutes=15),
) -> dict:
    """한 슬롯의 스냅샷을 수집해 landing에 저장하고 요약을 돌려준다."""
    now = datetime.now(KST)
    slot = floor_slot(slot or now)
    if abs(now - slot) > max_lag:
        raise ValueError(
            f"슬롯 {slot.isoformat()}이 현재와 {max_lag} 넘게 어긋남. "
            "실시간 API는 과거를 제공하지 않아 백필할 수 없다."
        )
    api_key = api_key or os.environ["SEOUL_OPEN_API_KEY"]

    fetched_at = now
    pages = fetch_snapshot(api_key, session=session)
    body = encode_snapshot(slot, fetched_at, pages)

    key = landing_key(slot)
    (s3 or get_s3_client()).put_object(
        Bucket=bucket_name(), Key=key, Body=body, ContentType="application/json"
    )
    stations = sum(len(_rows(p)) for p in pages)
    log.info("저장 s3://%s/%s (%s개 대여소, %s bytes)", bucket_name(), key, stations, len(body))
    return {"key": key, "stations": stations, "pages": len(pages), "bytes": len(body)}
