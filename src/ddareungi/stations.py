"""대여소 마스터(OA-13252) 동기화: 열린데이터광장 xlsx → raw/stations Parquet.

소스 명세와 검증 내용은 docs/sources/stations.md 참고.

설계 메모
- 이 소스는 Open API가 아니라 반기(6월·12월 기준)에 한 번 올라오는 파일이다.
  그래서 "날짜별 스냅샷"이 아니라 "파일이 바뀌었을 때만 저장"한다.
- 파티션은 수집일이 아니라 파일의 기준월(snapshot_month=YYYY-MM)이다.
- 변경 감지: 원본 파일의 sha256을 Parquet 객체 메타데이터에 남기고, 다음 실행에서 같은 기준월의
  해시와 비교한다. 같으면 건너뛰고(멱등), 다르면 같은 파티션을 덮어쓴다(운영자가 오류를 고쳐
  재게시한 경우). 파일이 0.3MB라 매일 내려받아도 비용은 무시할 수 있다.
- raw 원칙: 값은 한 글자도 바꾸지 않는다. 병합된 여러 행 헤더를 열 위치(A~J) → 영문 컬럼명으로
  평탄화하는 것만 한다. (앞 공백, 엑셀 날짜 일련번호, 빈 칸 모두 원본 그대로)
- 서버가 오류도 HTTP 200으로 돌려주므로(잘못된 파라미터 → 오류 HTML) 상태 코드가 아니라
  내용(zip 시그니처, 헤더)으로 검증한다. 헤더가 달라지면 조용히 넘어가지 않고 실패한다.
"""
from __future__ import annotations

import hashlib
import io
import logging
import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError

from ddareungi.lake import bucket_name, get_s3_client

log = logging.getLogger(__name__)

LIST_URL = "https://data.seoul.go.kr/dataList/OA-13252/F/1/datasetView.do"
DOWNLOAD_URL = "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false"
INF_ID = "OA-13252"
INF_SEQ = "2"  # 틀리면 오류 HTML이 200으로 온다

# xlsx 열 위치 → raw 컬럼명 (26.6월 기준 파일 구조)
COLUMNS = [
    ("A", "station_no"),
    ("B", "station_name"),
    ("C", "district"),
    ("D", "address"),
    ("E", "latitude"),
    ("F", "longitude"),
    ("G", "installed_at_serial"),
    ("H", "lcd_rack_cnt"),
    ("I", "qr_rack_cnt"),
    ("J", "operation_type"),
]
MIN_ROWS = 1000  # 이보다 적으면 파일이 잘렸거나 구조가 바뀐 것으로 본다 (현재 약 2,800)

_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


class SourceError(RuntimeError):
    """원천 파일/페이지가 기대한 형태가 아님."""


@dataclass(frozen=True)
class StationFile:
    seq: str
    name: str
    asof: str  # 파일 기준월 YYYY-MM


def _asof_from_name(name: str) -> str | None:
    # "공공자전거 대여소 정보(26.6월 기준).xlsx", "(21.01.31 기준).csv" → 2026-06, 2021-01
    m = re.search(r"\((\d{2})\.(\d{1,2})", name)
    return f"20{m.group(1)}-{int(m.group(2)):02d}" if m else None


def parse_listing(html: str) -> list[StationFile]:
    files = []
    for m in re.finditer(r'<span title="([^"]+)" onclick="javascript:downloadFile\(\'(\d+)\'\);"', html):
        name, seq = m.group(1), m.group(2)
        asof = _asof_from_name(name)
        if asof:
            files.append(StationFile(seq=seq, name=name, asof=asof))
    if not files:
        raise SourceError("파일 목록을 찾지 못함. 페이지 구조가 바뀐 것으로 보인다")
    return files


def pick_latest(files: list[StationFile]) -> StationFile:
    latest = max(files, key=lambda f: (f.asof, int(f.seq)))
    if not latest.name.lower().endswith(".xlsx"):
        raise SourceError(f"최신 파일({latest.name})이 xlsx가 아님. 파서 확장이 필요하다")
    return latest


def download(session, f: StationFile) -> bytes:
    resp = session.post(
        DOWNLOAD_URL,
        data={"infId": INF_ID, "seqNo": f.seq, "seq": f.seq, "infSeq": INF_SEQ},
        timeout=60,
    )
    resp.raise_for_status()
    body = resp.content
    if body[:2] != b"PK":  # xlsx는 zip. 오류 HTML이 200으로 올 수 있다
        raise SourceError(f"{f.name} 다운로드가 xlsx가 아님 (앞 100바이트: {body[:100]!r})")
    return body


def _cell_text(c, shared: list[str]) -> str | None:
    t = c.get("t")
    if t == "inlineStr":
        is_ = c.find("m:is", _NS)
        return "".join(x.text or "" for x in is_.iter("{%s}t" % _NS["m"])) if is_ is not None else None
    v = c.find("m:v", _NS)
    if v is None or v.text is None:
        return None
    return shared[int(v.text)] if t == "s" else v.text


def _norm(s: str | None) -> str:
    return re.sub(r"\s+", "", s or "")


def parse_xlsx(data: bytes) -> list[dict]:
    """첫 시트를 읽어 대여소 행 리스트를 돌려준다. 값은 원본 문자열 그대로, 빈 칸은 None."""
    z = zipfile.ZipFile(io.BytesIO(data))
    shared: list[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", _NS):
            shared.append("".join(t.text or "" for t in si.iter("{%s}t" % _NS["m"])))

    rows: list[dict[str, str | None]] = []
    sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    for r in sheet.find("m:sheetData", _NS):
        row = {}
        for c in r:
            col = re.match(r"[A-Z]+", c.get("r")).group(0)
            row[col] = _cell_text(c, shared)
        rows.append(row)

    # 헤더 검증: 구조가 바뀌면 조용히 틀린 컬럼으로 적재하지 않고 실패한다
    if not rows or "대여소" not in _norm(rows[0].get("A")) or "보관소" not in _norm(rows[0].get("B")):
        raise SourceError(f"헤더가 기대와 다름: {rows[0] if rows else '빈 시트'}")

    out = []
    in_data = False
    for row in rows:
        a = (row.get("A") or "").strip()
        if re.fullmatch(r"\d+", a):
            in_data = True
            out.append({name: row.get(col) for col, name in COLUMNS})
        elif in_data and any(v not in (None, "") for v in row.values()):
            # 데이터 사이에 번호 없는 내용 행이 있으면 조용히 버리지 않는다
            raise SourceError(f"대여소 번호가 숫자가 아닌 행: {row}")
    if len(out) < MIN_ROWS:
        raise SourceError(f"데이터 행이 {len(out)}개뿐임 (최소 {MIN_ROWS} 기대)")
    return out


def to_parquet(rows: list[dict], f: StationFile) -> bytes:
    cols = [name for _, name in COLUMNS]
    table = pa.table(
        {
            **{c: pa.array([r[c] for r in rows], type=pa.string()) for c in cols},
            "_source_file": pa.array([f.name] * len(rows), type=pa.string()),
            "_source_seq": pa.array([f.seq] * len(rows), type=pa.string()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="zstd")
    return buf.getvalue()


def raw_key(asof: str) -> str:
    return f"raw/stations/snapshot_month={asof}/stations.parquet"


def _existing_hash(s3, key: str) -> str | None:
    try:
        head = s3.head_object(Bucket=bucket_name(), Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise
    return (head.get("Metadata") or {}).get("source-sha256", "")


def sync_stations(s3=None, session=None) -> dict:
    """최신 대여소 파일을 확인해 새 파일이거나 내용이 바뀐 경우에만 raw에 저장한다."""
    session = session or requests.Session()
    s3 = s3 or get_s3_client()

    page = session.get(LIST_URL, timeout=60)
    page.raise_for_status()
    latest = pick_latest(parse_listing(page.content.decode("utf-8")))

    data = download(session, latest)
    sha = hashlib.sha256(data).hexdigest()
    key = raw_key(latest.asof)

    existing = _existing_hash(s3, key)
    if existing == sha:
        log.info("변경 없음: %s (%s)", latest.name, key)
        return {"status": "unchanged", "key": key, "source": latest.name}

    rows = parse_xlsx(data)
    body = to_parquet(rows, latest)
    s3.put_object(
        Bucket=bucket_name(),
        Key=key,
        Body=body,
        ContentType="application/vnd.apache.parquet",
        Metadata={"source-sha256": sha, "source-seq": latest.seq},
    )
    status = "created" if existing is None else "updated"
    log.info("%s: s3://%s/%s (%s행, %s)", status, bucket_name(), key, len(rows), latest.name)
    return {"status": status, "key": key, "rows": len(rows), "source": latest.name}
