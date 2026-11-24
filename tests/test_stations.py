import hashlib
import io
import zipfile
from xml.sax.saxutils import escape

import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from ddareungi import stations
from ddareungi.stations import SourceError, StationFile

LISTING = """
<tbody>
<tr id="fileTr_1"><td>1</td><td>데이터</td><td><span title="공공자전거 대여소 정보(26.6월 기준).xlsx" onclick="javascript:downloadFile('24');">x</span></td></tr>
<tr id="fileTr_2"><td>2</td><td>데이터</td><td><span title="공공자전거 대여소 정보(25.12월 기준).xlsx" onclick="javascript:downloadFile('23');">x</span></td></tr>
<tr id="fileTr_9"><td>9</td><td>데이터</td><td><span title="3. 공공자전거 대여소 정보(22.06월 기준).csv" onclick="javascript:downloadFile('16');">x</span></td></tr>
<tr id="fileTr_12"><td>12</td><td>데이터</td><td><span title="공공자전거 대여소 정보(21.01.31 기준).csv" onclick="javascript:downloadFile('11');">x</span></td></tr>
</tbody>
"""

HEADER_ROWS = [
    ["대여소\n번호", "보관소(대여소)명", "소재지(위치)", None, None, None, "설치\n시기", "설치형태", None, "운영\n방식"],
    [None, None, None, None, None, None, None, "LCD", "QR", None],
    [None, None, "자치구", "상세주소", "위도", "경도", None, None, None, None],
    [None, None, None, None, None, None, None, "거치\n대수", "거치\n대수", None],
    [None] * 10,
]


def make_xlsx(rows) -> bytes:
    """rows: 열 A~J 값 리스트의 리스트. 문자열은 공유 문자열, 숫자처럼 보이는 셀 중 일부는 숫자 셀로 넣는다."""
    shared: list[str] = []

    def sid(s):
        if s not in shared:
            shared.append(s)
        return shared.index(s)

    xml_rows = []
    for i, row in enumerate(rows, start=1):
        cells = []
        for col, val in zip("ABCDEFGHIJ", row):
            if val is None:
                continue
            ref = f"{col}{i}"
            if isinstance(val, (int, float)):
                cells.append(f'<c r="{ref}"><v>{val}</v></c>')
            else:
                cells.append(f'<c r="{ref}" t="s"><v>{sid(val)}</v></c>')
        xml_rows.append(f'<row r="{i}">{"".join(cells)}</row>')
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    sheet = f'<worksheet xmlns="{ns}"><sheetData>{"".join(xml_rows)}</sheetData></worksheet>'
    sst = f'<sst xmlns="{ns}">' + "".join(f"<si><t>{escape(s)}</t></si>" for s in shared) + "</sst>"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("xl/worksheets/sheet1.xml", sheet)
        z.writestr("xl/sharedStrings.xml", sst)
    return buf.getvalue()


def data_rows(n=1200):
    return [[str(100 + i), f" 대여소 {i}", "마포구", "서울특별시 마포구", "37.5", "126.9", 42253.98, None, "15", "QR"] for i in range(n)]


def good_xlsx(n=1200):
    return make_xlsx(HEADER_ROWS + data_rows(n))


# ---------- 목록 파싱 ----------
def test_parse_listing_and_asof():
    files = stations.parse_listing(LISTING)
    assert [(f.seq, f.asof) for f in files] == [("24", "2026-06"), ("23", "2025-12"), ("16", "2022-06"), ("11", "2021-01")]


def test_parse_listing_without_rows_fails():
    with pytest.raises(SourceError, match="페이지 구조"):
        stations.parse_listing("<html></html>")


def test_pick_latest_by_asof_not_by_list_order():
    files = list(reversed(stations.parse_listing(LISTING)))
    assert stations.pick_latest(files).seq == "24"


def test_pick_latest_rejects_non_xlsx():
    with pytest.raises(SourceError, match="xlsx가 아님"):
        stations.pick_latest([StationFile("30", "공공자전거 대여소 정보(27.6월 기준).csv", "2027-06")])


# ---------- 다운로드 ----------
class Resp:
    def __init__(self, content=b"", status=200):
        self.content, self.status = content, status

    def raise_for_status(self):
        assert self.status == 200


def test_download_rejects_html_error_page_served_as_200():
    class S:
        def post(self, url, data, timeout):
            return Resp(b"<html><script>alert('\xec\x9e\x98\xeb\xaa\xbb')</script></html>")

    with pytest.raises(SourceError, match="xlsx가 아님"):
        stations.download(S(), StationFile("24", "f.xlsx", "2026-06"))


def test_download_sends_expected_form():
    sent = {}

    class S:
        def post(self, url, data, timeout):
            sent.update(data)
            return Resp(b"PK\x03\x04...")

    assert stations.download(S(), StationFile("24", "f.xlsx", "2026-06")).startswith(b"PK")
    assert sent == {"infId": "OA-13252", "seqNo": "24", "seq": "24", "infSeq": "2"}


# ---------- xlsx 파싱 ----------
def test_parse_xlsx_keeps_values_raw():
    rows = stations.parse_xlsx(good_xlsx(1200))
    assert len(rows) == 1200
    first = rows[0]
    assert first["station_no"] == "100"
    assert first["station_name"] == " 대여소 0"  # 앞 공백 그대로
    assert first["installed_at_serial"] == "42253.98"  # 날짜로 바꾸지 않음
    assert first["lcd_rack_cnt"] is None  # 빈 칸은 None
    assert first["qr_rack_cnt"] == "15"


def test_parse_xlsx_fails_when_header_changes():
    bad = make_xlsx([["번호", "이름"] + [None] * 8] + data_rows(1200))
    with pytest.raises(SourceError, match="헤더"):
        stations.parse_xlsx(bad)


def test_parse_xlsx_fails_on_non_numeric_row_inside_data():
    rows = HEADER_ROWS + data_rows(1200) + [["합계", None, None, None, None, None, None, None, "9", None]]
    with pytest.raises(SourceError, match="숫자가 아닌 행"):
        stations.parse_xlsx(make_xlsx(rows))


def test_parse_xlsx_fails_when_too_few_rows():
    with pytest.raises(SourceError, match="최소"):
        stations.parse_xlsx(good_xlsx(10))


# ---------- Parquet ----------
def test_to_parquet_roundtrip_all_strings():
    f = StationFile("24", "공공자전거 대여소 정보(26.6월 기준).xlsx", "2026-06")
    body = stations.to_parquet(stations.parse_xlsx(good_xlsx(1200)), f)
    t = pq.read_table(io.BytesIO(body))
    assert t.num_rows == 1200
    assert set(str(x) for x in t.schema.types) == {"string"}
    assert t.column("_source_seq").to_pylist()[0] == "24"
    assert t.column("station_name").to_pylist()[0] == " 대여소 0"


# ---------- 동기화 (변경 감지 / 멱등) ----------
class FakeS3:
    def __init__(self):
        self.objs = {}
        self.puts = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objs:
            raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
        return {"Metadata": self.objs[Key]["Metadata"]}

    def put_object(self, Bucket, Key, Body, ContentType, Metadata):
        self.objs[Key] = {"Body": Body, "Metadata": Metadata}
        self.puts += 1


class FakeSession:
    def __init__(self, xlsx):
        self.xlsx = xlsx

    def get(self, url, timeout):
        return Resp(LISTING.encode("utf-8"))

    def post(self, url, data, timeout):
        return Resp(self.xlsx)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "test-bucket")


def test_sync_creates_then_skips_when_unchanged_then_updates_on_change():
    s3 = FakeS3()
    xlsx = good_xlsx(1200)

    first = stations.sync_stations(s3=s3, session=FakeSession(xlsx))
    assert first["status"] == "created" and first["rows"] == 1200
    assert first["key"] == "raw/stations/snapshot_month=2026-06/stations.parquet"
    assert s3.objs[first["key"]]["Metadata"]["source-sha256"] == hashlib.sha256(xlsx).hexdigest()

    again = stations.sync_stations(s3=s3, session=FakeSession(xlsx))  # 같은 날 재실행
    assert again["status"] == "unchanged" and s3.puts == 1

    fixed = good_xlsx(1201)  # 운영자가 같은 기준월 파일을 고쳐 재게시
    updated = stations.sync_stations(s3=s3, session=FakeSession(fixed))
    assert updated["status"] == "updated" and updated["rows"] == 1201 and s3.puts == 2


def test_sync_does_not_write_when_source_is_broken():
    s3 = FakeS3()
    with pytest.raises(SourceError):
        stations.sync_stations(s3=s3, session=FakeSession(b"<html>error</html>"))
    assert s3.puts == 0
