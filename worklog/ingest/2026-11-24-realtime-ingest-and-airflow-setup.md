# 실시간 대여정보 수집 파이프라인과 Airflow 로컬 환경 구축

- 브랜치: main
- 커밋: 01da47a..7e33db3 (13개)
- 주요파일: `src/ddareungi/realtime.py`, `dags/realtime_collect.py`, `docker-compose.yml`, `docs/sources/stations.md`

## 한 일

- 레포 초기 설정: `.gitignore`(PROJECT.md, `.env`, `data/` 제외), `.env.example`, `pyproject.toml`, 디렉터리 뼈대. 레포가 공개 포트폴리오라서 키와 비공개 문서는 추적에서 뺐다.
- 스토리지 접근: `lake.py`가 endpoint와 키를 환경변수로 받아 boto3 클라이언트를 만든다. `scripts/check_r2.py`로 업로드, 목록, 읽기, 삭제 연결 확인.
- 실시간 대여정보(bikeList) 수집: `realtime.py`가 1000건 단위로 페이지를 순회해 응답 원본을 `landing/realtime/dt=YYYY-MM-DD/HHMM.json.gz`에 저장한다. 수집 시각 `fetched_at`만 덧붙이고 응답은 그대로 둔다.
- Airflow 3.3.2와 Postgres 2개(메타DB, 웨어하우스)를 Compose로 구성하고, `realtime_collect` DAG로 5분마다 수집하도록 연결.
- 단위 테스트(`tests/test_realtime.py`): 페이지네이션 종료, 슬롯 키, gzip 결정성, 인증키 마스킹, 과거 슬롯 거부.
- 소스 명세 문서 2건: `docs/sources/realtime-bikelist.md`, `docs/sources/stations.md`.
- 커밋 컨벤션 문서와 worklog 워크플로우(pre-push 훅) 도입.

## 왜 이렇게 했나

- **저장 키를 호출 시각이 아니라 5분 슬롯에서 만든다.** 같은 슬롯을 재시도하면 같은 키를 덮어써서 멱등이 되고, gzip mtime을 고정해 같은 내용이면 바이트도 같다.
- **현재와 15분 넘게 어긋난 슬롯은 거부한다.** 실시간 API는 과거를 제공하지 않아서 오래된 슬롯을 지금 호출하면 현재 데이터에 과거 이름표를 붙이게 된다. 그래서 DAG도 `catchup=False`다.
- **인증키를 예외 메시지에서 마스킹한다.** 키가 URL 경로에 들어가서 `requests` 예외에 그대로 찍힌다.
- **마지막 페이지는 반환 행이 1000개 미만인지로 판단한다.** `list_total_count`가 문서 설명("총 데이터 건수")과 달리 그 호출의 행 수와 같아서 전체 건수로 쓸 수 없었다.
- **Airflow는 LocalExecutor로 단순화했다.** 공식 Compose의 CeleryExecutor(Redis, worker, flower)는 여러 머신에 worker를 나눌 때 필요하고, 단일 머신에서는 컨테이너만 늘어난다.
- **수집 코드(`realtime.py`, `lake.py`)는 Airflow를 import하지 않는다.** 실행 방식을 바꿔도(예: k8s CronJob) 수집 로직은 그대로 쓰고 Airflow 없이 단위 테스트할 수 있다.
- **대여소 정보(OA-13252)는 명세 문서만 먼저 남겼다.** 직접 확인해 보니 Open API가 아니라 반기에 한 번 올라오는 xlsx 파일이었고(페이지 표기 "실시간"은 실제와 다름), 일 단위 수집 계획이 소스와 맞지 않았다. 실시간 API의 `stationId`는 마스터에 없고 `stationName`의 접두 번호가 마스터의 대여소 번호와 대응한다.

## 남은 것 · 아는 문제

- 대여소 마스터 수집 구현은 이번 범위에 없다. `docs/sources/stations.md`의 미검증 항목(최신 파일 `seq` 자동 탐색, 이름 불일치 118건 원인, 마스터에만 있는 61개와 실시간에만 있는 16개의 성격)이 남아 있다.
- 실시간 API 미검증 항목: `stationId` 단일 조회, 범위를 벗어난 `START_INDEX`의 응답, 호출 빈도 제한, 오류 시 HTTP 상태 코드.
- 대여이력(OA-15182) 적재, 일 단위 compaction, 웨어하우스 적재와 dbt는 아직 시작하지 않았다.
- 로컬 PC에서 Docker Desktop이 꺼지면 그 시간의 수집은 비게 된다. 공백 감지 기능은 아직 없다.
- 원격 레포가 없어서 push한 적이 없다. 첫 push에는 지금까지의 모든 커밋이 한 번에 범위가 된다.

## 10:16 대여소 마스터 동기화 구현 (e2e284d..67101a5, 3개 커밋)

위 "남은 것"에 있던 대여소 마스터 수집 구현을 마쳤다. 주요 파일은 `src/ddareungi/stations.py`, `dags/stations_sync.py`, `tests/test_stations.py`.

- `sync_stations()`: 목록 페이지에서 파일명의 기준월이 가장 최신인 xlsx를 고르고, 내려받아 Parquet로 `raw/stations/snapshot_month=YYYY-MM/stations.parquet`에 저장한다.
- DAG `stations_sync`: 매일 07:00(Asia/Seoul)에 실행, `catchup=False`, 재시도 2회, 5분 타임아웃.
- 의존성에 `pyarrow` 추가. 단위 테스트 13개 추가(목록 파싱, 오류 HTML 거부, 헤더 변경 감지, 생성 → 건너뜀 → 갱신 흐름 등).

왜 이렇게 했나

- **파일이 바뀔 때만 저장한다.** 소스가 반기에 한 번 올라오는 파일이라, 계획했던 일 단위 스냅샷이면 같은 파일의 복사본만 쌓인다. 파티션도 수집일이 아니라 파일의 기준월(`snapshot_month`)로 잡았다.
- **변경 감지는 원본 파일의 sha256을 객체 메타데이터에 남겨 비교한다.** 같으면 건너뛰어 같은 날 재실행해도 결과가 같고(멱등), 같은 기준월에 내용이 달라진 파일이 오면 같은 파티션을 덮어쓴다(운영자가 오류를 고쳐 재게시하는 경우). 파일이 0.3MB라 매일 내려받아도 비용은 무시할 수 있다.
- **서버가 잘못된 요청에도 HTTP 200으로 오류 HTML을 준다.** 상태 코드가 아니라 zip 시그니처(`PK`)로 다운로드를 검증한다.
- **raw는 값을 바꾸지 않는다.** 병합된 여러 행 헤더를 열 위치(A~J) → 영문 컬럼명으로 평탄화하는 것만 하고, 앞 공백, 엑셀 날짜 일련번호, 빈 칸은 원본 그대로 둔다. 출처 추적용으로 `_source_file`, `_source_seq` 컬럼만 덧붙인다.
- **구조가 달라지면 조용히 넘기지 않고 실패한다.** 헤더가 기대와 다르거나, 데이터 사이에 번호가 숫자가 아닌 내용 행이 있거나, 데이터 행이 1,000개 미만이면 `SourceError`를 내고 R2에는 쓰지 않는다. 틀린 컬럼으로 적재되는 것이 더 나쁘다고 봤다.

남은 것 · 아는 문제

- 최신 파일이 xlsx가 아니면(예: csv) 실패한다. csv 파서는 만들지 않았다. 과거 csv 파일(22.06월, 21.01.31 기준)은 적재 대상이 아니다.
- 다운로드는 페이지의 JS 폼 POST를 따라 한 비공식 경로라서 페이지 구조가 바뀌면 깨질 수 있다. 목록 파싱 실패와 다운로드 형식 오류는 각각 명확한 예외로 드러난다.
- 실시간 `stationId`(`ST-xx`)와 마스터 대여소 번호의 조인은 아직 없다. 이름 접두 번호를 파싱해 조인해야 하고, 대여소 집합 불일치(실시간에만 16개, 마스터에만 61개)는 dbt 단계에서 다뤄야 한다.
