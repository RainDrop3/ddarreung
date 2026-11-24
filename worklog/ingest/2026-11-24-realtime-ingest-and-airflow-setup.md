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
