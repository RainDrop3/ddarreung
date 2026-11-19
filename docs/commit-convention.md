# 커밋 컨벤션

[Conventional Commits](https://www.conventionalcommits.org/ko/v1.0.0/) 형식을 따르고, 설명은 한국어로 쓴다.

```
<type>(<scope>): <제목>

<본문 — 선택>
```

## type

| type | 용도 | 예 |
|---|---|---|
| `feat` | 새 기능·새 파이프라인 단계 | `feat(ingest): 실시간 대여정보 5분 스냅샷 수집` |
| `fix` | 버그 수정 | `fix(ingest): 마지막 페이지 판정을 행 수 기준으로 변경` |
| `docs` | 문서만 변경 | `docs(sources): 대여소 정보 API 명세 추가` |
| `refactor` | 동작 변화 없는 구조 개선 | `refactor(lake): S3 클라이언트 생성을 함수로 분리` |
| `test` | 테스트 추가·수정 | `test(ingest): 페이지네이션 종료 조건 테스트` |
| `chore` | 빌드·의존성·설정 등 잡무 | `chore: requests 의존성 추가` |
| `ci` | CI 설정 | `ci: ruff 린트 워크플로 추가` |
| `perf` | 성능 개선 | `perf(load): COPY로 적재 방식 변경` |

## scope

변경이 속한 영역. 한 커밋이 여러 영역에 걸치면 생략한다.

| scope | 대상 |
|---|---|
| `ingest` | API 수집 코드 (`src/ddareungi/`) |
| `lake` | R2/S3 접근, 레이크 레이아웃 |
| `dag` | Airflow DAG (`dags/`) |
| `dbt` | dbt 모델·테스트 (`dbt/`) |
| `infra` | Docker Compose, 컨테이너 설정 |
| `sources` | 원천 데이터 명세 문서 (`docs/sources/`) |

새 영역이 생기면 이 표에 추가한다.

## 작성 규칙

- **제목은 50자 안팎, 마침표 없이.** 무엇을 했는지 명사형이나 짧은 서술로 쓴다.
- **본문에는 "왜"를 쓴다.** 무엇을 바꿨는지는 diff에 있고, 이유와 대안 검토는 diff에 없다. 설계 결정이 담긴 커밋은 본문을 쓴다.
- **한 커밋에는 한 가지 논리적 변경만.** 수집 코드와 문서 정리를 섞지 않는다.
- **파괴적 변경**은 type 뒤에 `!`를 붙이고 본문에 `BREAKING CHANGE:`를 적는다. 예: raw 파티션 경로 변경, 컬럼명 변경.

## 커밋하면 안 되는 것

- `.env`, API 키, R2 토큰, 인증키가 포함된 URL이 찍힌 로그
- 대여이력 원본 zip, 대용량 데이터 (`data/`는 `.gitignore` 대상)
- `PROJECT.md` (`.gitignore` 대상)

커밋 전에 `git diff --staged`로 키 문자열이 섞이지 않았는지 확인한다. 이 레포는 공개 포트폴리오다.

## 예시

```
feat(ingest): 실시간 대여정보 페이지네이션 수집 함수 추가

응답의 list_total_count가 전체 건수가 아니라 해당 호출의 행 수와 같아서,
반환 행이 1000개 미만이면 마지막 페이지로 판단한다.
호출 시각은 응답에 없으므로 fetched_at을 직접 기록한다.
```

```
fix(lake)!: landing 경로를 dt 파티션 기준으로 변경

BREAKING CHANGE: landing/realtime/YYYY-MM-DD/ → landing/realtime/dt=YYYY-MM-DD/
기존 객체는 재업로드 없이 lifecycle로 삭제된다.
```
