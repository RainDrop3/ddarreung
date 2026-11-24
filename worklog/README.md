# worklog

작업 기록을 시간순으로 쌓는 곳. **append-only** — 이미 쓴 줄은 고치지 않고 끝에만 덧붙인다.

- 문서 언어: 한국어
- push 한 번에 기록 한 번. 같은 날 같은 도메인이면 기존 문서에 이어 쓴다.
  `.githooks/pre-push` 가 이걸 강제한다.
- 문서는 Claude Code 에서 `/worklog:write` 로 만든다.
- 경로: `worklog/<도메인>/<YYYY-MM-DD>-<슬러그>.md`

여기 문서는 "그때 이랬다" 는 기록이라 지금도 맞는지는 보장하지 않는다.
현재 사실은 코드와 스펙 문서를 본다.

## 도메인

변경 파일 경로로 도메인을 하나 고른다. 여러 도메인에 걸치면 변경 파일이 가장 많은 쪽이다.

| 변경 경로 | 도메인 |
|---|---|
| `src/`, `scripts/`, `tests/` | `ingest` |
| `dags/` | `dag` |
| `docker/`, `docker-compose.yml` | `infra` |
| `dbt/` | `dbt` |
| 그 외 (`docs/`, 설정 파일 등) | `etc` |

## 설치

worklog 플러그인을 쓰면 Claude Code 를 켤 때 SessionStart 훅이 자동으로 잡는다.
플러그인 없이 수동으로 하려면:

```
git config core.hooksPath .githooks
```
