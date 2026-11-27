"""landing의 실시간 스냅샷을 날짜별 Parquet로 합치는 DAG.

- 매시간 실행하지만 이미 합친 날짜는 건너뛴다. PC를 끄는 시간이 있어서 "자정 직후 하루 한 번"
  실행에 기대지 않는다.
- 대상 날짜는 실행 날짜가 아니라 landing에 남은 파일로 정하므로 catchup이 필요 없다.
- landing은 7일 뒤 삭제된다. 이 DAG가 7일 넘게 실패하면 그 날짜는 영구 손실이다.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="realtime_compact",
    schedule="20 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["compaction", "realtime"],
)
def realtime_compact():
    @task(execution_timeout=timedelta(minutes=30))
    def compact() -> list[dict]:
        from ddareungi.compaction import compact_realtime

        return compact_realtime()

    compact()


realtime_compact()
