"""실시간 대여정보를 5분마다 수집해 R2 landing에 저장하는 DAG.

- catchup=False: 실시간 API는 과거를 제공하지 않아 놓친 슬롯을 나중에 채울 수 없다.
  (collect_realtime도 현재와 15분 넘게 어긋난 슬롯은 거부한다.)
- max_active_runs=1: 한 회차가 밀려도 호출이 겹쳐 쌓이지 않게 한다.
- 키(S3_*, SEOUL_OPEN_API_KEY)는 컨테이너 환경변수로 받는다.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="realtime_collect",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
    tags=["ingest", "realtime"],
)
def realtime_collect():
    @task(execution_timeout=timedelta(minutes=4))
    def collect() -> dict:
        # 파싱 시점에 import하지 않도록 task 안에서 가져온다
        from ddareungi.realtime import collect_realtime

        return collect_realtime()

    collect()


realtime_collect()
