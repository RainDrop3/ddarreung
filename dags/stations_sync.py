"""대여소 마스터(OA-13252) 동기화 DAG.

원천은 반기에 한 번 올라오는 파일이라, 매일 실행하되 새 파일이거나 내용이 바뀐 경우에만
raw에 저장한다. (변경 감지는 ddareungi.stations.sync_stations 참고)
같은 날 다시 실행해도 결과가 같다(멱등).
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="stations_sync",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["ingest", "stations"],
)
def stations_sync():
    @task(execution_timeout=timedelta(minutes=5))
    def sync() -> dict:
        from ddareungi.stations import sync_stations

        return sync_stations()

    sync()


stations_sync()
