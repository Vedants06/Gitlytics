"""Hourly GH Archive ingestion (decision D5).

Each run covers one data interval [H, H+1) and fires at H+1. GH Archive publishes the
file for hour H about 5 minutes later, so the sensor waits for it before ingesting.
catchup=True: if the laptop was off, missed hours are fetched when Airflow restarts.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from ingestion.gharchive import is_published
from ingestion.pipeline import ingest_hour

GO_LIVE = pendulum.datetime(2026, 9, 30, 0, tz="UTC")


@dag(
    dag_id="gh_hourly_ingest",
    schedule="@hourly",
    start_date=GO_LIVE,
    catchup=True,
    max_active_runs=3,
    default_args={"owner": "gitlytics", "retries": 3, "retry_delay": timedelta(minutes=10)},
    tags=["gitlytics", "ingest"],
)
def gh_hourly_ingest():

    @task.sensor(task_id="wait_for_file", poke_interval=300, timeout=2 * 3600, mode="reschedule")
    def wait_for_file(data_interval_start=None) -> bool:
        return is_published(data_interval_start)

    @task(task_id="ingest")
    def ingest(data_interval_start=None) -> dict:
        return ingest_hour(data_interval_start)

    wait_for_file() >> ingest()


gh_hourly_ingest()
