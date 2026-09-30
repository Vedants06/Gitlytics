"""Hourly GH Archive ingestion (decision D5) and silver build.

Each run covers one data interval [H, H+1) and fires at H+1. GH Archive publishes the
file for hour H about 5 minutes later, so the sensor waits for it before ingesting.
catchup=True: if the laptop was off, missed hours are fetched when Airflow restarts.
After ingestion, Hive flattens that hour into the silver tables.
"""

import logging
from datetime import timedelta

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from ingestion.config import REPO_ROOT
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
        # A network or SSL error (common for a minute after WSL resumes from sleep) means
        # "try again at the next poke", not "fail the run".
        try:
            return is_published(data_interval_start)
        except requests.RequestException as error:
            logging.warning("Could not check GH Archive yet: %s", error)
            return False

    @task(task_id="ingest")
    def ingest(data_interval_start=None) -> dict:
        return ingest_hour(data_interval_start)

    # One Hive build at a time: catch-up after a long sleep queues hours instead of
    # starting several Hive sessions at once on a 10 GB WSL.
    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"bash {REPO_ROOT}/scripts/build_silver.sh "
            "{{ data_interval_start.strftime('%Y-%m-%d') }} {{ data_interval_start.hour }}"
        ),
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(minutes=30),
    )

    wait_for_file() >> ingest() >> build_silver


gh_hourly_ingest()
