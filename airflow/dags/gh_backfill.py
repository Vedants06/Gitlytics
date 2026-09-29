"""Manual backfill of a range of past hours (decision D4: 2024-09-28 and 2024-09-29).

Trigger from the UI with "Trigger DAG w/ config", or:
    airflow dags trigger gh_backfill -c '{"start_hour": "2024-09-28-0", "end_hour": "2024-09-29-23"}'
Each hour becomes one mapped task, 3 running at a time.
"""

from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param

from ingestion.gharchive import hour_key, hour_range, parse_hour
from ingestion.pipeline import ingest_hour


@dag(
    dag_id="gh_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    params={
        "start_hour": Param("2024-09-28-0", type="string", description="First UTC hour, e.g. 2024-09-28-0"),
        "end_hour": Param("2024-09-29-23", type="string", description="Last UTC hour, inclusive"),
    },
    default_args={"owner": "gitlytics", "retries": 3, "retry_delay": timedelta(minutes=5)},
    tags=["gitlytics", "ingest"],
)
def gh_backfill():

    @task
    def list_hours(params=None) -> list[str]:
        start, end = parse_hour(params["start_hour"]), parse_hour(params["end_hour"])
        if end < start:
            raise ValueError("end_hour is before start_hour")
        return [hour_key(h) for h in hour_range(start, end)]

    @task(max_active_tis_per_dag=3)
    def ingest(hour: str) -> dict:
        return ingest_hour(parse_hour(hour))

    ingest.expand(hour=list_hours())


gh_backfill()
