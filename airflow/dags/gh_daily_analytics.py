"""Daily Gitlytics Analytics Orchestration (Decision D5).

Fires daily at 01:00 UTC to execute the compute-heavy, end-to-end analytical pipelines:
1. Runs Pig Bot Scoring to identify newly flagged accounts.
2. Performs a full silver rebuild to cleanse history of newly flagged bots.
3. Rebuilds Hive Gold tables from the updated silver tables.
4. Executes Stream, Recommender, and Graph Community analytics.
5. Invokes R scripts to refresh publication-ready plots.
"""

from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

from ingestion.config import REPO_ROOT

GO_LIVE = pendulum.datetime(2026, 9, 30, tz="UTC")


@dag(
    dag_id="gh_daily_analytics",
    schedule="0 1 * * *",  # Runs daily at 01:00 UTC
    start_date=GO_LIVE,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "gitlytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    tags=["gitlytics", "analytics", "daily"],
)
def gh_daily_analytics():

    run_bot_scoring = BashOperator(
        task_id="run_bot_scoring",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && bash {REPO_ROOT}/scripts/run_bot_scoring.sh ",
        execution_timeout=timedelta(minutes=60),
    )

    rebuild_silver = BashOperator(
        task_id="rebuild_silver",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && bash {REPO_ROOT}/scripts/build_silver.sh ",
        execution_timeout=timedelta(minutes=60),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && bash {REPO_ROOT}/scripts/build_gold.sh ",
        execution_timeout=timedelta(minutes=30),
    )

    run_stream_algorithms = BashOperator(
        task_id="run_stream_algorithms",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && python {REPO_ROOT}/streaming/run_streaming.py ",
        execution_timeout=timedelta(minutes=15),
    )

    run_recommender = BashOperator(
        task_id="run_recommender",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && python {REPO_ROOT}/recommender/item_cf.py ",
        execution_timeout=timedelta(minutes=20),
    )

    run_graph_analytics = BashOperator(
        task_id="run_graph_analytics",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && python {REPO_ROOT}/graph/communities.py ",
        execution_timeout=timedelta(minutes=20),
    )

    generate_r_plots = BashOperator(
        task_id="generate_r_plots",
        bash_command=f"source {REPO_ROOT}/scripts/env.sh && cd {REPO_ROOT} && for script in {REPO_ROOT}/r/*.R; do Rscript \"$script\"; done ",
        execution_timeout=timedelta(minutes=20),
    )

    # Topology
    run_bot_scoring >> rebuild_silver >> build_gold

    build_gold >> run_stream_algorithms >> generate_r_plots
    build_gold >> run_recommender >> generate_r_plots
    build_gold >> run_graph_analytics >> generate_r_plots


gh_daily_analytics()