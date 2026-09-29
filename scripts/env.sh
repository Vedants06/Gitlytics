# Gitlytics shell environment. Load it in every terminal you use for the project:
#     source ~/gitlytics/scripts/env.sh

GITLYTICS_HOME="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GITLYTICS_HOME

# Secrets and ports from .env
set -a
. "$GITLYTICS_HOME/.env"
set +a

# Java + Hadoop
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="$HOME/bigdata/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"

# Python venv first, then Hadoop tools; repo root importable (ingestion package)
export PATH="$GITLYTICS_HOME/.venv/bin:$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin"
export PYTHONPATH="$GITLYTICS_HOME"

# Airflow: native in WSL2 (D17), metadata in the gitlytics-airflow-db container
export AIRFLOW_HOME="$GITLYTICS_HOME/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$GITLYTICS_HOME/airflow/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__PARALLELISM=4
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__DEFAULT_TIMEZONE=utc
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${AIRFLOW_DB_USER}:${AIRFLOW_DB_PASSWORD}@localhost:5433/airflow"
export AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8080
# Silence the Airflow 3 timer-unit deprecation warning
export AIRFLOW__METRICS__TIMER_UNIT_CONSISTENCY=True
