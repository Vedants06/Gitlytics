#!/usr/bin/env bash
# Start everything Gitlytics needs except Airflow (run that in its own terminal:
#   source ~/gitlytics/scripts/env.sh && airflow standalone).
# Docker Desktop must already be open on Windows.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

sudo service ssh start

running="$(jps)"
grep -q " NameNode$" <<<"$running" || start-dfs.sh
grep -q " ResourceManager$" <<<"$running" || start-yarn.sh
# Pig (and job counters in the report) need the JobHistory server; without it every job
# stalls ~20 s retrying port 10020.
grep -q " JobHistoryServer$" <<<"$running" || mapred --daemon start historyserver

(cd "$GITLYTICS_HOME" && docker compose up -d)

echo
jps | sort -k2
echo
echo "HDFS  http://localhost:9870   YARN  http://localhost:8088   JobHistory  http://localhost:19888"
echo "MinIO http://localhost:9001   Airflow http://localhost:8080 (after 'airflow standalone')"
