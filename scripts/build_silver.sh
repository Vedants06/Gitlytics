#!/usr/bin/env bash
# Build the silver layer with Hive.
#   scripts/build_silver.sh                  every bronze partition (first run, or a full rebuild)
#   scripts/build_silver.sh 2026-09-30 5     one hour (what the hourly DAG calls)
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"
cd "$GITLYTICS_HOME"

# Known-bot list -> HDFS (empty at first; the Pig bot scoring adds to it later)
hdfs dfs -mkdir -p /gitlytics/reference/known_bots
hdfs dfs -put -f config/known_bots.txt /gitlytics/reference/known_bots/known_bots.txt

hive -S -f hive/02_create_bronze_table.hql
hive -S -f hive/03_create_silver_tables.hql

if [ $# -eq 0 ]; then
    FILTER="1=1"
    hive -S -e "USE gitlytics; MSCK REPAIR TABLE bronze_events;"
elif [ $# -eq 2 ]; then
    FILTER="dt='$1' AND hr=$2"
    hive -S -e "USE gitlytics; ALTER TABLE bronze_events ADD IF NOT EXISTS PARTITION (dt='$1', hr=$2) LOCATION '/gitlytics/bronze/dt=$1/hr=$2';"
else
    echo "usage: $0 [YYYY-MM-DD HOUR]" >&2
    exit 2
fi

echo "Building silver where: $FILTER"
hive --hivevar filter="$FILTER" -f hive/10_build_silver.hql
