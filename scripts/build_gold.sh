#!/usr/bin/env bash
# Rebuild every gold table from silver, then export each one to exports/gold/<name>.tsv (with header).
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"
cd "$GITLYTICS_HOME"

hive -S -f hive/04_create_gold_tables.hql
hive -f hive/20_build_gold.hql

mkdir -p exports/gold
TABLES="hourly_activity event_mix bot_share top_starred issue_activity top_contributors repo_collaboration trending_daily"
QUERIES=""
for t in $TABLES; do QUERIES+="SELECT '### $t'; SELECT * FROM gold_$t;"; done

# One Hive session for all exports, split on the ### markers
hive -S --hiveconf hive.cli.print.header=true -e "USE gitlytics; $QUERIES" 2>/dev/null \
    | grep -v "^_c0$" \
    | awk -v dir="exports/gold" '/^### /{ file = dir "/" $2 ".tsv"; next } file { sub(/^gold_[a-z_]+\./, ""); gsub(/\tgold_[a-z_]+\./, "\t"); print > file }'

for t in $TABLES; do printf "  %-20s %6s rows\n" "$t" "$(($(wc -l < "exports/gold/$t.tsv") - 1))"; done
