#!/usr/bin/env bash
# Start HiveServer2 (JDBC on port 10000, web UI on 10002) for beeline and GUI clients like DBeaver.
# Uses about 1 GB of RAM, so start it only when you want to query interactively.
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

if ss -ltn | grep -q ':10000 '; then
    echo "HiveServer2 already running"
    exit 0
fi

# setsid: keeps HiveServer2 running after the terminal that started it is closed
setsid -f hive --service hiveserver2 < /dev/null > /tmp/hiveserver2.log 2>&1
echo -n "Starting HiveServer2 (takes about a minute)"
for _ in $(seq 1 36); do
    ss -ltn | grep -q ':10000 ' && { echo; echo "Ready: jdbc:hive2://localhost:10000/gitlytics   UI: http://localhost:10002"; exit 0; }
    echo -n "."; sleep 5
done
echo; echo "Did not start; see /tmp/hiveserver2.log" >&2
exit 1
