#!/usr/bin/env bash
# Lab 2: commit-message word count.
#   mapreduce/run_wordcount.sh silver   7.2M commits from the silver table (main result)
#   mapreduce/run_wordcount.sh raw      raw 2024 bronze .json.gz files, one map task per hourly file
# Results: HDFS /gitlytics/mr_output/wordcount_<mode>/{counts,top}
#          exports/wordcount_<mode>_top50.tsv and exports/wordcount_<mode>_counters.txt
set -euo pipefail

MODE="${1:-silver}"
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/scripts/env.sh"
cd "$GITLYTICS_HOME"

case "$MODE" in
    silver) INPUT="/gitlytics/silver/commits/dt=*/hr=*" ;;
    raw)    INPUT="/gitlytics/bronze/dt=2024-*/hr=*" ;;
    *)      echo "usage: $0 silver|raw" >&2; exit 2 ;;
esac
OUTPUT="/gitlytics/mr_output/wordcount_${MODE}"

[ -f mapreduce/gitlytics-mr.jar ] || bash mapreduce/build.sh
mkdir -p exports

hadoop jar mapreduce/gitlytics-mr.jar gitlytics.wordcount.WordCountDriver \
    -files config/stopwords.txt \
    -D gitlytics.input.format="$MODE" \
    -D gitlytics.top.n=50 \
    "$INPUT" "$OUTPUT" 2>&1 | tee "exports/wordcount_${MODE}_counters.txt"

hdfs dfs -cat "$OUTPUT/top/part-r-00000" > "exports/wordcount_${MODE}_top50.tsv"
echo
echo "Top 20 words ($MODE):"
head -20 "exports/wordcount_${MODE}_top50.tsv"
