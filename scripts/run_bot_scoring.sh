#!/usr/bin/env bash
# Run the Pig bot scoring, publish the flagged accounts to known_bots, and export results.
# Afterwards rebuild silver and gold so the new known bots are excluded:
#     scripts/build_silver.sh && scripts/build_gold.sh
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"
cd "$GITLYTICS_HOME"

pig -x mapreduce -l /tmp -f pig/bot_scoring.pig

# known_bots table = the (manual) seed file + the Pig-flagged accounts
hdfs dfs -mkdir -p /gitlytics/reference/known_bots
hdfs dfs -cat /gitlytics/tmp/pig_known_bots/part-* | hdfs dfs -put -f - /gitlytics/reference/known_bots/known_bots_pig.txt

mkdir -p exports/gold
{
    printf 'era\tactor_login\tevents\tdistinct_repos\tactive_hours\tstars\tpeak_events_hour\tpeak_repos_hour\tpeak_stars_hour\tmax_hours_in_day\ttop_type_share\tmax_same_repo_stars\tfarmed_repos_starred\trules_fired\tbot_score\tis_labelled_bot\n'
    hdfs dfs -cat /gitlytics/gold/bot_scores/part-*
} > exports/gold/bot_scores.tsv
{ printf 'era\trepo\tstarrers\tfarm_starrers\tfarm_pct\n'; hdfs dfs -cat /gitlytics/gold/farmed_repos/part-*; } > exports/gold/farmed_repos.tsv
{ printf 'era\tis_labelled_bot\taccounts\tflagged\n'; hdfs dfs -cat /gitlytics/gold/bot_rule_eval/part-*; } > exports/gold/bot_rule_eval.tsv

echo
echo "Rule evaluation (is_labelled_bot=1 rows: [bot] accounts caught; 0 rows: other accounts flagged):"
column -t -s $'\t' exports/gold/bot_rule_eval.tsv
echo "New known bots: $(hdfs dfs -cat /gitlytics/reference/known_bots/known_bots_pig.txt | wc -l)"
echo "Farmed repos:   $(($(wc -l < exports/gold/farmed_repos.tsv) - 1))"
