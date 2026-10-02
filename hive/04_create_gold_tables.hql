-- Gold: small aggregate tables answering the project's questions. Rebuilt in full by
-- hive/20_build_gold.hql and exported to exports/gold/*.tsv for R and Power BI.
USE gitlytics;

CREATE EXTERNAL TABLE IF NOT EXISTS gold_hourly_activity (
    dt STRING, hr INT, era STRING, event_type STRING, events BIGINT, bot_events BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/hourly_activity';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_event_mix (
    era STRING, event_type STRING, events BIGINT, pct DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/event_mix';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_bot_share (
    era STRING, event_type STRING, events BIGINT, bot_events BIGINT, bot_pct DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/bot_share';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_top_starred (
    dt STRING, rank_no INT, repo STRING, stars BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/top_starred';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_issue_activity (
    dt STRING, action STRING, events BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/issue_activity';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_top_contributors (
    era STRING, rank_no INT, actor_login STRING, events BIGINT, distinct_repos BIGINT, active_hours BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/top_contributors';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_repo_collaboration (
    era STRING, rank_no INT, repo STRING, contributors BIGINT, events BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/repo_collaboration';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_trending_daily (
    dt STRING, rank_no INT, repo STRING, stars BIGINT, baseline DOUBLE, baseline_days INT, trend_score DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/trending_daily';

-- Written by pig/bot_scoring.pig (scripts/run_bot_scoring.sh), registered here for querying
CREATE EXTERNAL TABLE IF NOT EXISTS gold_bot_scores (
    era STRING, actor_login STRING, events BIGINT, distinct_repos BIGINT, active_hours BIGINT, stars BIGINT,
    peak_events_hour BIGINT, peak_repos_hour BIGINT, peak_stars_hour BIGINT, max_hours_in_day BIGINT,
    top_type_share DOUBLE, max_same_repo_stars BIGINT, farmed_repos_starred BIGINT,
    rules_fired STRING, bot_score INT, is_labelled_bot INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/bot_scores';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_farmed_repos (
    era STRING, repo STRING, starrers BIGINT, farm_starrers BIGINT, farm_pct DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/farmed_repos';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_bot_rule_eval (
    era STRING, is_labelled_bot INT, accounts BIGINT, flagged BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '/gitlytics/gold/bot_rule_eval';
