-- Silver: flattened, de-duplicated, typed, bot-flagged rows. Tab-separated text so that
-- MapReduce and Pig can read the same files Hive writes. Partitioned like bronze (UTC).
-- era: v2024 = push payloads carry commits, v2026 = trimmed payloads (GitHub, 2025).
-- Note: never put a semicolon in an inline comment, the Hive CLI splits statements on it.
USE gitlytics;

CREATE EXTERNAL TABLE IF NOT EXISTS events (
    event_id    BIGINT,
    event_type  STRING,
    actor_login STRING,
    is_bot      BOOLEAN,
    repo_owner  STRING,
    repo        STRING,
    created_at  TIMESTAMP,
    weekday     STRING,
    era         STRING
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gitlytics/silver/events';

CREATE EXTERNAL TABLE IF NOT EXISTS stars (
    actor_login STRING,
    is_bot      BOOLEAN,
    repo_owner  STRING,
    repo        STRING,
    created_at  TIMESTAMP
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gitlytics/silver/stars';

CREATE EXTERNAL TABLE IF NOT EXISTS commits (
    repo_owner  STRING,
    repo        STRING,
    actor_login STRING,
    is_bot      BOOLEAN,
    sha         STRING,
    message     STRING,     -- tabs/newlines replaced by spaces, first 1,000 characters
    created_at  TIMESTAMP
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gitlytics/silver/commits';

CREATE EXTERNAL TABLE IF NOT EXISTS issues (
    repo_owner   STRING,
    repo         STRING,
    actor_login  STRING,
    is_bot       BOOLEAN,
    action       STRING,
    issue_number INT,
    title        STRING,
    created_at   TIMESTAMP
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gitlytics/silver/issues';

CREATE EXTERNAL TABLE IF NOT EXISTS comments (
    repo_owner   STRING,
    repo         STRING,
    actor_login  STRING,
    is_bot       BOOLEAN,
    issue_number INT,
    body_length  INT,
    body         STRING,    -- tabs/newlines replaced by spaces, first 1,000 characters
    created_at   TIMESTAMP
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/gitlytics/silver/comments';
