-- Bronze: the raw GH Archive files, read in place (.json.gz, one file per dt/hr partition).
-- Only the fields the silver layer needs are declared; the JSON SerDe ignores the rest
-- and returns NULL for fields an event type does not have.
USE gitlytics;

CREATE EXTERNAL TABLE IF NOT EXISTS bronze_events (
    id          STRING,
    type        STRING,
    actor       STRUCT<id: BIGINT, login: STRING>,
    repo        STRUCT<id: BIGINT, name: STRING>,
    payload     STRUCT<
                    action:  STRING,
                    commits: ARRAY<STRUCT<sha: STRING, message: STRING>>,
                    issue:   STRUCT<number: INT, title: STRING>,
                    comment: STRUCT<body: STRING>
                >,
    created_at  STRING
)
PARTITIONED BY (dt STRING, hr INT)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/gitlytics/bronze';

-- Accounts known to be bots that do not end in [bot]; filled later by the Pig bot scoring.
CREATE EXTERNAL TABLE IF NOT EXISTS known_bots (login STRING)
STORED AS TEXTFILE
LOCATION '/gitlytics/reference/known_bots';
