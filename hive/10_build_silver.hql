-- Build silver from bronze for the partitions matching ${hivevar:filter}
--   all data:  --hivevar filter="1=1"
--   one hour:  --hivevar filter="dt='2026-09-30' AND hr=5"
-- INSERT OVERWRITE with dynamic partitions replaces only the partitions it writes,
-- so re-running an hour is safe.
USE gitlytics;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=5000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.exec.compress.output=false;
SET hive.auto.convert.join=true;

-- Pass 1: one scan of bronze feeds four silver tables (Hive multi-table insert).
--   rn   = 1 keeps the first copy of each event id (drops duplicates)
--   era  = v2024 if any push event in that hour still carries commits
FROM (
    SELECT
        CAST(e.id AS BIGINT)                                   AS event_id,
        e.type                                                 AS event_type,
        lower(e.actor.login)                                   AS actor_login,
        (lower(e.actor.login) LIKE '%[bot]' OR kb.login IS NOT NULL) AS is_bot,
        lower(split(e.repo.name, '/')[0])                      AS repo_owner,
        lower(split(e.repo.name, '/')[1])                      AS repo,
        CAST(regexp_replace(regexp_replace(e.created_at, 'T', ' '), 'Z', '') AS TIMESTAMP) AS created_at,
        e.payload                                              AS payload,
        CASE WHEN max(CASE WHEN e.type = 'PushEvent' AND size(e.payload.commits) > 0 THEN 1 ELSE 0 END)
                  OVER (PARTITION BY e.dt, e.hr) = 1
             THEN 'v2024' ELSE 'v2026' END                     AS era,
        row_number() OVER (PARTITION BY e.id ORDER BY e.created_at) AS rn,
        e.dt                                                   AS dt,
        e.hr                                                   AS hr
    FROM bronze_events e
    LEFT JOIN known_bots kb ON lower(e.actor.login) = kb.login
    WHERE ${hivevar:filter}
) b
INSERT OVERWRITE TABLE events PARTITION (dt, hr)
    SELECT event_id, event_type, actor_login, is_bot, repo_owner, repo, created_at,
           date_format(created_at, 'EEE'), era, dt, hr
    WHERE rn = 1
INSERT OVERWRITE TABLE stars PARTITION (dt, hr)
    SELECT actor_login, is_bot, repo_owner, repo, created_at, dt, hr
    WHERE rn = 1 AND event_type = 'WatchEvent'
INSERT OVERWRITE TABLE issues PARTITION (dt, hr)
    SELECT repo_owner, repo, actor_login, is_bot, payload.action, payload.issue.number,
           regexp_replace(payload.issue.title, '[\\t\\n\\r]+', ' '), created_at, dt, hr
    WHERE rn = 1 AND event_type = 'IssuesEvent'
INSERT OVERWRITE TABLE comments PARTITION (dt, hr)
    SELECT repo_owner, repo, actor_login, is_bot, payload.issue.number, length(payload.comment.body),
           substr(regexp_replace(payload.comment.body, '[\\t\\n\\r]+', ' '), 1, 1000), created_at, dt, hr
    WHERE rn = 1 AND event_type = 'IssueCommentEvent';

-- Pass 2: one row per commit (only v2024 hours have commits in their push payloads).
INSERT OVERWRITE TABLE commits PARTITION (dt, hr)
SELECT
    lower(split(e.repo.name, '/')[0]),
    lower(split(e.repo.name, '/')[1]),
    lower(e.actor.login),
    lower(e.actor.login) LIKE '%[bot]',
    c.sha,
    substr(regexp_replace(c.message, '[\\t\\n\\r]+', ' '), 1, 1000),
    CAST(regexp_replace(regexp_replace(e.created_at, 'T', ' '), 'Z', '') AS TIMESTAMP),
    e.dt,
    e.hr
FROM bronze_events e
LATERAL VIEW explode(e.payload.commits) cv AS c
WHERE e.type = 'PushEvent' AND ${hivevar:filter};
