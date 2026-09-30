-- Build every gold table from silver. Safe to re-run: each INSERT OVERWRITE replaces its table.
-- "Human" below means is_bot = false (login not ending in [bot] and not in known_bots).
USE gitlytics;

SET hive.exec.parallel=true;
SET hive.strict.checks.cartesian.product=false;

-- 1. Activity per hour and event type (busiest hours, bot load per hour)
INSERT OVERWRITE TABLE gold_hourly_activity
SELECT dt, hr, era, event_type, count(*), sum(CASE WHEN is_bot THEN 1 ELSE 0 END)
FROM events
GROUP BY dt, hr, era, event_type;

-- 2. Event mix per era, as a share of that era's events (shows the 2025 payload change)
INSERT OVERWRITE TABLE gold_event_mix
SELECT era, event_type, events, round(100 * events / sum(events) OVER (PARTITION BY era), 2)
FROM (SELECT era, event_type, count(*) AS events FROM events GROUP BY era, event_type) t;

-- 3. Share of each event type made by bots, per era
INSERT OVERWRITE TABLE gold_bot_share
SELECT era, event_type, count(*), sum(CASE WHEN is_bot THEN 1 ELSE 0 END),
       round(100 * avg(CASE WHEN is_bot THEN 1 ELSE 0 END), 2)
FROM events
GROUP BY era, event_type;

-- 4. Top 20 repos by human stars, per day
INSERT OVERWRITE TABLE gold_top_starred
SELECT dt, rnk, repo, stars
FROM (
    SELECT dt, concat(repo_owner, '/', repo) AS repo, stars,
           row_number() OVER (PARTITION BY dt ORDER BY stars DESC, repo_owner, repo) AS rnk
    FROM (SELECT dt, repo_owner, repo, count(*) AS stars
          FROM stars WHERE NOT is_bot GROUP BY dt, repo_owner, repo) daily
) ranked
WHERE rnk <= 20;

-- 5. Issues opened, closed and reopened per day
INSERT OVERWRITE TABLE gold_issue_activity
SELECT dt, action, count(*)
FROM issues
GROUP BY dt, action;

-- 6. Top 50 most active human accounts per era
INSERT OVERWRITE TABLE gold_top_contributors
SELECT era, rnk, actor_login, events, distinct_repos, active_hours
FROM (
    SELECT era, actor_login, events, distinct_repos, active_hours,
           row_number() OVER (PARTITION BY era ORDER BY events DESC, actor_login) AS rnk
    FROM (SELECT era, actor_login, count(*) AS events,
                 count(DISTINCT concat(repo_owner, '/', repo)) AS distinct_repos,
                 count(DISTINCT concat(dt, '-', hr)) AS active_hours
          FROM events WHERE NOT is_bot GROUP BY era, actor_login) a
) ranked
WHERE rnk <= 50;

-- 7. Top 50 repos by distinct human contributors (code, review and issue activity), per era
INSERT OVERWRITE TABLE gold_repo_collaboration
SELECT era, rnk, repo, contributors, events
FROM (
    SELECT era, repo, contributors, events,
           row_number() OVER (PARTITION BY era ORDER BY contributors DESC, repo) AS rnk
    FROM (SELECT era, concat(repo_owner, '/', repo) AS repo,
                 count(DISTINCT actor_login) AS contributors, count(*) AS events
          FROM events
          WHERE NOT is_bot
            AND event_type IN ('PushEvent', 'PullRequestEvent', 'PullRequestReviewEvent',
                               'PullRequestReviewCommentEvent', 'IssuesEvent', 'IssueCommentEvent')
          GROUP BY era, concat(repo_owner, '/', repo)) r
) ranked
WHERE rnk <= 50;

-- 8. Trending: a repo's human stars on a complete day vs its average over the previous
--    complete days (up to 7). trend_score = (stars - baseline) / sqrt(baseline + 1).
--    Only complete days (24 hours ingested) count, so partial days never look like drops.
WITH complete_days AS (
    SELECT dt FROM (SELECT dt, count(DISTINCT hr) AS hours FROM events GROUP BY dt) d WHERE hours = 24
),
day_window AS (
    SELECT d.dt, sum(CASE WHEN p.dt >= CAST(date_sub(d.dt, 7) AS STRING) AND p.dt < d.dt THEN 1 ELSE 0 END) AS baseline_days
    FROM complete_days d CROSS JOIN complete_days p
    GROUP BY d.dt
),
daily AS (
    SELECT s.dt, s.repo_owner, s.repo, count(*) AS stars
    FROM stars s JOIN complete_days c ON s.dt = c.dt
    WHERE NOT s.is_bot
    GROUP BY s.dt, s.repo_owner, s.repo
),
with_history AS (
    SELECT t.dt, t.repo_owner, t.repo, t.stars,
           sum(CASE WHEN p.dt >= CAST(date_sub(t.dt, 7) AS STRING) AND p.dt < t.dt THEN p.stars ELSE 0 END) AS prev_stars
    FROM daily t LEFT JOIN daily p ON t.repo_owner = p.repo_owner AND t.repo = p.repo
    GROUP BY t.dt, t.repo_owner, t.repo, t.stars
),
scored AS (
    SELECT h.dt, concat(h.repo_owner, '/', h.repo) AS repo, h.stars,
           h.prev_stars / w.baseline_days AS baseline, w.baseline_days,
           (h.stars - h.prev_stars / w.baseline_days) / sqrt(h.prev_stars / w.baseline_days + 1) AS trend_score
    FROM with_history h JOIN day_window w ON h.dt = w.dt
    WHERE w.baseline_days > 0 AND h.stars >= 20
)
FROM (SELECT scored.*, row_number() OVER (PARTITION BY dt ORDER BY trend_score DESC, repo) AS rnk FROM scored) ranked
INSERT OVERWRITE TABLE gold_trending_daily
SELECT dt, rnk, repo, stars, round(baseline, 2), baseline_days, round(trend_score, 2)
WHERE rnk <= 20;
