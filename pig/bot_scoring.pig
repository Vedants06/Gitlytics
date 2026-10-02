/*
 * Gitlytics bot scoring. Reads silver events, scores every account per era with
 * behaviour rules, and flags accounts scoring 4 or more.
 *
 *   R2  > 200 events in one hour                                   weight 3
 *   R3  > 50 distinct repos in one hour                            weight 3
 *   R4  active in all 24 hours of one day                          weight 2
 *   R5  > 100 stars in one hour                                    weight 4
 *   R6  >= 200 events and > 95% of them one event type             weight 1
 *   R7  starred the same repo 5 or more times (star/unstar loop)   weight 4
 *   R8  star-only account that starred 3 or more farmed repos      weight 4
 *
 * Farmed repo: >= 20 distinct starrers, >= 60% of them star-only accounts with >= 5 stars.
 * Accounts ending in [bot] are labels (is_labelled_bot) for measuring recall; the rules never use the suffix.
 *
 * Outputs (tab-separated):
 *   /gitlytics/gold/bot_scores      every account that fired a rule or is a labelled bot
 *   /gitlytics/gold/farmed_repos    repos whose stars come mainly from star farms
 *   /gitlytics/gold/bot_rule_eval   per era: labelled bots caught, unlabelled accounts flagged
 *   /gitlytics/tmp/pig_known_bots   flagged accounts without the [bot] suffix (-> known_bots)
 */

%default INPUT '/gitlytics/silver/events/dt=*/hr=*'

rmf /gitlytics/gold/bot_scores
rmf /gitlytics/gold/farmed_repos
rmf /gitlytics/gold/bot_rule_eval
rmf /gitlytics/tmp/pig_known_bots

raw = LOAD '$INPUT' USING PigStorage('\t') AS (
    event_id:long, event_type:chararray, actor_login:chararray, is_bot:chararray,
    repo_owner:chararray, repo:chararray, created_at:chararray, weekday:chararray, era:chararray);

ev = FOREACH raw GENERATE
    era,
    actor_login                              AS actor,
    event_type                               AS type,
    CONCAT(repo_owner, CONCAT('/', repo))    AS repo,
    SUBSTRING(created_at, 0, 13)             AS hour,
    (actor_login MATCHES '.*\\[bot\\]' ? 1 : 0) AS labelled;

-- Account totals ------------------------------------------------------------
by_account = GROUP ev BY (era, actor);
account = FOREACH by_account {
    repos = DISTINCT ev.repo;
    hours = DISTINCT ev.hour;
    stars = FILTER ev BY type == 'WatchEvent';
    GENERATE FLATTEN(group) AS (era, actor),
             COUNT(ev) AS events, COUNT(repos) AS distinct_repos, COUNT(hours) AS active_hours,
             COUNT(stars) AS stars, MAX(ev.labelled) AS labelled;
};

-- Hourly peaks (R2, R3, R5) ---------------------------------------------------
by_hour = GROUP ev BY (era, actor, hour);
hourly = FOREACH by_hour {
    repos = DISTINCT ev.repo;
    stars = FILTER ev BY type == 'WatchEvent';
    GENERATE FLATTEN(group) AS (era, actor, hour), COUNT(ev) AS n_events, COUNT(repos) AS n_repos, COUNT(stars) AS n_stars;
};
peaks = FOREACH (GROUP hourly BY (era, actor)) GENERATE
    FLATTEN(group) AS (era, actor),
    MAX(hourly.n_events) AS peak_events, MAX(hourly.n_repos) AS peak_repos, MAX(hourly.n_stars) AS peak_stars;

-- Hours active per day (R4) ----------------------------------------------------
hour_days = FOREACH hourly GENERATE era, actor, SUBSTRING(hour, 0, 10) AS day;
day_hours = FOREACH (GROUP hour_days BY (era, actor, day)) GENERATE
    FLATTEN(group) AS (era, actor, day), COUNT(hour_days) AS hours_in_day;
max_day = FOREACH (GROUP day_hours BY (era, actor)) GENERATE
    FLATTEN(group) AS (era, actor), MAX(day_hours.hours_in_day) AS max_hours_in_day;

-- Dominant event type (R6) -----------------------------------------------------
type_counts = FOREACH (GROUP ev BY (era, actor, type)) GENERATE
    FLATTEN(group) AS (era, actor, type), COUNT(ev) AS n;
dominant = FOREACH (GROUP type_counts BY (era, actor)) GENERATE
    FLATTEN(group) AS (era, actor), MAX(type_counts.n) AS top_type_events;

-- Stars per account and repo (R7, R8) -------------------------------------------
star_events = FILTER ev BY type == 'WatchEvent';
account_repo_stars = FOREACH (GROUP star_events BY (era, actor, repo)) GENERATE
    FLATTEN(group) AS (era, actor, repo), COUNT(star_events) AS n;
repeat_stars = FOREACH (GROUP account_repo_stars BY (era, actor)) GENERATE
    FLATTEN(group) AS (era, actor), MAX(account_repo_stars.n) AS max_same_repo_stars;

star_only = FILTER account BY stars == events AND stars >= 5;
farm_links_j = JOIN account_repo_stars BY (era, actor), star_only BY (era, actor);
farm_links = FOREACH farm_links_j GENERATE
    account_repo_stars::era AS era, account_repo_stars::actor AS actor, account_repo_stars::repo AS repo;

repo_starrers = FOREACH (GROUP account_repo_stars BY (era, repo)) GENERATE
    FLATTEN(group) AS (era, repo), COUNT(account_repo_stars) AS starrers;
repo_farm = FOREACH (GROUP farm_links BY (era, repo)) GENERATE
    FLATTEN(group) AS (era, repo), COUNT(farm_links) AS farm_starrers;
repo_mix_j = JOIN repo_starrers BY (era, repo), repo_farm BY (era, repo);
repo_mix = FOREACH repo_mix_j GENERATE
    repo_starrers::era AS era, repo_starrers::repo AS repo, starrers, farm_starrers,
    (double) farm_starrers / (double) starrers AS farm_pct;
farmed_repos = FILTER repo_mix BY starrers >= 20 AND farm_pct >= 0.6;

farm_hits_j = JOIN farm_links BY (era, repo), farmed_repos BY (era, repo);
farm_hits = FOREACH (GROUP farm_hits_j BY (farm_links::era, farm_links::actor)) GENERATE
    FLATTEN(group) AS (era, actor), COUNT(farm_hits_j) AS farmed_repos_starred;

-- Combine (left outer joins keep every account) ----------------------------------
j1 = JOIN account BY (era, actor), peaks BY (era, actor);
a1 = FOREACH j1 GENERATE account::era AS era, account::actor AS actor, events, distinct_repos, active_hours,
     stars, labelled, peak_events, peak_repos, peak_stars;
j2 = JOIN a1 BY (era, actor) LEFT OUTER, max_day BY (era, actor);
a2 = FOREACH j2 GENERATE a1::era AS era, a1::actor AS actor, events, distinct_repos, active_hours, stars,
     labelled, peak_events, peak_repos, peak_stars, max_hours_in_day;
j3 = JOIN a2 BY (era, actor) LEFT OUTER, dominant BY (era, actor);
a3 = FOREACH j3 GENERATE a2::era AS era, a2::actor AS actor, events, distinct_repos, active_hours, stars,
     labelled, peak_events, peak_repos, peak_stars, max_hours_in_day, top_type_events;
j4 = JOIN a3 BY (era, actor) LEFT OUTER, repeat_stars BY (era, actor);
a4 = FOREACH j4 GENERATE a3::era AS era, a3::actor AS actor, events, distinct_repos, active_hours, stars,
     labelled, peak_events, peak_repos, peak_stars, max_hours_in_day, top_type_events,
     (max_same_repo_stars IS NULL ? 0L : max_same_repo_stars) AS max_same_repo_stars;
j5 = JOIN a4 BY (era, actor) LEFT OUTER, farm_hits BY (era, actor);
features = FOREACH j5 GENERATE a4::era AS era, a4::actor AS actor, events, distinct_repos, active_hours, stars,
     labelled, peak_events, peak_repos, peak_stars, max_hours_in_day,
     (double) top_type_events / (double) events AS top_type_share,
     max_same_repo_stars,
     (farmed_repos_starred IS NULL ? 0L : farmed_repos_starred) AS farmed_repos_starred;

-- Score ---------------------------------------------------------------------
rules = FOREACH features GENERATE *,
    (peak_events > 200 ? 3 : 0)                              AS r2,
    (peak_repos > 50 ? 3 : 0)                                AS r3,
    (max_hours_in_day >= 24 ? 2 : 0)                         AS r4,
    (peak_stars > 100 ? 4 : 0)                               AS r5,
    (events >= 200 AND top_type_share > 0.95 ? 1 : 0)        AS r6,
    (max_same_repo_stars >= 5 ? 4 : 0)                       AS r7,
    (farmed_repos_starred >= 3 ? 4 : 0)                      AS r8;

scored = FOREACH rules GENERATE era, actor, events, distinct_repos, active_hours, stars,
    peak_events, peak_repos, peak_stars, max_hours_in_day, top_type_share, max_same_repo_stars, farmed_repos_starred,
    CONCAT((r2 > 0 ? 'R2 ' : ''), CONCAT((r3 > 0 ? 'R3 ' : ''), CONCAT((r4 > 0 ? 'R4 ' : ''),
        CONCAT((r5 > 0 ? 'R5 ' : ''), CONCAT((r6 > 0 ? 'R6 ' : ''), CONCAT((r7 > 0 ? 'R7 ' : ''), (r8 > 0 ? 'R8' : ''))))))) AS rules_fired,
    r2 + r3 + r4 + r5 + r6 + r7 + r8 AS bot_score,
    labelled;

interesting = FILTER scored BY bot_score > 0 OR labelled == 1;
ranked = ORDER interesting BY bot_score DESC, events DESC;
STORE ranked INTO '/gitlytics/gold/bot_scores' USING PigStorage('\t');

farmed_sorted = ORDER farmed_repos BY era, farm_starrers DESC;
STORE farmed_sorted INTO '/gitlytics/gold/farmed_repos' USING PigStorage('\t');

-- Evaluation: how many labelled [bot] accounts do the rules catch, and how many others get flagged
eval_rows = FOREACH scored GENERATE era, labelled, (bot_score >= 4 ? 1 : 0) AS flagged;
evaluation = FOREACH (GROUP eval_rows BY (era, labelled)) GENERATE
    FLATTEN(group) AS (era, labelled), COUNT(eval_rows) AS accounts, SUM(eval_rows.flagged) AS flagged;
STORE evaluation INTO '/gitlytics/gold/bot_rule_eval' USING PigStorage('\t');

-- New known bots: flagged accounts the [bot] suffix does not already cover
new_bots = FILTER scored BY bot_score >= 4 AND labelled == 0;
new_bot_logins = DISTINCT (FOREACH new_bots GENERATE actor);
STORE new_bot_logins INTO '/gitlytics/tmp/pig_known_bots' USING PigStorage('\t');
