# Gitlytics: Open-Source Trend Intelligence, Bot Detection & Repository Recommendation

**Course:** CSC702 Big Data Analysis (B.E. Sem VII, Mumbai University) · **Status:** Planning · **Last updated:** 30 Sep 2026

Gitlytics ingests the public GitHub event stream every hour (about 100,000 events per hour, verified on 29 Sep 2026) into a Hadoop-based data lake. It then uses that data to detect trending repositories, flag bot activity and recommend repositories. It covers all 8 experiments of the Big Data Analytics lab and Modules 1 to 6 of the syllabus.

---

## Contents

1. [Decisions at a glance](#1-decisions-at-a-glance)
2. [Problem and use cases](#2-problem-and-use-cases)
3. [Goals, non-goals and success metrics](#3-goals-non-goals-and-success-metrics)
4. [Dataset](#4-dataset)
5. [Architecture](#5-architecture)
6. [Pipeline flow](#6-pipeline-flow)
7. [Lab experiment mapping](#7-lab-experiment-mapping)
8. [Analytics modules](#8-analytics-modules)
9. [Data model](#9-data-model)
10. [Repository structure](#10-repository-structure)
11. [Environment and tooling](#11-environment-and-tooling)
12. [Implementation plan](#12-implementation-plan)
13. [Timeline](#13-timeline)
14. [Team split](#14-team-split)
15. [Risks and mitigations](#15-risks-and-mitigations)
16. [Open questions](#16-open-questions)
17. [Sources](#17-sources)

---

## 1. Decisions at a glance

We are switching from the food-inflation topic to GitHub event analytics. The new source is large, updates every hour, and no other group in the class uses it.

| # | Decision | Choice | Why |
|---|---|---|---|
| D1 | Project topic | Gitlytics: trends, bot detection and repo recommendation on GitHub events | Unique in the class; covers recommender systems (Module 5) and streams (Module 4), which most topics skip |
| D2 | Data source | Raw hourly files from `data.gharchive.org` | Free, public, no API key, a new file every hour |
| D3 | BigQuery | Not used | It bypasses Hadoop; the lab is graded on HDFS, MapReduce, Hive and Pig |
| D4 | Data scope | 2 days of 2024 backfill + at least 7 days of live 2026 hours | 2024 files still have commit messages and more stars; 2026 gives the live feed |
| D5 | Orchestration | Apache Airflow, hourly DAG, **catchup on** | A new file lands every hour. GH Archive keeps old files, so hours missed while the laptop was asleep are fetched automatically on wake-up |
| D6 | Raw storage | MinIO bucket as the landing zone (bronze) | Keeps an untouched copy of every file; re-runs never re-download. Image: `chainguard/minio`, because the official community image and binary were discontinued in 2025 |
| D7 | Lake storage | HDFS, partitioned by date and hour | Syllabus requirement; one hourly file per partition |
| D8 | Raw file format in HDFS | Keep `.json.gz` as-is | Hadoop reads gzip natively; each hourly file becomes one map task, so the parallelism is real |
| D9 | Cleaned format (silver) | Tab-separated text, one table per event family | Readable by MapReduce, Hive and Pig with no extra libraries |
| D10 | Warehouse | Hive external tables over silver and gold | Syllabus tool; takes the role Snowflake would have |
| D11 | Snowflake | Not used | The trial expires after 30 days, and it is not a Hadoop-ecosystem tool |
| D12 | NoSQL | MongoDB, one full day of raw events with nested payloads | Native JSON; aggregation pipelines per event type |
| D13 | Stream algorithms | Python, replaying events in time order | Bloom filter (lab 6), plus sampling, Flajolet-Martin and DGIM from Module 4 |
| D14 | Recommender | Item-based collaborative filtering on user × repo stars | Module 5.1 and the 5.2 product-recommendation case study |
| D15 | Graph analysis | Repo co-contribution graph with community detection | Module 5.3 |
| D16 | Visualisation | R for lab 7 (mandatory); Power BI dashboard as a bonus | R is graded; many groups use Power BI, so it is secondary |
| D17 | Where services run | Hadoop, Hive, Pig and Airflow natively in WSL2 Ubuntu; MinIO and MongoDB in Docker | Native Hadoop gives the `jps` and web-UI screenshots lab manuals expect; Airflow can call `hdfs` directly |
| D18 | Time zone | All partitions and timestamps in UTC | GH Archive names files by UTC hour. Convert to IST (UTC + 5:30) only in charts |
| D19 | Repository | New repo `gitlytics`; the food-inflation work is kept as an archive | Clean history for the report |

---

## 2. Problem and use cases

GitHub produces about 2.4 million public events a day, far more than anyone can read by hand. Developers, companies and platform teams need it summarised: what is trending, who is a bot, and what to look at next.

| User | Question they ask | What Gitlytics gives them |
|---|---|---|
| Developer | Which new repos are taking off right now? | Hourly trending list based on star velocity |
| Developer | What should I star or contribute to next? | Top-10 repo recommendations from their star history |
| Engineering manager | Which open-source projects are healthy and active? | Activity index per repo: pushes, issues and contributors per day |
| Platform / trust team | Which accounts are bots or star-farmers? | Bot score per account, with the rule that fired |
| Researcher | Which projects form communities of shared contributors? | Repo communities from the co-contribution graph |
| Analyst | When is GitHub busiest, and with which event types? | Hour-of-day × weekday heatmaps and event-mix trends |

---

## 3. Goals, non-goals and success metrics

The project succeeds when the pipeline runs unattended for 7 days and every lab experiment has a working output from our own data.

**Goals**

1. Ingest every GH Archive hour automatically, with no manual downloads.
2. Store at least 25 million events across HDFS partitions (backfill plus live).
3. Deliver all 8 lab experiments on this one dataset.
4. Answer every question in the use-case table with numbers and charts.

**Non-goals**

- A public web app or live website.
- Deep learning models or GPU work.
- Calling the GitHub REST API (rate-limited, and not needed).
- Full history since 2011 (too large for a laptop).

| Metric | Target | How we measure it |
|---|---|---|
| Hourly ingestion success | 99% of hours landed within 30 min of publication (while the laptop is on) | Airflow task history over 7 days |
| Total events in HDFS | 25 million or more | `hdfs dfs -du -h` and Hive `COUNT(*)` |
| Parallel map tasks | 1 per hourly file (48+ in one job) | MapReduce job counters screenshot |
| Silver parse errors | Under 0.1% of lines | Counter in the flatten job |
| Duplicate events after cleaning | 0 by event id | Hive `COUNT(DISTINCT event_id)` vs `COUNT(*)` |
| Bloom filter false-positive rate | Within 20% of the theoretical rate | Measured vs formula, in a table |
| Flajolet-Martin error | Under 15% vs the exact distinct count | Estimate vs Hive exact count, per hour |
| Recommender hit rate @10 | Clearly above the popularity baseline | Hold out each user's last star |

---

## 4. Dataset

GH Archive publishes one gzipped JSON file per UTC hour, about 5 minutes after the hour ends. We checked this against the live server on 29 Sep 2026.

**URL pattern:** `https://data.gharchive.org/YYYY-MM-DD-H.json.gz`. `H` is the hour from 0 to 23, UTC, without a leading zero.

| Measure | One hour (2026-09-29, 12:00 UTC) | Per day (approx.) | Per week (approx.) |
|---|---|---|---|
| Events | 100,721 | 2.4 million | 17 million |
| Compressed size | 21 MB | 500 MB | 3.5 GB |
| Uncompressed JSON | 97 MB | 2.3 GB | 16 GB |
| Distinct users (actors) | 38,614 | | |
| Distinct repos | 59,607 | | |
| Bot events (login ends in `[bot]`) | 13,843 (14%) | | |

The per-day and per-week figures are that one hour multiplied out. Real traffic varies by hour and weekday.

**Event mix in that hour:** PushEvent 75,261 · CreateEvent 15,407 · DeleteEvent 2,944 · PullRequestEvent 2,625 · IssueCommentEvent 1,458 · IssuesEvent 965 · PullRequestReviewEvent 663 · WatchEvent (star) 447 · ReleaseEvent 238 · ForkEvent 116.

### Schema drift (important)

GitHub trimmed its Events API payloads during 2025. Compressed hourly files fell from 62–90 MB (mid-2025) to 15–22 MB (Sep 2026).

| Field | 2024 files | 2026 files |
|---|---|---|
| Push commits with messages | Present (checked) | Missing (0 of 75,261 pushes) |
| Issue titles | Expected present (confirm in week 1) | Present (965 of 965) |
| Issue comment bodies | Expected present (confirm in week 1) | Present (1,458 of 1,458) |
| Pull request titles | Expected present (confirm in week 1) | Missing (0 of 2,625) |
| Stars (WatchEvent) per hour | Expected higher (confirm in week 1) | About 450 |

This drives the scope decision (D4):

- **Backfill:** 2024-09-28 and 2024-09-29, 48 hourly files, about 2.8 GB compressed. Used for the commit-message word count and to seed the recommender with stars.
- **Live:** every hour from go-live, for at least 7 days. Used for the streaming algorithms, trends, bot detection and dashboards.
- **Expected total:** 25 to 30 million events. The 2024 size is an estimate from one file (59 MB compressed per hour).

The pipeline records an `era` (`v2024` / `v2026`) on every row. Each job knows which fields to expect, and the report can present this as a real schema-drift problem.

---

## 5. Architecture

```
                         data.gharchive.org  (one .json.gz per UTC hour)
                                     │
                                     ▼
┌─────────────────────────────── Apache Airflow (WSL2) ────────────────────────────────┐
│  gh_hourly_ingest ──► gh_silver_build ──► gh_daily_analytics      gh_backfill (manual)│
└──────┬──────────────────────┬─────────────────────────┬──────────────────────────────┘
       │ upload               │ hdfs dfs -put           │ runs jobs
       ▼                      ▼                         ▼
┌──────────────┐   ┌─────────────────────────────────────────────────────────────┐
│ MinIO        │   │ HDFS  /gitlytics                                             │
│ gh-raw/      │   │   bronze/dt=/hr=/*.json.gz   (raw, 1 file = 1 map task)     │
│ (landing,    │   │   silver/<table>/dt=/hr=/    (flattened TSV)                │
│  bronze)     │   │   gold/<metric>/dt=/         (aggregates)                   │
└──────────────┘   └───────┬───────────────┬───────────────┬─────────────────────┘
                           │               │               │
                  ┌────────▼──────┐ ┌──────▼──────┐ ┌──────▼───────┐
                  │ MapReduce     │ │ Hive        │ │ Pig          │
                  │ word count,   │ │ warehouse,  │ │ bot scoring  │
                  │ stars, pairs  │ │ trends      │ │              │
                  └────────┬──────┘ └──────┬──────┘ └──────┬───────┘
                           └───────────────┼───────────────┘
                                           ▼
        ┌──────────────────┐   ┌───────────────────────┐   ┌────────────────────────┐
        │ MongoDB (Docker) │   │ Python stream replay  │   │ Recommender + graph    │
        │ 1 day raw events │   │ Bloom · FM · DGIM ·   │   │ item-based CF ·        │
        │ aggregations     │   │ sampling              │   │ communities            │
        └────────┬─────────┘   └───────────┬───────────┘   └───────────┬────────────┘
                 └─────────────────────────┼───────────────────────────┘
                                           ▼
                               exports/*.csv (gold)
                                  │            │
                                  ▼            ▼
                           R (lab 7)     Power BI (bonus)
```

---

## 6. Pipeline flow

Data moves through three layers (bronze, silver, gold), driven by four Airflow DAGs. Every step can be re-run for any hour without creating duplicates.

| Layer | Where | Format | Contents | Written by |
|---|---|---|---|---|
| Landing (bronze) | MinIO `gh-raw/YYYY/MM/DD/` | `.json.gz`, untouched | Exact file from GH Archive | `gh_hourly_ingest` |
| Raw lake (bronze) | HDFS `/gitlytics/bronze/dt=/hr=/` | `.json.gz`, untouched | Same file, used as MapReduce input | `gh_hourly_ingest` |
| Silver | HDFS `/gitlytics/silver/<table>/dt=/hr=/` | Tab-separated text | Flattened, de-duplicated, typed, bot-flagged rows | `gh_silver_build` |
| Gold | HDFS `/gitlytics/gold/<metric>/dt=/` | Tab-separated text | Trends, bot scores, recommendations, communities | `gh_daily_analytics` |
| Serving | `exports/` CSV + MongoDB | CSV, JSON | Inputs for R, Power BI and the report | `gh_daily_analytics` |

### Airflow DAGs

| DAG | Schedule | Steps |
|---|---|---|
| `gh_hourly_ingest` | `@hourly` with a 10-min offset, catchup on, `max_active_runs=3` | 1. HTTP sensor waits for the file (checks every 5 min, times out after 2 h). 2. Download to a local temp folder. 3. Validate: gzip opens, over 1,000 lines, every line is JSON. 4. Upload to MinIO. 5. `hdfs dfs -put -f` into the bronze partition. 6. Write the row count to `ingest_log`. 7. Delete the temp file. |
| `gh_backfill` | Manual, with a start and end hour as parameters | The same steps over a range of past hours. Used for 2024-09-28 and 2024-09-29. |
| `gh_silver_build` | Triggered after each successful ingest | 1. Flatten (Hadoop Streaming job). 2. Drop duplicate event ids. 3. Flag bots. 4. Split into silver tables. 5. `ALTER TABLE ... ADD PARTITION`. 6. Record the schema-drift check. |
| `gh_daily_analytics` | `@daily` at 01:00 UTC | 1. MapReduce jobs. 2. Hive gold queries. 3. Pig bot scoring. 4. Stream-algorithm replay over the previous day. 5. Load one day into MongoDB (first run only). 6. Export CSVs for R and Power BI. |

### Cleaning rules (silver build)

1. Parse `created_at` as UTC; derive `dt`, `hr` and `weekday`.
2. Drop an event whose `id` was already seen, since a replay can re-deliver a file.
3. Lower-case logins and repo names; split `repo.name` into `repo_owner` and `repo`.
4. Set `is_bot` = login ends with `[bot]`, or the login is in `config/known_bots.txt`.
5. Set `era` = `v2024` or `v2026` from the payload shape, so later jobs know which fields exist.
6. Strip tabs and newlines from free text (commit messages, titles, comments) so TSV rows stay intact.
7. Write lines that fail to parse to `/gitlytics/quarantine/dt=/hr=/`, and count them.

---

## 7. Lab experiment mapping

Every experiment runs on Gitlytics data, so the lab file doubles as the project report.

| Exp | Lab aim | CO | What we build | Evidence for the lab file |
|---|---|---|---|---|
| 1 | Install Hadoop; basic file commands | CO1 | Pseudo-distributed Hadoop in WSL2; bronze/silver/gold HDFS layout | `jps`, `hdfs dfs -ls -R`, `-du -h`, `-count`, NameNode UI, `hdfs fsck` block report |
| 2 | MapReduce word count | CO2 | (a) Word count on 2024 commit messages and 2026 issue titles, with a combiner and stop-words. (b) Stars per repo per day | Job counters showing 48+ map tasks; top 20 words; top repos |
| 3 | MongoDB NoSQL commands | CO3 | One day of raw events with nested payloads; CRUD, indexes, aggregation pipelines | Shell output for each command; `explain()` before and after an index |
| 4 | Hive: create a database, analyse data | CO3 | `gitlytics` database; external partitioned tables over silver; 8 analysis queries | Query text and results; partition pruning shown with `EXPLAIN` |
| 5 | Pig script analysis | CO3 | Bot-scoring script: `LOAD`, `FILTER`, `GROUP`, `FOREACH`, `JOIN`, `ORDER`, `STORE` | Script, `DESCRIBE` / `ILLUSTRATE` output, top suspected bots |
| 6 | Bloom filter membership | CO4 | Bloom filter of the known-bot set, applied to the replayed event stream | False-positive rate, measured vs formula, for several bit-array sizes and hash counts |
| 7 | Data visualisation in R | CO5 (as in the lab list) | 10 charts from gold CSVs | PNG files and R scripts |
| 8 | Any big data technique | CO6 (as in the lab list) | Repo recommender and graph communities; plus Flajolet-Martin and DGIM | Hit rate @10 vs baseline; community table; estimate-vs-exact tables |

**Hive queries (experiment 4)**

1. Events per hour by type, with the busiest hours shown in UTC and IST.
2. Top 20 repos by stars in the last 24 hours.
3. Event mix per day (share of each event type).
4. Share of events made by bots, per event type.
5. Issues opened vs closed per day.
6. Most active contributors, excluding bots.
7. Week-over-week growth in stars per repo (trend candidates).
8. Repos with the most distinct contributors.

**R charts (experiment 7)**

1. Events per hour, stacked by type (time series).
2. Hour-of-day × weekday heatmap (IST).
3. Event-type share, 2024 vs 2026 (shows the schema drift).
4. Top 20 trending repos (bar chart of trend score).
5. Star-velocity lines for the top 5 trending repos.
6. Bot vs human activity by event type.
7. Distribution of events per account, log scale (bots stand out in the long tail).
8. Bloom filter: measured vs theoretical false-positive rate.
9. Flajolet-Martin estimate vs exact distinct actors per hour.
10. Repo community network plot (`igraph`).

---

## 8. Analytics modules

### 8.1 Trending repositories (Hive + DGIM)

- `stars_24h` = stars in the last 24 hours; `baseline` = mean daily stars over the prior 7 days.
- `trend_score = (stars_24h − baseline) / sqrt(baseline + 1)`, counted only for repos with at least 20 stars in 24 hours.
- The daily top 20 goes to `gold.trending_daily`.

### 8.2 Bot detection (Pig)

| Rule | Signal | Weight |
|---|---|---|
| R1 | Login ends with `[bot]` (ground-truth label, never used to score) | – |
| R2 | More than 200 events in one hour | 3 |
| R3 | More than 50 distinct repos in one hour | 3 |
| R4 | Active in all 24 hours of a day | 2 |
| R5 | More than 100 stars in one hour (star farming) | 4 |
| R6 | Over 95% of events are one type, with at least 100 events a day | 1 |

- `bot_score` = the sum of weights of the rules that fired; an account is flagged at 4 or more.
- Evaluation: the `[bot]` accounts are the labelled positives, so we report recall of R2–R6 on them. We also list the top non-`[bot]` accounts flagged, for manual review.

### 8.3 Stream algorithms (Module 4, Python replay)

Events are replayed in `created_at` order to simulate a live stream.

| Algorithm | Syllabus | Question it answers | How we check it |
|---|---|---|---|
| Sampling | 4.2 | Keep a fixed 10% of users, not 10% of events, by hashing the login | Per-user averages: sample vs full data |
| Bloom filter | 4.3 (lab 6) | Is this actor a known bot? Drop the event before storage | Measured false-positive rate vs `(1 − e^(−kn/m))^k` for m = 4n, 8n, 16n and the optimal k = (m/n)·ln 2 |
| Flajolet-Martin | 4.4 | How many distinct actors were active this hour? | Median of group averages over several hash functions, vs Hive's exact `COUNT(DISTINCT)` |
| DGIM | 4.5 | How many stars did a repo get in the last N events? | Estimate vs exact count; error stays within the 50% bound |
| Decaying window | 4.5 | Which repos are most popular right now? | Top 10 by exponentially decayed star count vs the 8.1 ranking |

### 8.4 Repository recommender (Module 5.1–5.2)

- **Data:** user × repo star pairs (WatchEvent) from the backfill plus the live days. Users with 2–500 stars, excluding bots.
- **Similarity:** a MapReduce job counts co-starred repo pairs, then cosine similarity = `co_stars(a,b) / sqrt(stars(a) · stars(b))`.
- **Scoring:** for a user, sum the similarity of every candidate repo to the repos they already starred, then take the top 10.
- **Cold start:** users with fewer than 2 stars get the current trending list (8.1).
- **Evaluation:** hide each user's last star, then measure hit rate @10 vs a most-popular baseline.

### 8.5 Repo communities (Module 5.3)

- **Graph:** nodes are repos. An edge joins two repos that share non-bot contributors (actors of Push, PullRequest or Issues events). Keep edges with a weight of 2 or more.
- **Method:** Louvain community detection on the full graph; Girvan-Newman on a small subgraph to show the syllabus algorithm step by step.
- **Output:** `gold.communities` (repo, community id, size) and a network plot in R.

---

## 9. Data model

### HDFS layout

```
/gitlytics
├── bronze/dt=2026-10-12/hr=14/2026-10-12-14.json.gz
├── silver/
│   ├── events/dt=/hr=/        all events, common fields
│   ├── stars/dt=/hr=/         WatchEvent
│   ├── commits/dt=/hr=/       PushEvent commits (v2024 only)
│   ├── issues/dt=/hr=/        IssuesEvent
│   └── comments/dt=/hr=/      IssueCommentEvent
├── gold/
│   ├── hourly_activity/dt=/
│   ├── trending_daily/dt=/
│   ├── bot_scores/dt=/
│   ├── recommendations/dt=/
│   └── communities/dt=/
├── quarantine/dt=/hr=/
└── mr_output/                 raw MapReduce outputs (lab 2)
```

### Hive tables (database `gitlytics`, all EXTERNAL, TSV)

| Table | Columns | Partitioned by |
|---|---|---|
| `events` | `event_id BIGINT, event_type STRING, actor_login STRING, is_bot BOOLEAN, repo_owner STRING, repo STRING, created_at TIMESTAMP, weekday STRING, era STRING` | `dt STRING, hr INT` |
| `stars` | `actor_login STRING, repo_owner STRING, repo STRING, created_at TIMESTAMP` | `dt, hr` |
| `commits` | `repo STRING, actor_login STRING, sha STRING, message STRING` | `dt, hr` |
| `issues` | `repo STRING, actor_login STRING, action STRING, issue_number INT, title STRING` | `dt, hr` |
| `comments` | `repo STRING, actor_login STRING, issue_number INT, body_length INT, body STRING` | `dt, hr` |
| `trending_daily` | `repo STRING, stars_24h INT, baseline DOUBLE, trend_score DOUBLE, rank INT` | `dt` |
| `bot_scores` | `actor_login STRING, events INT, distinct_repos INT, active_hours INT, rules_fired STRING, bot_score INT, is_labelled_bot BOOLEAN` | `dt` |
| `recommendations` | `actor_login STRING, rank INT, repo STRING, score DOUBLE` | `dt` |
| `communities` | `repo STRING, community_id INT, community_size INT` | `dt` |

### MongoDB (database `gitlytics`)

| Collection | Contents | Indexes |
|---|---|---|
| `events` | One full day of raw events, payload kept nested (about 2.4 million documents) | `type`, `repo.name`, `actor.login`, `created_at` |
| `ingest_log` | One document per hour: file, size, line count, status, duration | `hour` (unique) |
| `bot_profiles` | Per-account summary and rules fired (from Pig gold output) | `actor_login` (unique) |

---

## 10. Repository structure

```
gitlytics/
├── README.md                      short project intro + setup
├── PRD.md                         this document
├── docker-compose.yml             MinIO + MongoDB
├── .env.example                   MinIO / Mongo credentials, paths
├── requirements.txt               Python dependencies
├── config/
│   ├── pipeline.yaml              HDFS paths, MinIO bucket, date ranges
│   ├── known_bots.txt             seed list for is_bot and the Bloom filter
│   └── stopwords.txt              for the word count
├── scripts/
│   └── env.sh                     source in every terminal: .env, Java, Hadoop, venv, Airflow settings
├── setup/
│   ├── install_hadoop.sh          Hadoop 3.3.6 pseudo-distributed in WSL2
│   ├── install_hive.sh            Hive 3.1.3 + guava fix + Derby metastore
│   ├── install_pig.sh             Pig 0.17.0
│   ├── install_airflow.sh         Airflow 2.11.2 + requirements into .venv
│   ├── hadoop-conf/               core-site, hdfs-site, mapred-site, yarn-site
│   └── wslconfig.example          memory limit for WSL2
├── airflow/
│   ├── dags/
│   │   ├── gh_hourly_ingest.py
│   │   ├── gh_backfill.py
│   │   ├── gh_silver_build.py
│   │   └── gh_daily_analytics.py
│   (DAGs import the ingestion package; scripts/env.sh puts the repo root on PYTHONPATH)
├── ingestion/
│   ├── config.py                  settings from .env (endpoints, paths, thresholds)
│   ├── gharchive.py               hour naming, publish check, download, validate + era detection
│   ├── storage.py                 MinIO upload, HDFS bronze put
│   ├── ingest_log.py              one MongoDB document per hour
│   ├── pipeline.py                ingest_hour(): the end-to-end step both DAGs call
│   └── cli.py                     run the same ingestion by hand, without Airflow
├── mapreduce/
│   ├── flatten/                   Hadoop Streaming: bronze JSON → silver TSV
│   ├── wordcount/                 Java: WordCount mapper, combiner, reducer, driver (lab 2)
│   ├── stars_per_repo/            Java: stars per repo per day
│   └── copairs/                   Java: co-starred repo pairs (recommender)
├── hive/
│   ├── 01_create_database.hql
│   ├── 02_create_silver_tables.hql
│   ├── 03_create_gold_tables.hql
│   ├── 10_analysis_queries.hql    the 8 lab-4 queries
│   └── 20_gold_trending.hql
├── pig/
│   └── bot_scoring.pig            lab 5
├── mongodb/
│   ├── load_day.py                one day of raw events → MongoDB
│   └── queries.js                 CRUD, indexes, aggregations (lab 3)
├── streaming/
│   ├── replay.py                  time-ordered event generator
│   ├── bloom_filter.py            lab 6
│   ├── flajolet_martin.py
│   ├── dgim.py
│   ├── sampling.py
│   └── decaying_window.py
├── recommender/
│   ├── build_matrix.py
│   ├── item_cf.py
│   └── evaluate.py                hit rate @10 vs popularity baseline
├── graph/
│   ├── build_graph.py
│   └── communities.py             Louvain + Girvan-Newman demo
├── r/
│   ├── 00_load.R
│   └── 01..10_*.R                 one script per chart (lab 7)
├── powerbi/
│   └── gitlytics.pbix              bonus dashboard
├── exports/                       gold CSVs (git-ignored)
├── report/
│   ├── screenshots/               per experiment
│   └── lab_file.docx
└── tests/
    ├── test_validate.py
    ├── test_bloom_filter.py
    └── test_flatten.py
```

---

## 11. Environment and tooling

Everything runs on one Windows 11 laptop. Hadoop, Hive, Pig and Airflow run natively in WSL2 (Ubuntu 24.04); MinIO, MongoDB and the Airflow metadata database (Postgres) run in Docker Desktop with WSL integration.

| Component | Version | Runs in | Notes |
|---|---|---|---|
| Java | OpenJDK 8 | WSL2 | Hive 3.1.3 and Pig 0.17 need Java 8 |
| Hadoop | 3.3.6 | WSL2 | Pseudo-distributed, replication 1, block size 128 MB |
| Hive | 3.1.3 | WSL2 | Replace `hive/lib/guava-19.0.jar` with Hadoop's `guava-27.0-jre.jar`, or Hive fails at startup; Derby metastore |
| Pig | 0.17.0 | WSL2 | MapReduce mode |
| Airflow | 2.11.2 | WSL2, `.venv` | LocalExecutor; metadata in the `gitlytics-airflow-db` Postgres container on host port 5433 (5432 is taken by a Windows PostgreSQL); UI on port 8080 |
| Python | 3.12 | WSL2 | `requests`, `minio`, `pymongo`, `mmh3`, `networkx`, `python-louvain`, `pandas` |
| MinIO | `chainguard/minio:latest` | Docker | Console on port 9001, API on host port 9100 (9000 is taken by the HDFS NameNode); the `gh-raw` bucket is created by the ingestion code |
| MongoDB | 7.0 | Docker | Host port 27018 (27017 is taken by a MongoDB service installed on Windows); volume on the WSL2 disk |
| R | 4.4 | Windows or WSL2 | `ggplot2`, `dplyr`, `data.table`, `lubridate`, `igraph` |
| Power BI Desktop | latest | Windows | Reads `exports/*.csv` |

**Laptop limits**

- **RAM:** 16 GB recommended. Cap WSL2 at 10 GB in `.wslconfig`. Stop MongoDB while running heavy MapReduce jobs.
- **Disk:** about 30 GB used (bronze kept twice, in MinIO and HDFS, about 13 GB; silver about 10 GB; MongoDB day about 5 GB), so keep 60 GB free.
- **Sleep:** when the laptop sleeps, hourly runs pause. With catchup on (D5), missed hours are fetched once it wakes.

---

## 12. Implementation plan

### Phase 1: Environment (week 1)

- [ ] Install WSL2 Ubuntu 24.04, set `.wslconfig` memory to 10 GB
- [ ] Install Java 8 and Hadoop 3.3.6; format the NameNode; start HDFS and YARN
- [ ] Create the `/gitlytics` HDFS layout; capture the **Exp 1** screenshots
- [ ] Install Hive 3.1.3 (guava fix) and Pig 0.17.0; run a smoke test on each
- [ ] `docker compose up` MinIO and MongoDB; create the `gh-raw` bucket
- [ ] Install Airflow in a venv; confirm a hello-world DAG runs
- [ ] Download one 2024 hour and one 2026 hour; confirm the schema-drift table in section 4

### Phase 2: Ingestion (week 2)

- [x] `ingestion` package: download, validate with era detection, MinIO, HDFS, ingest log
- [ ] `gh_hourly_ingest` DAG with HTTP sensor, retries, MinIO upload and HDFS put
- [ ] `gh_backfill` DAG; load 2024-09-28 and 2024-09-29 (48 files)
- [ ] **Go live:** switch on the hourly DAG (the clock for 7+ days of data starts here)
- [ ] `ingest_log` in MongoDB; check for gaps each day

### Phase 3: Silver layer and MapReduce (week 3)

- [ ] Hadoop Streaming flatten job: bronze → 5 silver tables, with de-duplication and the bot flag
- [ ] `gh_silver_build` DAG triggered after each ingest
- [ ] **Exp 2:** Java WordCount with combiner and stop-words on commit messages and issue titles
- [ ] Stars-per-repo-per-day MapReduce job
- [ ] Record job counters (map tasks, input records, combine ratio)

### Phase 4: Warehouse and NoSQL (week 4)

- [ ] **Exp 4:** Hive database, external partitioned tables, the 8 analysis queries, `EXPLAIN` for pruning
- [ ] Gold: `hourly_activity` and `trending_daily`
- [ ] **Exp 3:** load one day into MongoDB; CRUD, indexes, `explain()`, 5+ aggregation pipelines

### Phase 5: Pig and bot detection (week 5)

- [ ] **Exp 5:** `bot_scoring.pig` with rules R2–R6
- [ ] Recall on `[bot]` accounts; manual review of the top 20 flagged non-`[bot]` accounts
- [ ] Export `bot_scores` and update `known_bots.txt`

### Phase 6: Stream algorithms (week 6)

- [ ] `replay.py` time-ordered generator
- [ ] **Exp 6:** Bloom filter with a false-positive experiment (m = 4n, 8n, 16n)
- [ ] Flajolet-Martin vs exact distinct actors per hour
- [ ] DGIM and decaying window vs exact counts
- [ ] 10% user sampling vs full-data comparison

### Phase 7: Recommender, graph and R (week 7)

- [ ] Co-starred pairs MapReduce job → cosine similarity
- [ ] Item-based CF top 10; hit rate @10 vs popularity baseline
- [ ] Repo co-contribution graph; Louvain; Girvan-Newman on a subgraph
- [ ] **Exp 8** write-up: recommender + communities + FM/DGIM
- [ ] **Exp 7:** the 10 R charts from gold CSVs

### Phase 8: Dashboard, report, viva (week 8)

- [ ] `gh_daily_analytics` DAG running the full gold refresh end to end
- [ ] Power BI dashboard (bonus)
- [ ] Lab file: aim, theory, code, output screenshots, conclusion for each experiment
- [ ] Final numbers: total events, days live, ingestion success rate
- [ ] Viva prep: why HDFS, why gzip gives one mapper per file, how Bloom false positives arise, schema drift

---

## 13. Timeline

This assumes a start on Monday 5 Oct 2026 and 8 weeks of work. Adjust it once the submission date is known (see open questions).

```
Week            1        2        3        4        5        6        7        8
Starts        Oct 5   Oct 12   Oct 19   Oct 26   Nov 2    Nov 9    Nov 16   Nov 23
              ───────────────────────────────────────────────────────────────────────
Pipeline (A)  [Env ][Ingest ][Silver+MR]                         [Daily DAG ]
                        ▲ G1              ▲ G2
Analytics (B) [Env ]          [ Plan Hive ][Hive+Mongo][ Pig  ]  [ R charts ][Report]
Streams (A)                                        [Bloom FM DGIM]
Exp 8 (A+B)                                                  [Recsys+Graph]
Live data     ........████████████████████████████████████████████████████████  (hourly)
                                                                      ▲ G3      ▲ G4
```

| Gate | When | Pass condition |
|---|---|---|
| G1 | End of week 2 (Oct 16) | Hourly DAG green for 24 hours in a row; backfill of 48 files done |
| G2 | End of week 3 (Oct 23) | 25 million+ events in HDFS; silver tables queryable in Hive |
| G3 | End of week 7 (Nov 20) | Every experiment (1–8) has code and an output screenshot |
| G4 | End of week 8 (Nov 27) | Lab file and report submitted; viva questions rehearsed |

---

## 14. Team split

| Area | Member A (pipeline) | Member B (analytics) |
|---|---|---|
| Environment | Hadoop, Hive, Pig, Airflow setup | MinIO, MongoDB (Docker), R setup |
| Ingestion and silver | All DAGs, flatten job, validation | Reviews data quality reports |
| Exp 1: HDFS | Owner | Screenshots on their own machine, if possible |
| Exp 2: MapReduce | Owner | Picks stop-words, interprets results |
| Exp 3: MongoDB | Support | Owner |
| Exp 4: Hive | Creates the tables | Writes the 8 queries |
| Exp 5: Pig | Support | Owner |
| Exp 6: Bloom filter | Owner | Plots the false-positive chart |
| Exp 7: R | Supplies gold CSVs | Owner |
| Exp 8: Recommender + graph | Co-pairs MapReduce job, CF | Graph communities, network plot |
| Report and viva | Pipeline and streams chapters | Analysis and visualisation chapters |

---

## 15. Risks and mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Guide rejects the topic change | Medium | High | Get approval before week 1; the food-inflation cleaning work stays as a fallback |
| Hadoop, Hive or Pig version conflicts | High | Medium | Fixed versions (section 11); guava fix scripted in `install_hive.sh` |
| Laptop runs out of RAM | Medium | Medium | Cap WSL2 at 10 GB; run MongoDB only when needed; one heavy job at a time |
| Disk fills up | Medium | High | Delete local temp files after upload; keep only 2 bronze copies; watch `hdfs dfs -df -h` |
| Laptop asleep, hours missed | High | Low | Catchup on (D5); GH Archive keeps every file |
| A GH Archive hour is late or missing | Low | Low | Sensor waits up to 2 h, then the run fails and is recorded in `ingest_log` as a gap |
| GitHub changes the payload again | Low | Medium | `gharchive.validate()` detects the era; unknown shapes go to quarantine, not into silver |
| Too few stars for the recommender | Medium | Medium | Seed with 2024 backfill stars; add more backfill days if hit-rate evaluation has fewer than 1,000 users |
| Only one laptop can run the stack | Medium | Medium | Member B works from exported CSVs and MongoDB dumps |

---

## 16. Open questions

- What is the final submission date and the viva date? The timeline assumes the week of 23 Nov 2026.
- Has the guide approved switching from food inflation to Gitlytics?
- Does each member's laptop have 16 GB RAM and 60 GB free disk?
- Will the whole stack run on one laptop, or do both members need it?
- Is Power BI accepted as extra work, or should everything visual be in R?
- Should the 2024 backfill be longer than 2 days if the week-1 check shows fewer stars than expected?

---

## 17. Sources

- GH Archive, project page and URL format: https://www.gharchive.org
- Hourly files: https://data.gharchive.org/ (sizes and event counts above measured on 29 Sep 2026 from `2026-09-29-12.json.gz`, `2025-06-15-12`, `2025-10-15-12` and `2024-09-29-12`)
- CSC702 Big Data Analysis syllabus and lab experiment list, Mumbai University, academic year 2025–26
- A. Rajaraman, J. Ullman, *Mining of Massive Datasets* (Bloom filter, Flajolet-Martin, DGIM, recommender systems, social-graph clustering)
