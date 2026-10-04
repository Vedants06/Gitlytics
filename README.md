# Gitlytics

**Open-source trend intelligence, bot detection, and ecosystem recommendations over the public GitHub event stream.**

![Airflow](https://img.shields.io/badge/Airflow-2.11.2-017CEE?logo=apacheairflow&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-66CCFF?logo=apachehadoop&logoColor=black)
![Hive](https://img.shields.io/badge/Hive-3.1.3-FDEE21?logo=apachehive&logoColor=black)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![R](https://img.shields.io/badge/R-4.4-276DC3?logo=r&logoColor=white)

Gitlytics is a Big Data Analytics platform built on [GH Archive](https://www.gharchive.org/). It ingests hourly event payloads into a Hadoop lakehouse, isolates schema drift between API versions, flags bot accounts and star-farming rings, runs stream-mining algorithms, and produces repository recommendations and developer-ecosystem graphs.

---

## Table of Contents

- [Highlights](#highlights)
- [Results](#results)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Technical Deep Dive](#technical-deep-dive)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Visualizations](#visualizations)

---

## Highlights

- **Automated ingestion:** Airflow DAGs pull hourly `.json.gz` archives into MinIO (landing) and HDFS (bronze).
- **Hive lakehouse:** Bronze → Silver → Gold layers with de-duplication, typed TSV partitions, and era-tagged schema-drift isolation (2024 vs 2026).
- **Pig bot detection:** Heuristic scoring flags high-velocity accounts, single-event dominance, and star-farming loops.
- **NoSQL event store:** MongoDB holds 3.98M raw nested events with compound-index optimization.
- **Stream mining:** Bloom Filter, Flajolet-Martin, DGIM, and deterministic user sampling in Python.
- **Recommender:** Item-based collaborative filtering (cosine similarity) with Leave-One-Out evaluation.
- **Graph analytics:** Louvain and Girvan-Newman community detection on a repo co-contribution graph.
- **Dashboards:** 10 programmatic R plots and a 2-page Power BI dashboard.

---

## Results

| Area | Result |
|---|---|
| Data scale | **8.35M events** across 2024 backfill and 2026 live streams (~500 MB gzipped JSON/day) |
| MapReduce | Custom Combiner cut shuffle volume by **92.6%** over 26M commit-message tokens |
| MongoDB | Compound index: 51.8 ms (`COLLSCAN`) → 15.4 ms (`IXSCAN`), a **3.5x speedup** |
| Bot filtering | **848** bot accounts flagged, **112** farmed repos quarantined (`*-cracked`, `*-autoclicker`) |
| Flajolet-Martin | **5.2%** final error vs exact unique-user count over 1.5M events |
| DGIM | **2.1%–12.6%** error over a 10,000-event window (theoretical ceiling: 50%) |
| Recommender | **4.99% Hit Rate @ 10**, **+80.1%** relative gain over popularity baseline |
| Graph communities | Louvain modularity **0.9671** (ecosystems such as Homebrew, FossifyOrg, SciML) |

---

## Architecture

```
                  data.gharchive.org (Hourly .json.gz Feed)
                                     │
                                     ▼
┌───────────────────────────── Apache Airflow ─────────────────────────────┐
│  gh_hourly_ingest (@hourly)         │   gh_daily_analytics (@daily)      │
└──────┬──────────────────────┬───────┴─────────────────┬──────────────────┘
       │ MinIO Upload         │ HDFS Put                │ Runs Analytics
       ▼                      ▼                         ▼
┌──────────────┐   ┌───────────────────────────────────────────────────────┐
│ MinIO        │   │ HDFS (/gitlytics)                                     │
│ (gh-raw)     │   │   /bronze/ (*.json.gz raw files)                      │
│ Landing Zone │   │   /silver/ (Typed TSV tables: events, stars, commits) │
└──────────────┘   │   /gold/   (Aggregates: trends, bot scores, graphs)   │
                   └───────┬───────────────┬───────────────┬───────────────┘
                           │               │               │
                  ┌────────▼──────┐ ┌──────▼──────┐ ┌──────▼───────┐
                  │ MapReduce     │ │ Hive        │ │ Pig          │
                  │ WordCount     │ │ Warehouse   │ │ Bot Scoring  │
                  └────────┬──────┘ └──────┬──────┘ └──────┬───────┘
                           └───────────────┼───────────────┘
                                           ▼
        ┌──────────────────┐   ┌───────────────────────┐   ┌────────────────────────┐
        │ MongoDB Store    │   │ Stream Algorithms     │   │ Recommender & Graph    │
        │ 3.98M Raw Events │   │ Bloom, FM, DGIM       │   │ Item-CF & Louvain      │
        └────────┬─────────┘   └───────────┬───────────┘   └───────────┬────────────┘
                 └─────────────────────────┼───────────────────────────┘
                                           ▼
                                 exports/ & exports/gold/
                                           │
                                 ┌─────────┴─────────┐
                                 ▼                   ▼
                           R Visualizations     Power BI
                           (10 PNG Plots)       Dashboard
```

### Data layers

| Layer | Location | Description |
|---|---|---|
| **Bronze** | MinIO `gh-raw`, HDFS `/gitlytics/bronze/dt=/hr=/` | Raw `.json.gz` files, untouched |
| **Silver** | HDFS `/gitlytics/silver/<table>/dt=/hr=/` | Hive external tables over a JSON SerDe: de-duplicated by event ID, line breaks stripped, types enforced, `era` tag assigned (`v2024` / `v2026`); projected to typed TSVs (`events`, `stars`, `commits`, `issues`, `comments`) |
| **Gold** | HDFS `/gitlytics/gold/<metric>/dt=/` | Hourly volume, event mix, bot activity share, top distinct starrers, issue activity, top contributors, collaboration networks, trending repos (z-score vs 7-day baseline) |
| **Serving** | MongoDB | 3.98M raw nested documents for document-level CRUD and secondary indexing |

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.11.2 |
| Distributed storage | Hadoop HDFS 3.3.6, MinIO |
| Serving / NoSQL | MongoDB 7.0, PostgreSQL 16 |
| Data processing | Apache Hive 3.1.3, Apache Pig 0.17.0, Java MapReduce |
| Analytics & stream mining | Python 3.12 (pymongo, mmh3, networkx, python-louvain) |
| Visualization | R 4.4 (ggplot2, igraph, data.table), Power BI |

---

## Technical Deep Dive

### Schema drift isolation
Between 2024 and 2026, GitHub removed commit messages from `PushEvent` payloads and title fields from `PullRequestEvent` payloads. Gitlytics injects an `era` tag (`v2024` / `v2026`) during the Silver build, routing NLP workloads (e.g. MapReduce word count) to 2024 records only, while real-time stream analytics run over 2026 live data.

### Pig bot detection
A heuristic scoring script evaluates account behavior per hour and per day:

- High event velocity (> 200 events/hour)
- High repo breadth (> 50 distinct repos/hour)
- Uninterrupted 24-hour activity
- Excessive star velocity (> 100 stars/hour)
- Single event-type dominance (> 95% of one type)
- Star-farming loops (repeated stars on identical repo clusters)
- Accounts starring exclusively farmed repositories

Accounts scoring **≥ 4** are written to `config/known_bots.txt` and excluded from subsequent Silver/Gold rebuilds.

### Stream mining

| Algorithm | Purpose | Details |
|---|---|---|
| **Bloom Filter** | Drop known bots before storage | MurmurHash3 double hashing; FPR validated across m/n ratios |
| **Flajolet-Martin** | Unique active-user cardinality | 64 hash functions in 8 buckets, geometric averaging, α = 0.77351 |
| **DGIM** | Sliding-window bit counting | Exponential buckets, O(log² N) memory, 10,000-event window |
| **User sampling** | Uniform sample of the stream | Hash over actor logins, 10% sample |

### Recommender & graph mining
- **Item-based CF:** repo–repo cosine similarity over co-starred user pairs, evaluated with Leave-One-Out on 23,000 users.
- **Co-contribution graph:** nodes are repositories; edges connect repos sharing active human committers. Louvain finds clusters; Girvan-Newman edge-betweenness gives hierarchical decomposition.

---

## Repository Structure

```
gitlytics/
├── airflow/dags/
│   ├── gh_hourly_ingest.py        # Ingestion & Silver build DAG
│   └── gh_daily_analytics.py      # Analytics & Gold pipeline DAG
├── config/
│   ├── known_bots.txt             # Seed and Pig-flagged bot accounts
│   └── stopwords.txt              # Stop-words for MapReduce
├── exports/
│   ├── gold/                      # Hive gold table TSV exports
│   └── plots/                     # 10 R plots (PNG)
├── graph/
│   └── communities.py             # Louvain & Girvan-Newman analysis
├── hive/                          # HQL: database, bronze/silver/gold DDL and builds
├── ingestion/
│   ├── config.py                  # Environment and credential loader
│   ├── gharchive.py               # GH Archive publication checker
│   ├── pipeline.py                # Hourly ingestion task driver
│   └── storage.py                 # MinIO and HDFS interfaces
├── mapreduce/wordcount/           # Java MapReduce (Combiner + Driver)
├── mongodb/
│   ├── load_day.py                # Bulk loader (~27k docs/s)
│   ├── queries.js                 # CRUD, indexes, aggregation pipelines
│   └── run_queries.py             # Python driver with execution stats
├── pig/bot_scoring.pig            # Bot detection script
├── powerbi/gitlytics.pbix         # 2-page Power BI dashboard
├── r/                             # 10 R plotting scripts
├── recommender/item_cf.py         # Item-CF & Leave-One-Out eval
├── scripts/                       # env.sh, build_silver.sh, build_gold.sh,
│                                  # run_bot_scoring.sh, start_services.sh
└── streaming/
    ├── bloom_filter.py
    ├── dgim.py
    ├── flajolet_martin.py
    ├── replay.py                  # MongoDB stream event generator
    ├── run_streaming.py           # Benchmark pipeline driver
    └── seed_bots.py               # Bot login seeder
```

---

## Getting Started

### Prerequisites

- Linux or WSL2 (Ubuntu 24.04) with Java 8, OpenSSL, Python 3.12, R 4.4
- Hadoop 3.3.6, Hive 3.1.3, Pig 0.17.0, Airflow 2.11.2
- Docker Desktop running MinIO, MongoDB 7.0, and PostgreSQL 16

### Setup

```bash
git clone https://github.com/Vedants06/gitlytics.git
cd gitlytics

# Load environment variables and start supporting Docker services
source scripts/env.sh
./scripts/start_services.sh
```

### Start Hadoop

```bash
start-dfs.sh
start-yarn.sh
nohup $HADOOP_HOME/bin/mapred historyserver > /tmp/historyserver.log 2>&1 &
```

---

## Running the Pipeline

### Option A: Airflow orchestration

```bash
source scripts/env.sh
airflow standalone
```

Open `http://localhost:8080` and trigger or monitor:

- `gh_hourly_ingest` (`@hourly`): fetches files, lands them in MinIO/HDFS, builds Hive Silver.
- `gh_daily_analytics` (`@daily`): Pig bot detection, Silver/Gold rebuilds, stream algorithms, recommender evaluation, R plots.

### Option B: Manual execution

```bash
# Hive Silver and Gold builds
bash scripts/build_silver.sh
bash scripts/build_gold.sh

# Pig bot detection
bash scripts/run_bot_scoring.sh

# MongoDB load and queries
python mongodb/load_day.py --date 2024-09-28 --drop
python mongodb/run_queries.py

# Stream mining
python streaming/seed_bots.py
python streaming/run_streaming.py

# Recommender and graph
python recommender/item_cf.py
python graph/communities.py

# R visualizations
for script in r/*.R; do Rscript "$script"; done
```

---

## Visualizations

Ten R plots are written to `exports/plots/`:

| # | Script | Plot |
|---|---|---|
| 01 | `01_hourly_activity.R` | Stacked area of hourly activity |
| 02 | `02_weekday_heatmap.R` | Activity intensity heatmap (IST) |
| 03 | `03_event_mix_comparison.R` | Event distribution by era |
| 04 | `04_top_starred_repos.R` | Top repos by distinct starrers |
| 05 | `05_bot_vs_human_share.R` | Bot vs human traffic share |
| 06 | `06_bot_score_distribution.R` | Pig bot score histogram |
| 07 | `07_bloom_filter_fpr.R` | Bloom FPR, actual vs theoretical |
| 08 | `08_flajolet_martin_accuracy.R` | FM cardinality estimate vs exact |
| 09 | `09_recommender_performance.R` | Item-CF vs popularity baseline |
| 10 | `10_repo_network.R` | Force-directed repo network |

The interactive 2-page Power BI dashboard lives at `powerbi/gitlytics.pbix`.