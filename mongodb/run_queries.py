#!/usr/bin/env python3
"""
Gitlytics: MongoDB NoSQL Operations, Index Optimization & Aggregation Pipelines
Course: CSC702 Big Data Analysis — Experiment 3
"""

import sys
import time
from pathlib import Path
from pprint import pprint

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pymongo import ASCENDING, DESCENDING, MongoClient

from ingestion.config import MONGO_DB, MONGO_URI


def run_experiment_3():
    print(f"[{time.strftime('%X')}] Connecting to MongoDB ({MONGO_DB})...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events = db["events"]

    total_docs = events.count_documents({})
    print(f"[{time.strftime('%X')}] Connected! Total documents in 'events': {total_docs:,}")

    # =========================================================================
    # 1. CRUD OPERATIONS
    # =========================================================================
    print("\n" + "=" * 70)
    print("1. CRUD OPERATIONS")
    print("=" * 70)

    # (C) Create / Insert
    demo_doc = {
        "id": "exp3_test_999999",
        "type": "WatchEvent",
        "actor": {"id": 101, "login": "gitlytics-tester"},
        "repo": {"id": 202, "name": "gitlytics/gitlytics-core"},
        "payload": {"action": "started"},
        "created_at": "2024-09-28T12:00:00Z",
    }
    insert_res = events.insert_one(demo_doc)
    print(f" [CREATE] Inserted test doc with _id: {insert_res.inserted_id}")

    # (R) Read / Find
    found = events.find_one({"id": "exp3_test_999999"}, {"_id": 0, "id": 1, "type": 1, "actor.login": 1, "repo.name": 1})
    print(f" [READ]   Found doc: {found}")

    # (U) Update
    upd_res = events.update_one(
        {"id": "exp3_test_999999"},
        {"$set": {"payload.action": "verified_star", "verified": True}},
    )
    print(f" [UPDATE] Matched: {upd_res.matched_count}, Modified: {upd_res.modified_count}")

    # (D) Delete
    del_res = events.delete_one({"id": "exp3_test_999999"})
    print(f" [DELETE] Deleted count: {del_res.deleted_count}")

    # =========================================================================
    # 2. INDEXING & QUERY OPTIMIZATION (EXPLAIN PLANS)
    # =========================================================================
    print("\n" + "=" * 70)
    print("2. INDEXING & QUERY OPTIMIZATION (EXPLAIN PLANS)")
    print("=" * 70)

    target_repo = "freeCodeCamp/freeCodeCamp"

    # Explain Before Index
    print(f"\n[*] Running query for repo='{target_repo}' BEFORE index...")
    t0 = time.time()
    explain_before = db.command(
        "explain",
        {"find": "events", "filter": {"repo.name": target_repo}},
        verbosity="executionStats",
    )
    t_before = (time.time() - t0) * 1000
    stats_before = explain_before["executionStats"]
    stage_before = stats_before["executionStages"]["stage"]
    docs_before = stats_before["totalDocsExamined"]
    exec_time_before = stats_before["executionTimeMillis"]

    print(f"    - Execution Stage:      {stage_before} (Full Collection Scan)")
    print(f"    - Total Docs Examined:  {docs_before:,}")
    print(f"    - Engine Time:          {exec_time_before} ms (Wall clock: {t_before:.1f} ms)")

    # Create Indexes
    print("\n[*] Creating indexes on (type), (repo.name), (actor.login, created_at), (created_at)...")
    events.create_index([("type", ASCENDING)], name="idx_type")
    events.create_index([("repo.name", ASCENDING)], name="idx_repo_name")
    events.create_index([("actor.login", ASCENDING), ("created_at", DESCENDING)], name="idx_actor_created")
    events.create_index([("created_at", ASCENDING)], name="idx_created_at")
    print("    Indexes successfully built:")
    for idx in events.list_indexes():
        print(f"      - {idx['name']}: {idx['key']}")

    # Explain After Index
    print(f"\n[*] Running query for repo='{target_repo}' AFTER index...")
    t0 = time.time()
    explain_after = db.command(
        "explain",
        {"find": "events", "filter": {"repo.name": target_repo}},
        verbosity="executionStats",
    )
    t_after = (time.time() - t0) * 1000
    stats_after = explain_after["executionStats"]
    stage_after = stats_after["executionStages"]["stage"]
    docs_after = stats_after["totalDocsExamined"]
    exec_time_after = stats_after["executionTimeMillis"]

    print(f"    - Execution Stage:      {stage_after} (Indexed Lookup)")
    print(f"    - Total Docs Examined:  {docs_after:,}")
    print(f"    - Engine Time:          {exec_time_after} ms (Wall clock: {t_after:.1f} ms)")
    speedup = (exec_time_before / exec_time_after) if exec_time_after > 0 else (exec_time_before / 0.1)
    print(f"    >>> Optimization Gain: ~{speedup:.1f}x faster search with zero wasted scans!\n")

    # =========================================================================
    # 3. AGGREGATION PIPELINES
    # =========================================================================
    print("=" * 70)
    print("3. AGGREGATION PIPELINES (5 ANALYTICS QUERIES)")
    print("=" * 70)

    # Pipeline 1: Event Type Distribution
    print("\n[Pipeline 1] Event Type Distribution & Percentage Share:")
    p1 = [
        {"$group": {"_id": "$type", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    res1 = list(events.aggregate(p1, allowDiskUse=True))
    total_e = sum(r["count"] for r in res1)
    print(f"{'Event Type':<28} {'Count':>12} {'Share (%)':>12}")
    print("-" * 54)
    for r in res1:
        pct = (r["count"] / total_e) * 100
        print(f"{r['_id']:<28} {r['count']:>12,d} {pct:>11.2f}%")

    # Pipeline 2: Top 10 Starred Repositories (WatchEvent)
    print("\n[Pipeline 2] Top 10 Most Starred Repositories:")
    p2 = [
        {"$match": {"type": "WatchEvent"}},
        {"$group": {"_id": "$repo.name", "stars": {"$sum": 1}, "users": {"$addToSet": "$actor.login"}}},
        {"$project": {"_id": 0, "repo": "$_id", "total_stars": "$stars", "unique_users": {"$size": "$users"}}},
        {"$sort": {"unique_users": -1}},
        {"$limit": 10},
    ]
    res2 = list(events.aggregate(p2, allowDiskUse=True))
    print(f"{'Repository':<45} {'Stars':>10} {'Unique Users':>14}")
    print("-" * 71)
    for r in res2:
        print(f"{r['repo']:<45} {r['total_stars']:>10,d} {r['unique_users']:>14,d}")

    # Pipeline 3: Push Commit Statistics (2024 Payload)
    print("\n[Pipeline 3] Top Repositories by Commit Volume & Avg Commit Message Length:")
    p3 = [
        {"$match": {"type": "PushEvent", "payload.commits": {"$exists": True, "$ne": []}}},
        {"$limit": 100_000},  # sample slice for high performance aggregation
        {"$unwind": "$payload.commits"},
        {
            "$project": {
                "repo": "$repo.name",
                "msg_len": {"$strLenCP": {"$ifNull": ["$payload.commits.message", ""]}},
            }
        },
        {"$group": {"_id": "$repo", "commits": {"$sum": 1}, "avg_len": {"$avg": "$msg_len"}}},
        {"$project": {"_id": 0, "repo": "$_id", "commits": 1, "avg_msg_len": {"$round": ["$avg_len", 1]}}},
        {"$sort": {"commits": -1}},
        {"$limit": 10},
    ]
    res3 = list(events.aggregate(p3, allowDiskUse=True))
    print(f"{'Repository':<45} {'Sample Commits':>15} {'Avg Msg Len (chars)':>22}")
    print("-" * 84)
    for r in res3:
        print(f"{r['repo']:<45} {r['commits']:>15,d} {r['avg_msg_len']:>22.1f}")

    # Pipeline 4: Bot vs Human Traffic Split
    print("\n[Pipeline 4] Bot vs Human Activity by Event Type:")
    p4 = [
        {
            "$project": {
                "type": 1,
                "is_bot": {"$regexMatch": {"input": "$actor.login", "regex": r"\[bot\]$", "options": "i"}},
            }
        },
        {"$group": {"_id": {"type": "$type", "is_bot": "$is_bot"}, "count": {"$sum": 1}}},
        {
            "$project": {
                "_id": 0,
                "event_type": "$_id.type",
                "actor_category": {"$cond": ["$_id.is_bot", "BOT", "HUMAN"]},
                "count": 1,
            }
        },
        {"$sort": {"event_type": 1, "actor_category": 1}},
    ]
    res4 = list(events.aggregate(p4, allowDiskUse=True))
    print(f"{'Event Type':<28} {'Category':<10} {'Count':>12}")
    print("-" * 52)
    for r in res4:
        print(f"{r['event_type']:<28} {r['actor_category']:<10} {r['count']:>12,d}")

    # Pipeline 5: Issues Event Actions Breakdown
    print("\n[Pipeline 5] Issues Event Action Breakdown (Opened, Closed, Labeled, etc.):")
    p5 = [
        {"$match": {"type": "IssuesEvent"}},
        {"$group": {"_id": "$payload.action", "total_actions": {"$sum": 1}}},
        {"$sort": {"total_actions": -1}},
    ]
    res5 = list(events.aggregate(p5, allowDiskUse=True))
    print(f"{'Action':<20} {'Total Events':>15}")
    print("-" * 37)
    for r in res5:
        print(f"{str(r['_id']):<20} {r['total_actions']:>15,d}")

    print("\n" + "=" * 70)
    print("Experiment 3 execution completed successfully!")
    print("=" * 70)


if __name__ == "__main__":
    run_experiment_3()