#!/usr/bin/env python3
"""
Item-Based Collaborative Filtering Repository Recommender (Experiment 8 / Module 5.1-5.2).
Evaluates Hit Rate @ K (K=5, 10, 20) using Leave-One-Out cross-validation vs Popularity Baseline.
"""

import math
import random
import sys
import time
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pymongo import MongoClient

from ingestion.config import MONGO_DB, MONGO_URI

EXPORT_DIR = REPO_ROOT / "exports"


def load_star_interactions():
    """Load human star interactions from MongoDB."""
    print(f"[{time.strftime('%X')}] Loading star interactions from MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events = db["events"]

    # Filter out bots and extract (user, repo) WatchEvents
    query = {
        "type": "WatchEvent",
        "actor.login": {"$not": {"$regex": r"\[bot\]$", "$options": "i"}},
    }
    cursor = events.find(query, {"_id": 0, "actor.login": 1, "repo.name": 1})

    user_stars = defaultdict(set)
    repo_stars = defaultdict(int)

    count = 0
    for doc in cursor:
        user = doc["actor"]["login"]
        repo = doc["repo"]["name"]
        user_stars[user].add(repo)
        repo_stars[repo] += 1
        count += 1

    print(f"[{time.strftime('%X')}] Loaded {count:,} stars across {len(user_stars):,} users and {len(repo_stars):,} repos.")
    return user_stars, repo_stars


def train_test_split(user_stars, min_stars=2):
    """
    Leave-One-Out Split:
    For users with >= min_stars, hold out exactly 1 starred repo as test ground-truth.
    """
    random.seed(42)
    train_user_stars = {}
    test_user_stars = {}

    eval_users_count = 0
    for user, repos in user_stars.items():
        if len(repos) >= min_stars:
            repo_list = list(repos)
            held_out = random.choice(repo_list)
            train_user_stars[user] = set(repo_list) - {held_out}
            test_user_stars[user] = held_out
            eval_users_count += 1
        else:
            train_user_stars[user] = repos

    print(f"[{time.strftime('%X')}] Split complete: {eval_users_count:,} evaluation users (with >= {min_stars} stars).")
    return train_user_stars, test_user_stars


def build_item_similarity_matrix(train_user_stars):
    """
    Compute Item-Item Cosine Similarity Matrix:
    sim(A, B) = |U_A ∩ U_B| / sqrt(|U_A| * |U_B|)
    """
    print(f"[{time.strftime('%X')}] Building repo co-occurrence & cosine similarity matrix...")
    repo_starrers = defaultdict(set)
    for user, repos in train_user_stars.items():
        for repo in repos:
            repo_starrers[repo].add(user)

    # Compute co-stars for pairs
    co_stars = defaultdict(lambda: defaultdict(int))
    for user, repos in train_user_stars.items():
        repo_list = list(repos)
        for i in range(len(repo_list)):
            for j in range(i + 1, len(repo_list)):
                r1, r2 = repo_list[i], repo_list[j]
                co_stars[r1][r2] += 1
                co_stars[r2][r1] += 1

    # Compute cosine similarity
    similarities = defaultdict(dict)
    for r1, neighbors in co_stars.items():
        size_r1 = len(repo_starrers[r1])
        for r2, count in neighbors.items():
            size_r2 = len(repo_starrers[r2])
            sim = count / math.sqrt(size_r1 * size_r2)
            similarities[r1][r2] = sim

    print(f"[{time.strftime('%X')}] Similarity matrix built for {len(similarities):,} repos with co-occurrences.")
    return similarities, repo_starrers


def evaluate_models(train_user_stars, test_user_stars, similarities, repo_starrers):
    """Evaluate Hit Rate @ K for Popularity Baseline vs Item Collaborative Filtering."""
    print(f"[{time.strftime('%X')}] Evaluating recommendations on test users...")

    # Precompute Global Popularity ranking from training set
    global_popularity = sorted(
        repo_starrers.keys(), key=lambda r: len(repo_starrers[r]), reverse=True
    )

    k_values = [5, 10, 20]
    pop_hits = {k: 0 for k in k_values}
    cf_hits = {k: 0 for k in k_values}
    total_eval_users = len(test_user_stars)

    sample_recommendations = []

    for user, target_repo in test_user_stars.items():
        user_history = train_user_stars.get(user, set())

        # 1. Popularity Baseline: Recommend top-K most popular repos not already starred
        pop_recs = [r for r in global_popularity if r not in user_history]
        for k in k_values:
            if target_repo in pop_recs[:k]:
                pop_hits[k] += 1

        # 2. Item-Based Collaborative Filtering Scoring:
        # score(candidate) = sum_{r in history} sim(r, candidate)
        candidate_scores = defaultdict(float)
        for starred_repo in user_history:
            if starred_repo in similarities:
                for cand_repo, sim_score in similarities[starred_repo].items():
                    if cand_repo not in user_history:
                        candidate_scores[cand_repo] += sim_score

        if candidate_scores:
            cf_recs = sorted(candidate_scores.keys(), key=lambda r: candidate_scores[r], reverse=True)
        else:
            # Fallback to popularity if user has no similarities
            cf_recs = pop_recs

        for k in k_values:
            if target_repo in cf_recs[:k]:
                cf_hits[k] += 1

        # Save some sample recommendations for display
        if len(sample_recommendations) < 5 and len(user_history) >= 2 and target_repo in cf_recs[:10]:
            sample_recommendations.append({
                "user": user,
                "history": list(user_history)[:3],
                "target": target_repo,
                "top_cf_recs": cf_recs[:5],
            })

    # Output Results
    print("\n" + "=" * 70)
    print(f"RECOMMENDER EVALUATION (Hit Rate @ K on {total_eval_users:,} users)")
    print("=" * 70)
    print(f"{'Metric':<15} {'Popularity Baseline':<22} {'Item-CF Recommender':<22} {'Relative Gain':<15}")
    print("-" * 74)

    eval_tsv = EXPORT_DIR / "recommender_eval.tsv"
    with open(eval_tsv, "w") as f:
        f.write("metric\tpopularity_baseline\titem_cf\trelative_gain\n")
        for k in k_values:
            pop_rate = pop_hits[k] / total_eval_users
            cf_rate = cf_hits[k] / total_eval_users
            gain = ((cf_rate - pop_rate) / pop_rate * 100) if pop_rate > 0 else 0
            print(f"Hit Rate @ {k:<3} {pop_rate:>18.2%} {cf_rate:>20.2%} {gain:>14.1f}%")
            f.write(f"Hit Rate @ {k}\t{pop_rate:.5f}\t{cf_rate:.5f}\t{gain:.2f}%\n")

    print("\n" + "=" * 70)
    print("SAMPLE RECOMMENDATION RESULTS (Item-CF)")
    print("=" * 70)
    for sample in sample_recommendations:
        print(f"User: {sample['user']}")
        print(f"  Starred History: {', '.join(sample['history'])}")
        print(f"  Target Star:     {sample['target']}")
        print(f"  Top-5 Item-CF:   {', '.join(sample['top_cf_recs'])}")
        print("-" * 70)


def main():
    EXPORT_DIR.mkdir(exist_ok=True)
    user_stars, repo_stars = load_star_interactions()
    train_stars, test_stars = train_test_split(user_stars, min_stars=2)
    similarities, repo_starrers = build_item_similarity_matrix(train_stars)
    evaluate_models(train_stars, test_stars, similarities, repo_starrers)


if __name__ == "__main__":
    main()