#!/usr/bin/env python3
"""
Main driver for Experiment 6 Stream Algorithms.
Replays MongoDB events, collects metrics, and exports comparison reports.
"""

import json
import math
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from streaming.bloom_filter import BloomFilter, run_bloom_experiment
from streaming.dgim import DGIM
from streaming.flajolet_martin import FlajoletMartin
from streaming.replay import stream_events

KNOWN_BOTS_FILE = REPO_ROOT / "config" / "known_bots.txt"
EXPORT_DIR = REPO_ROOT / "exports"


def run_streaming_pipeline(max_events: int = 1_000_000):
    print("=" * 70)
    print("  EXPERIMENT 6: STREAM ALGORITHMS PIPELINE")
    print("=" * 70)

    EXPORT_DIR.mkdir(exist_ok=True)

    # 1. RUN BLOOM FILTER BENCHMARK EXPERIMENT
    print("\n[*] Step 1: Running Bloom Filter False-Positive Benchmark...")
    bf_results = run_bloom_experiment(KNOWN_BOTS_FILE)

    # Save Bloom benchmark metrics
    bloom_csv = EXPORT_DIR / "stream_bloom_results.tsv"
    with open(bloom_csv, "w") as f:
        f.write("target_fpr\tbit_array_m\thash_k\tactual_fpr\ttheoretical_fpr\n")
        for r in bf_results:
            f.write(f"{r['target_fpr']}\t{r['m']}\t{r['k']}\t{r['actual_fpr']:.5f}\t{r['theoretical_fpr']:.5f}\n")
    print(f"    - Bloom Filter stats saved to: {bloom_csv}")

    # Initialize a production Bloom Filter with optimal FPR (1%) to filter stream
    with open(KNOWN_BOTS_FILE, "r") as f:
        bots_list = [line.strip() for line in f if line.strip()]
    prod_bf = BloomFilter(len(bots_list), 0.01)
    for b in bots_list:
        prod_bf.add(b)

    # 2. INITIALIZE ALGORITHMS FOR THE STREAM
    print(f"\n[*] Step 2: Processing stream up to {max_events:,} events...")
    fm = FlajoletMartin(num_hashes=64)
    dgim_push = DGIM(N=10_000)  # window of 10k events for pushes

    # Exact Ground-Truth counters
    exact_distinct_actors = set()
    exact_pushes_in_window = []  # sliding queue of 1s and 0s
    total_push_count_exact = 0

    # User Sampling (10%)
    sampled_users = set()
    all_users = set()

    # Metrics collections over stream intervals (every 100k events)
    interval_size = 100_000
    fm_reports = []
    dgim_reports = []

    t0 = time.time()
    processed_count = 0

    for event in stream_events(limit=max_events):
        processed_count += 1
        actor = event["actor"]["login"]
        e_type = event["type"]

        # A. Sampling (Hash to select 10% of users)
        # Using hash % 10 == 0 ensures all events for selected users are captured
        user_hash = abs(hash(actor)) % 10
        all_users.add(actor)
        if user_hash == 0:
            sampled_users.add(actor)

        # B. Flajolet-Martin Distinct Estimation
        fm.add(actor)
        exact_distinct_actors.add(actor)

        # C. DGIM Counting (for PushEvent bits in a 10K sliding window)
        is_push = 1 if e_type == "PushEvent" else 0
        dgim_push.update(is_push)

        # Track exact queue in window
        exact_pushes_in_window.append(is_push)
        if len(exact_pushes_in_window) > 10_000:
            exact_pushes_in_window.pop(0)

        # D. Hourly/Interval Checkpoint Prints
        if processed_count % interval_size == 0:
            # FM estimation check
            fm_est = fm.estimate()
            exact_count = len(exact_distinct_actors)
            fm_err = abs(fm_est - exact_count) / exact_count if exact_count > 0 else 0
            fm_reports.append((processed_count, fm_est, exact_count, fm_err))

            # DGIM window check
            dgim_est = dgim_push.count()
            dgim_exact = sum(exact_pushes_in_window)
            dgim_err = abs(dgim_est - dgim_exact) / dgim_exact if dgim_exact > 0 else 0
            dgim_reports.append((processed_count, dgim_est, dgim_exact, dgim_err))

            print(
                f"  Progress: {processed_count:>9,d} events | "
                f"FM Err: {fm_err:>5.1%} (Est: {fm_est:>7,.0f} vs {exact_count:>7,d}) | "
                f"DGIM Err: {dgim_err:>5.1%} (Est: {dgim_est:>4,d} vs {dgim_exact:>4,d})"
            )

    elapsed = time.time() - t0
    print(f"\n[*] Stream processed in {elapsed:.2f} seconds ({processed_count/elapsed:.0f} events/s)")

    # 3. EXPORT METRICS FOR REPORTS & CHARTS
    # Export FM report
    fm_tsv = EXPORT_DIR / "stream_fm_comparison.tsv"
    with open(fm_tsv, "w") as f:
        f.write("events_processed\tfm_estimate\texact_distinct\terror_pct\n")
        for row in fm_reports:
            f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]:.4f}\n")

    # Export DGIM report
    dgim_tsv = EXPORT_DIR / "stream_dgim_comparison.tsv"
    with open(dgim_tsv, "w") as f:
        f.write("events_processed\tdgim_estimate\texact_window\terror_pct\n")
        for row in dgim_reports:
            f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]:.4f}\n")

    # 4. SAMPLING REPORT COMPARISON
    sample_rate = len(sampled_users) / len(all_users) if all_users else 0
    print(f"\n[*] Step 3: User Sampling Stats:")
    print(f"    - Total unique users in stream:  {len(all_users):,}")
    print(f"    - Sampled unique users (10% target): {len(sampled_users):,}")
    print(f"    - Measured Sampling Rate:        {sample_rate:.3%} (Error: {abs(sample_rate - 0.10):.3%})")

    sampling_tsv = EXPORT_DIR / "stream_sampling_results.tsv"
    with open(sampling_tsv, "w") as f:
        f.write("metric\tvalue\n")
        f.write(f"total_unique_users\t{len(all_users)}\n")
        f.write(f"sampled_unique_users\t{len(sampled_users)}\n")
        f.write(f"actual_sampling_rate\t{sample_rate:.5f}\n")

    print(f"\nAll streaming data benchmarks saved to: {EXPORT_DIR}/\n")
    print("=" * 70)


if __name__ == "__main__":
    # We will process 1.5M events by default for high-quality statistics in under 20s
    run_streaming_pipeline(max_events=1_500_000)