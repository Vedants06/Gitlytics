#!/usr/bin/env python3
"""
Load one complete day of raw GitHub Archive events into MongoDB (Experiment 3).
Streams directly from MinIO (or HDFS fallback) to avoid writing multi-GB temp files.

Usage:
    python mongodb/load_day.py --date 2024-09-28
    python mongodb/load_day.py --date 2024-09-28 --source hdfs
    python mongodb/load_day.py --date 2024-09-28 --drop
"""

import argparse
import gzip
import io
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Generator, Iterable

# Add repo root to sys.path so config can be imported directly
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from minio import Minio
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from ingestion.config import (
    HDFS_BIN,
    HDFS_ROOT,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MONGO_DB,
    MONGO_URI,
)

BATCH_SIZE = 5_000


def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def get_mongo_db():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    # Ping to confirm connection
    client.admin.command("ping")
    return client[MONGO_DB]


def stream_minio_hour(minio_client: Minio, object_name: str) -> Generator[dict, None, None]:
    """Stream and decompress a .json.gz object from MinIO line by line."""
    response = None
    try:
        response = minio_client.get_object(MINIO_BUCKET, object_name)
        with gzip.GzipFile(fileobj=io.BytesIO(response.read())) as gz:
            for line in gz:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line.decode("utf-8"))
                    except Exception:
                        continue
    finally:
        if response:
            response.close()
            response.release_conn()


def stream_hdfs_hour(dt: str, hr: int) -> Generator[dict, None, None]:
    """Stream and decompress an hourly .json.gz file from HDFS via hdfs dfs -cat."""
    hdfs_path = f"{HDFS_ROOT}/bronze/dt={dt}/hr={hr}/*.json.gz"
    proc = subprocess.Popen(
        [HDFS_BIN, "dfs", "-cat", hdfs_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    with gzip.GzipFile(fileobj=proc.stdout) as gz:
        for line in gz:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line.decode("utf-8"))
                except Exception:
                    continue
    proc.wait()


def batch_stream(iterable: Iterable, batch_size: int) -> Generator[list, None, None]:
    """Yield lists of items of length batch_size."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def load_day(dt: str, source: str = "minio", drop_existing: bool = False):
    print(f"[{time.strftime('%X')}] Connecting to MongoDB ({MONGO_DB})...")
    db = get_mongo_db()
    events_col = db["events"]

    if drop_existing:
        print(f"[{time.strftime('%X')}] Dropping existing 'events' collection...")
        events_col.drop()

    year, month, day = dt.split("-")
    minio_client = get_minio_client()

    total_inserted = 0
    start_time = time.time()

    print(f"[{time.strftime('%X')}] Starting load for date: {dt} (Source: {source.upper()})")

    for hr in range(24):
        hr_start = time.time()
        hour_events = 0

        if source == "minio":
            # Check possible object naming conventions in MinIO
            candidates = [
                f"{year}/{month}/{day}/{dt}-{hr}.json.gz",
                f"{dt}/{hr}/{dt}-{hr}.json.gz",
                f"{dt}-{hr}.json.gz",
            ]
            obj_name = None
            for c in candidates:
                try:
                    minio_client.stat_object(MINIO_BUCKET, c)
                    obj_name = c
                    break
                except Exception:
                    continue

            if not obj_name:
                # Fallback search by prefix
                prefix = f"{year}/{month}/{day}/"
                objs = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))
                match = [o.object_name for o in objs if f"-{hr}.json.gz" in o.object_name]
                if match:
                    obj_name = match[0]

            if not obj_name:
                print(f"  hr={hr:02d}: Object not found in MinIO bucket '{MINIO_BUCKET}', skipping.")
                continue

            event_generator = stream_minio_hour(minio_client, obj_name)
        else:
            event_generator = stream_hdfs_hour(dt, hr)

        for batch in batch_stream(event_generator, BATCH_SIZE):
            try:
                result = events_col.insert_many(batch, ordered=False)
                count = len(result.inserted_ids)
                hour_events += count
                total_inserted += count
            except BulkWriteError as bwe:
                count = bwe.details.get("nInserted", 0)
                hour_events += count
                total_inserted += count

        elapsed_hr = time.time() - hr_start
        rate = hour_events / elapsed_hr if elapsed_hr > 0 else 0
        print(
            f"  hr={hr:02d}: Inserted {hour_events:>7,d} events in {elapsed_hr:>5.1f}s "
            f"({rate:>6.0f} doc/s) | Day Total: {total_inserted:>9,d}"
        )

    total_elapsed = time.time() - start_time
    avg_rate = total_inserted / total_elapsed if total_elapsed > 0 else 0

    print(f"\n{'='*60}")
    print(f"Load complete for {dt}!")
    print(f"Total documents inserted: {total_inserted:,}")
    print(f"Total time:              {total_elapsed:.1f}s ({total_elapsed/60:.2f} min)")
    print(f"Average throughput:      {avg_rate:.0f} doc/s")
    print(f"Collection stats:        {db.command('collstats', 'events').get('size', 0) / (1024**2):.1f} MB on disk")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load one day of GitHub events into MongoDB")
    parser.add_argument("--date", default="2024-09-28", help="Date in YYYY-MM-DD format (default: 2024-09-28)")
    parser.add_argument("--source", choices=["minio", "hdfs"], default="minio", help="Source storage (default: minio)")
    parser.add_argument("--drop", action="store_true", help="Drop events collection before loading")
    args = parser.parse_args()

    load_day(dt=args.date, source=args.source, drop_existing=args.drop)