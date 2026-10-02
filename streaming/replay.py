#!/usr/bin/env python3
"""
Simulate a live stream by chronologically replaying MongoDB events.
"""

import sys
from pathlib import Path
from typing import Generator

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pymongo import MongoClient

from ingestion.config import MONGO_DB, MONGO_URI


def stream_events(batch_size: int = 20_000, limit: int = None) -> Generator[dict, None, None]:
    """Yields events chronologically from MongoDB."""
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events_col = db["events"]

    cursor = (
        events_col.find({}, {"_id": 0, "id": 1, "type": 1, "actor.login": 1, "repo.name": 1, "created_at": 1})
        .sort("created_at", 1)
        .batch_size(batch_size)
    )

    count = 0
    for doc in cursor:
        yield doc
        count += 1
        if limit and count >= limit:
            break