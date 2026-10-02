#!/usr/bin/env python3
"""
Generate config/known_bots.txt by pulling real bot accounts from MongoDB events.
"""

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pymongo import MongoClient

from ingestion.config import MONGO_DB, MONGO_URI


def seed_bots():
    print("Connecting to MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events = db["events"]

    print("Querying for unique logins ending in [bot]...")
    bot_logins = events.distinct(
        "actor.login", {"actor.login": {"$regex": r"\[bot\]$", "$options": "i"}}
    )

    print(f"Found {len(bot_logins):,} distinct bot logins.")

    config_dir = REPO_ROOT / "config"
    config_dir.mkdir(exist_ok=True)
    out_file = config_dir / "known_bots.txt"

    with open(out_file, "w", encoding="utf-8") as f:
        for login in sorted(bot_logins):
            f.write(login + "\n")

    print(f"Saved {len(bot_logins):,} bots to {out_file}")


if __name__ == "__main__":
    seed_bots()