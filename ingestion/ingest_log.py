"""One MongoDB document per ingested hour, used to spot gaps and report success rate."""

from pymongo import ASCENDING, MongoClient

from ingestion.config import MONGO_DB, MONGO_URI


def _collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    collection = client[MONGO_DB]["ingest_log"]
    collection.create_index([("hour", ASCENDING)], unique=True)
    return collection


def record(entry: dict) -> None:
    """Insert or replace the log entry for entry['hour']."""
    _collection().update_one({"hour": entry["hour"]}, {"$set": entry}, upsert=True)
