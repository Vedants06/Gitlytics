"""Ingest one GH Archive hour end to end. Safe to re-run: every step overwrites."""

import logging
import time
from datetime import datetime, timezone

from ingestion import ingest_log
from ingestion.config import TMP_DIR
from ingestion.gharchive import download, hour_key, validate
from ingestion.storage import put_to_hdfs, upload_to_minio

log = logging.getLogger(__name__)


def ingest_hour(hour: datetime, keep_local: bool = False) -> dict:
    """Download -> validate -> MinIO -> HDFS -> ingest_log. Returns the log entry."""
    key = hour_key(hour)
    started = time.monotonic()
    path = None
    try:
        path = download(hour, TMP_DIR)
        stats = validate(path)
        object_name = upload_to_minio(path, hour)
        hdfs_path = put_to_hdfs(path, hour)
        entry = {
            "hour": key,
            "status": "ok",
            "minio_object": object_name,
            "hdfs_path": hdfs_path,
            "duration_s": round(time.monotonic() - started, 1),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            **stats,
        }
        ingest_log.record(entry)
        log.info("Ingested %s: %s events, era %s", key, stats["lines"], stats["era"])
        return entry
    except Exception as error:
        try:
            ingest_log.record({
                "hour": key,
                "status": "failed",
                "error": str(error)[:500],
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception:
            log.exception("Could not write the failure to ingest_log")
        raise
    finally:
        if path is not None and not keep_local:
            path.unlink(missing_ok=True)
