"""Write raw hourly files to MinIO (landing zone) and HDFS (bronze lake)."""

import logging
import subprocess
from datetime import datetime
from pathlib import Path

from minio import Minio

from ingestion.config import (
    HDFS_BIN,
    HDFS_ROOT,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
)

log = logging.getLogger(__name__)


def minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def upload_to_minio(path: Path, hour: datetime) -> str:
    """Upload to gh-raw/YYYY/MM/DD/<file>. Re-uploading the same hour overwrites it."""
    client = minio_client()
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
    object_name = f"{hour:%Y/%m/%d}/{path.name}"
    client.fput_object(MINIO_BUCKET, object_name, str(path), content_type="application/gzip")
    log.info("Uploaded s3://%s/%s", MINIO_BUCKET, object_name)
    return object_name


def bronze_partition(hour: datetime) -> str:
    """/gitlytics/bronze/dt=2026-09-29/hr=12"""
    return f"{HDFS_ROOT}/bronze/dt={hour:%Y-%m-%d}/hr={hour.hour}"


def _hdfs(*args: str) -> None:
    result = subprocess.run([HDFS_BIN, "dfs", *args], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"hdfs dfs {' '.join(args)} failed: {result.stderr.strip()[-500:]}")


def put_to_hdfs(path: Path, hour: datetime) -> str:
    """Copy the file into its bronze partition, replacing any earlier copy of that hour."""
    partition = bronze_partition(hour)
    _hdfs("-mkdir", "-p", partition)
    _hdfs("-put", "-f", str(path), f"{partition}/")
    hdfs_path = f"{partition}/{path.name}"
    log.info("Put %s", hdfs_path)
    return hdfs_path
