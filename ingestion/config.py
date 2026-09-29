"""Settings, read from the environment with the repo's .env as fallback."""

import os
from pathlib import Path
from urllib.parse import quote_plus

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv() -> None:
    """Load KEY=VALUE lines from .env without overriding variables already set."""
    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_dotenv()

# GH Archive publishes one file per UTC hour, e.g. 2026-09-29-12.json.gz
GH_ARCHIVE_URL = "https://data.gharchive.org/{key}.json.gz"

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9100")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "gitlytics")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "gh-raw")

# Built from the same user/password the container is created with, so the password lives in one place
MONGO_URI = "mongodb://{user}:{password}@{host}:{port}/?authSource=admin".format(
    user=quote_plus(os.environ.get("MONGO_ROOT_USER", "gitlytics")),
    password=quote_plus(os.environ.get("MONGO_ROOT_PASSWORD", "")),
    host=os.environ.get("MONGO_HOST", "localhost"),
    port=os.environ.get("MONGO_PORT", "27018"),
)
MONGO_DB = "gitlytics"

HDFS_ROOT = os.environ.get("HDFS_ROOT", "/gitlytics")
HADOOP_HOME = os.environ.get("HADOOP_HOME", str(Path.home() / "bigdata" / "hadoop"))
HDFS_BIN = str(Path(HADOOP_HOME) / "bin" / "hdfs")

TMP_DIR = REPO_ROOT / "tmp"

# Validation thresholds
MIN_LINES = 1_000           # a real hour has ~100k events; fewer means a broken file
MAX_BAD_LINE_RATIO = 0.001  # tolerate at most 0.1% unparseable lines
