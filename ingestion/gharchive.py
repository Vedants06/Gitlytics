"""Locate, download and validate GH Archive hourly files."""

import gzip
import json
import logging
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from ingestion.config import GH_ARCHIVE_URL, MAX_BAD_LINE_RATIO, MIN_LINES

log = logging.getLogger(__name__)


def hour_key(hour: datetime) -> str:
    """2026-09-29 12:00 UTC -> '2026-09-29-12' (GH Archive naming, no zero-padded hour)."""
    return f"{hour:%Y-%m-%d}-{hour.hour}"


def parse_hour(text: str) -> datetime:
    """Accept '2026-09-29-12', '2026-09-29T12' or '2026-09-29T12:00:00'; return an aware UTC hour."""
    text = text.strip()
    if "T" in text:
        dt = datetime.fromisoformat(text if ":" in text else f"{text}:00")
    else:
        date_part, hour_part = text.rsplit("-", 1)
        dt = datetime.fromisoformat(date_part).replace(hour=int(hour_part))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def hour_range(start: datetime, end: datetime) -> list[datetime]:
    """Every hour from start to end, inclusive."""
    hours, current = [], start
    while current <= end:
        hours.append(current)
        current += timedelta(hours=1)
    return hours


def file_url(hour: datetime) -> str:
    return GH_ARCHIVE_URL.format(key=hour_key(hour))


def is_published(hour: datetime) -> bool:
    """True once GH Archive has published the file (about 5 minutes after the hour ends)."""
    response = requests.head(file_url(hour), timeout=30, allow_redirects=True)
    return response.status_code == 200


def download(hour: datetime, dest_dir: Path) -> Path:
    """Stream the hourly file to dest_dir. Writes to .part first so a crash never leaves a half file."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / f"{hour_key(hour)}.json.gz"
    partial = target.with_suffix(".gz.part")
    with requests.get(file_url(hour), stream=True, timeout=(10, 120)) as response:
        response.raise_for_status()
        with open(partial, "wb") as out:
            for chunk in response.iter_content(chunk_size=1 << 20):
                out.write(chunk)
    partial.replace(target)
    log.info("Downloaded %s (%.1f MB)", target.name, target.stat().st_size / 1e6)
    return target


def validate(path: Path) -> dict:
    """Check the file is complete gzip + JSON lines, and profile it.

    Returns line counts, event-type mix and the payload era:
      v2024 = push events still carry commit messages
      v2026 = push events without commits (GitHub trimmed payloads in 2025)
    """
    lines = bad = pushes = pushes_with_commits = 0
    types: Counter = Counter()
    first_ts = last_ts = None

    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            lines += 1
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                bad += 1
                continue
            event_type = event.get("type", "Unknown")
            types[event_type] += 1
            if event_type == "PushEvent":
                pushes += 1
                if event.get("payload", {}).get("commits"):
                    pushes_with_commits += 1
            created = event.get("created_at")
            if created:
                first_ts = created if first_ts is None or created < first_ts else first_ts
                last_ts = created if last_ts is None or created > last_ts else last_ts

    if lines < MIN_LINES:
        raise ValueError(f"{path.name}: only {lines} lines (expected at least {MIN_LINES})")
    if bad / lines > MAX_BAD_LINE_RATIO:
        raise ValueError(f"{path.name}: {bad} of {lines} lines are not valid JSON")

    if pushes_with_commits:
        era = "v2024"
    elif pushes:
        era = "v2026"
    else:
        era = "unknown"

    return {
        "lines": lines,
        "bad_lines": bad,
        "size_bytes": path.stat().st_size,
        "event_types": dict(types.most_common()),
        "pushes_with_commits": pushes_with_commits,
        "era": era,
        "first_event_at": first_ts,
        "last_event_at": last_ts,
    }
