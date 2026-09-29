"""Ingest hours from the command line, without Airflow (for testing).

    python -m ingestion.cli 2026-09-29-12
    python -m ingestion.cli --from 2024-09-28-0 --to 2024-09-28-2
"""

import argparse
import logging

from ingestion.gharchive import hour_range, parse_hour
from ingestion.pipeline import ingest_hour


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest GH Archive hours into MinIO and HDFS")
    parser.add_argument("hours", nargs="*", help="hours like 2026-09-29-12 (UTC)")
    parser.add_argument("--from", dest="start", help="first hour of a range")
    parser.add_argument("--to", dest="end", help="last hour of a range (inclusive)")
    args = parser.parse_args()

    hours = [parse_hour(h) for h in args.hours]
    if args.start and args.end:
        hours += hour_range(parse_hour(args.start), parse_hour(args.end))
    if not hours:
        parser.error("give at least one hour, or --from and --to")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    for hour in hours:
        entry = ingest_hour(hour)
        print(f"{entry['hour']}: {entry['lines']:,} events, era {entry['era']}, "
              f"{entry['size_bytes'] / 1e6:.1f} MB, {entry['duration_s']} s")


if __name__ == "__main__":
    main()
