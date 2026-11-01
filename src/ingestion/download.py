"""Hourly GH Archive ingestion: raw .json.gz -> Parquet bronze (idempotent)."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import requests

BASE_URL = "https://data.gharchive.org"
RAW_DIR = Path("data/raw")
BRONZE_DIR = Path("data/bronze/events")

log = logging.getLogger("ingestion")


def hour_slots(start: datetime, end: datetime):
    """Yield every hour in [start, end]."""
    slot = start
    while slot <= end:
        yield slot
        slot += timedelta(hours=1)


def bronze_path(slot: datetime) -> Path:
    return BRONZE_DIR / f"date={slot:%Y-%m-%d}" / f"hour={slot.hour:02d}.parquet"


def download(slot: datetime) -> Path:
    """Download one hourly archive. GH Archive hours are not zero-padded."""
    name = f"{slot:%Y-%m-%d}-{slot.hour}.json.gz"
    target = RAW_DIR / name
    if target.exists():
        return target

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".part")
    with requests.get(f"{BASE_URL}/{name}", stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with tmp.open("wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
    tmp.replace(target)
    return target


def to_parquet(raw_file: Path, slot: datetime) -> Path:
    """Convert to Parquet. Nested fields stay JSON: the bronze layer keeps the raw shape."""
    out = bronze_path(slot)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".tmp")

    duckdb.sql(
        f"""
        COPY (
            SELECT
                id,
                type,
                actor,
                repo,
                org,
                payload,
                public,
                created_at,
                '{raw_file.name}' AS _source_file,
                now() AS _ingested_at
            FROM read_json(
                '{raw_file}',
                format = 'newline_delimited',
                columns = {{
                    id: 'VARCHAR',
                    type: 'VARCHAR',
                    actor: 'JSON',
                    repo: 'JSON',
                    org: 'JSON',
                    payload: 'JSON',
                    public: 'BOOLEAN',
                    created_at: 'TIMESTAMP'
                }}
            )
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp.replace(out)
    return out


def ingest_hour(slot: datetime, keep_raw: bool = False) -> None:
    if bronze_path(slot).exists():
        log.info("skip %s (already ingested)", slot)
        return
    raw = download(slot)
    to_parquet(raw, slot)
    if not keep_raw:
        raw.unlink()
    log.info("ingested %s", slot)


def parse_hour(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest GH Archive hourly files.")
    parser.add_argument("start", type=parse_hour, help="e.g. 2024-01-15T00")
    parser.add_argument("end", type=parse_hour, help="e.g. 2024-01-15T23")
    parser.add_argument("--keep-raw", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    for slot in hour_slots(args.start, args.end):
        ingest_hour(slot, keep_raw=args.keep_raw)


if __name__ == "__main__":
    main()