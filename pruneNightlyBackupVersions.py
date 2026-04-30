#!/usr/bin/env python3
"""Prune old noncurrent S3 versions for CatMapper nightly Neo4j backups."""

from __future__ import annotations

import argparse
from configparser import ConfigParser
from datetime import datetime, timedelta, timezone

import boto3


BUCKET = "catmapper"
DATABASES = ("archamap1", "gisdb", "sociomap1", "userdb")
SOURCE_NAME = "neo4j-backup.tar.zst"
DEFAULT_RETENTION_DAYS = 60


def aws_client():
    parser = ConfigParser()
    parser.read(["config.ini", "CatMapperAPI/config.ini", "/app/config.ini"])
    return boto3.client(
        "s3",
        aws_access_key_id=parser.get("AWS", "AccessKeyId"),
        aws_secret_access_key=parser.get("AWS", "SecretAccessKey"),
    )


def source_key(database: str) -> str:
    return f"backups/{database}/{SOURCE_NAME}"


def flush_deletes(s3, objects: list[dict], dry_run: bool) -> int:
    if not objects:
        return 0
    if not dry_run:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objects, "Quiet": True})
    return len(objects)


def prune_database(s3, database: str, cutoff: datetime, dry_run: bool) -> tuple[int, int]:
    key = source_key(database)
    scanned = 0
    deleted = 0
    batch: list[dict] = []
    paginator = s3.get_paginator("list_object_versions")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=key):
        for version in page.get("Versions", []):
            if version["Key"] != key:
                continue
            scanned += 1
            if version.get("IsLatest"):
                continue
            if version["LastModified"] >= cutoff:
                continue

            batch.append({"Key": key, "VersionId": version["VersionId"]})
            if len(batch) == 1000:
                deleted += flush_deletes(s3, batch, dry_run)
                batch = []

    deleted += flush_deletes(s3, batch, dry_run)
    return scanned, deleted


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Delete noncurrent versions of nightly Neo4j backup keys older than "
            "the retention window. Monthly archive keys are not touched."
        )
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=DEFAULT_RETENTION_DAYS,
        help=f"Keep noncurrent nightly versions from the last N days. Default: {DEFAULT_RETENTION_DAYS}.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without deleting anything.",
    )
    args = parser.parse_args()

    if args.retention_days < 1:
        raise SystemExit("--retention-days must be at least 1")

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.retention_days)
    s3 = aws_client()
    mode = "would delete" if args.dry_run else "deleted"

    print(
        f"Pruning noncurrent nightly backup versions older than {cutoff.isoformat()} "
        f"from s3://{BUCKET}/backups/*/{SOURCE_NAME}"
    )
    for database in DATABASES:
        scanned, deleted = prune_database(s3, database, cutoff, args.dry_run)
        print(f"{database}: scanned {scanned} versions, {mode} {deleted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
