#!/usr/bin/env python3
"""Create monthly S3 archive copies from versioned CatMapper Neo4j backups."""

from __future__ import annotations

import argparse
from collections import defaultdict
from configparser import ConfigParser
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


BUCKET = "catmapper"
DATABASES = ("archamap1", "gisdb", "sociomap1", "userdb")
SOURCE_NAME = "neo4j-backup.tar.zst"


def aws_client():
    parser = ConfigParser()
    parser.read(["config.ini", "CatMapperAPI/config.ini", "/app/config.ini"])
    return boto3.client(
        "s3",
        aws_access_key_id=parser.get("AWS", "AccessKeyId"),
        aws_secret_access_key=parser.get("AWS", "SecretAccessKey"),
    )


def parse_month(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m").strftime("%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("month must use YYYY-MM format") from exc


def version_month(version: dict) -> str:
    last_modified = version["LastModified"]
    if last_modified.tzinfo is None:
        last_modified = last_modified.replace(tzinfo=timezone.utc)
    return last_modified.strftime("%Y-%m")


def latest_versions_by_month(s3, database: str) -> dict[str, dict]:
    key = f"backups/{database}/{SOURCE_NAME}"
    versions_by_month: dict[str, list[dict]] = defaultdict(list)
    paginator = s3.get_paginator("list_object_versions")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=key):
        for version in page.get("Versions", []):
            if version["Key"] == key:
                versions_by_month[version_month(version)].append(version)

    return {
        month: max(versions, key=lambda item: item["LastModified"])
        for month, versions in versions_by_month.items()
    }


def object_exists(s3, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
            return False
        raise


def archive_month(s3, database: str, month: str, version: dict, force: bool) -> str:
    source_key = version["Key"]
    archive_key = f"backups/{database}/monthly/neo4j-backup-{month}.tar.zst"

    if not force and object_exists(s3, archive_key):
        return f"skip existing s3://{BUCKET}/{archive_key}"

    s3.copy_object(
        Bucket=BUCKET,
        Key=archive_key,
        CopySource={
            "Bucket": BUCKET,
            "Key": source_key,
            "VersionId": version["VersionId"],
        },
        MetadataDirective="COPY",
        TaggingDirective="REPLACE",
        Tagging=f"backup-retention=monthly&database={database}",
    )
    last_modified = version["LastModified"].strftime("%Y-%m-%dT%H:%M:%SZ")
    size_gib = version["Size"] / 1024**3
    return (
        f"archived {database} {month} from {last_modified} "
        f"({size_gib:.2f} GiB) to s3://{BUCKET}/{archive_key}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Create monthly archive copies from versioned Neo4j backups."
    )
    parser.add_argument(
        "--month",
        type=parse_month,
        help="Archive only one month in YYYY-MM format. Defaults to all months found.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing monthly archive keys.",
    )
    args = parser.parse_args()

    s3 = aws_client()
    for database in DATABASES:
        versions = latest_versions_by_month(s3, database)
        months = [args.month] if args.month else sorted(versions)
        for month in months:
            version = versions.get(month)
            if not version:
                print(f"skip {database} {month}: no source backup version")
                continue
            print(archive_month(s3, database, month, version, args.force))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
