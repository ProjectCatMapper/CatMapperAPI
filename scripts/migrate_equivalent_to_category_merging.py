#!/usr/bin/env python3
"""Backfill legacy EQUIVALENT ties into DATASET->CATEGORY MERGING ties."""

import argparse
import os
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM import getDriver, getQuery  # noqa: E402


def load_renviron(path):
    if not path.exists():
        return
    pattern = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$")
    for line in path.read_text().splitlines():
        match = pattern.match(line)
        if not match:
            continue
        key, value = match.groups()
        os.environ.setdefault(key, value.strip().strip('"').strip("'"))


def scalar(rows, key):
    if not rows:
        return 0
    return rows[0].get(key, 0) or 0


def migrate(database, dry_run=False):
    driver = getDriver(database)

    stats_query = """
    MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)
    OPTIONAL MATCH (d:DATASET {CMID: e.dataset})
    RETURN
      count(e) AS oldTieCount,
      count(CASE WHEN d IS NOT NULL AND e.Key IS NOT NULL AND e.stack IS NOT NULL THEN 1 END) AS migratableCount,
      count(CASE WHEN d IS NULL OR e.Key IS NULL OR e.stack IS NULL THEN 1 END) AS skippedCount
    """
    stats = getQuery(stats_query, driver) or []

    if dry_run:
        return {
            "database": database,
            "dryRun": True,
            "oldTieCount": scalar(stats, "oldTieCount"),
            "migratableCount": scalar(stats, "migratableCount"),
            "skippedCount": scalar(stats, "skippedCount"),
            "createdOrMatchedCount": 0,
            "deletedCount": 0,
        }

    migration_query = """
    MATCH (:CATEGORY)-[e:EQUIVALENT]->(to:CATEGORY)
    OPTIONAL MATCH (d:DATASET {CMID: e.dataset})
    WITH e, to, d
    WHERE d IS NOT NULL AND e.Key IS NOT NULL AND e.stack IS NOT NULL
    MERGE (d)-[m:MERGING {Key: e.Key, stack: e.stack}]->(to)
    WITH collect(e) AS migrated, count(m) AS createdOrMatchedCount
    FOREACH (rel IN migrated | DELETE rel)
    RETURN createdOrMatchedCount, size(migrated) AS deletedCount
    """
    migrated = getQuery(migration_query, driver) or []

    return {
        "database": database,
        "dryRun": False,
        "oldTieCount": scalar(stats, "oldTieCount"),
        "migratableCount": scalar(stats, "migratableCount"),
        "skippedCount": scalar(stats, "skippedCount"),
        "createdOrMatchedCount": scalar(migrated, "createdOrMatchedCount"),
        "deletedCount": scalar(migrated, "deletedCount"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("database", help="Neo4j database name, such as SocioMap or ArchaMap")
    parser.add_argument("--dry-run", action="store_true", help="Report counts without writing")
    args = parser.parse_args()

    load_renviron(Path.home() / ".Renviron")
    result = migrate(args.database, dry_run=args.dry_run)
    for key, value in result.items():
        print(f"{key}: {value}")


if __name__ == "__main__":
    main()
