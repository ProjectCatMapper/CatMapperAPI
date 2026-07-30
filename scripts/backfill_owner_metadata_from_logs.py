#!/usr/bin/env python
"""Audit or reconcile simplified owner-edit metadata.

Dry-run is the default. Pass ``--write`` to create the internal PROPERTY
definitions, backfill ``ownerUserId`` from the legacy creator field, derive the
monotonic human-modification lock, and remove deprecated ownership properties.
"""

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM.ownership import reconcile_owner_edit_metadata  # noqa: E402
from CM.utils import getDriver, getQuery  # noqa: E402


DATABASES = ("sociomap", "archamap")


def audit_database(database):
    driver = getDriver(database)
    rows = getQuery(
        query="""
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET)
          AND NOT n:DELETED
          AND (n.ownerUserId IS NOT NULL OR n.createdByUserId IS NOT NULL)
        WITH count(n) AS candidateNodes,
             count(CASE
               WHEN n.ownerUserId IS NULL THEN 1
             END) AS nodesMissingOwner,
             count(CASE
               WHEN n.modifiedByOtherUser IS NULL THEN 1
             END) AS nodesMissingLock,
             count(CASE
               WHEN n.createdByUserId IS NOT NULL
                 OR n.createdAt IS NOT NULL
                 OR n.contributionId IS NOT NULL
               THEN 1
             END) AS nodesWithLegacy
        MATCH ()-[r:USES]->()
        WHERE r.ownerUserId IS NOT NULL OR r.createdByUserId IS NOT NULL
        RETURN candidateNodes,
               nodesMissingOwner,
               nodesMissingLock,
               nodesWithLegacy,
               count(r) AS candidateUses,
               count(CASE
                 WHEN r.ownerUserId IS NULL THEN 1
               END) AS usesMissingOwner,
               count(CASE
                 WHEN r.modifiedByOtherUser IS NULL THEN 1
               END) AS usesMissingLock,
               count(CASE
                 WHEN r.createdByUserId IS NOT NULL
                   OR r.createdAt IS NOT NULL
                   OR r.contributionId IS NOT NULL
                 THEN 1
               END) AS usesWithLegacy
        """,
        driver=driver,
        type="dict",
    )
    return (rows or [{}])[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", choices=DATABASES, action="append")
    parser.add_argument(
        "--write",
        action="store_true",
        help="Apply reconciliation instead of reporting candidate counts.",
    )
    args = parser.parse_args()

    databases = args.database or list(DATABASES)
    for database in databases:
        if args.write:
            result = reconcile_owner_edit_metadata(database, return_type="data")
            print(f"{database}: reconciled {result}")
        else:
            print(f"{database}: dry-run {audit_database(database)}")


if __name__ == "__main__":
    main()
