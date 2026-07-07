#!/usr/bin/env python
"""Backfill owner metadata from unambiguous CatMapper LOG history.

Dry-run is the default. Pass --write to update graph objects.
"""

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from CM.utils import getDriver, getQuery  # noqa: E402


DATABASES = ("sociomap", "archamap")


def _node_query(write):
    set_clause = """
    SET
      n.createdByUserId = createdByUserId,
      n.ownerUserId = ownerUserId,
      n.createdAt = createdAt,
      n.contributionId = contributionId
    """ if write else ""
    return f"""
    MATCH (n)-[:HAS_LOG]->(l:LOG)
    WHERE (n.createdByUserId IS NULL OR n.ownerUserId IS NULL)
      AND toLower(coalesce(l.action, '')) STARTS WITH 'created node'
      AND coalesce(toString(l.user), '') <> ''
    WITH n, collect(DISTINCT toString(l.user)) AS users, min(l.timestamp) AS createdAt
    WHERE size(users) = 1
    WITH
      n,
      users[0] AS createdByUserId,
      users[0] AS ownerUserId,
      coalesce(createdAt, toString(datetime())) AS createdAt,
      coalesce(n.contributionId, 'backfill_' + users[0]) AS contributionId
    {set_clause}
    RETURN count(n) AS count
    """


def _uses_query(write):
    set_clause = """
    SET
      r.createdByUserId = createdByUserId,
      r.ownerUserId = ownerUserId,
      r.createdAt = createdAt,
      r.contributionId = contributionId
    """ if write else ""
    return f"""
    MATCH ()-[r:USES]->()
    WHERE (r.createdByUserId IS NULL OR r.ownerUserId IS NULL)
      AND r.logID IS NOT NULL
    WITH r, [id IN apoc.coll.flatten([r.logID], true) | toString(id)] AS logIds
    MATCH (l:LOG)
    WHERE elementId(l) IN logIds
      AND toLower(coalesce(l.action, '')) STARTS WITH 'created relationship'
      AND coalesce(toString(l.user), '') <> ''
    WITH r, collect(DISTINCT toString(l.user)) AS users, min(l.timestamp) AS createdAt
    WHERE size(users) = 1
    WITH
      r,
      users[0] AS createdByUserId,
      users[0] AS ownerUserId,
      coalesce(createdAt, toString(datetime())) AS createdAt,
      coalesce(r.contributionId, 'backfill_' + users[0]) AS contributionId
    {set_clause}
    RETURN count(r) AS count
    """


def _count(result):
    if not result:
        return 0
    row = result[0]
    if isinstance(row, dict):
        return int(row.get("count") or 0)
    return int(row or 0)


def backfill_database(database, write=False):
    driver = getDriver(database)
    node_count = _count(getQuery(_node_query(write), driver=driver, type="dict"))
    uses_count = _count(getQuery(_uses_query(write), driver=driver, type="dict"))
    return {"database": database, "nodes": node_count, "uses": uses_count, "write": write}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", choices=DATABASES, action="append")
    parser.add_argument("--write", action="store_true", help="Apply updates instead of dry-running.")
    args = parser.parse_args()

    databases = args.database or list(DATABASES)
    for database in databases:
        result = backfill_database(database, write=args.write)
        mode = "updated" if args.write else "would update"
        print(f"{result['database']}: {mode} {result['nodes']} node(s), {result['uses']} USES tie(s)")


if __name__ == "__main__":
    main()
