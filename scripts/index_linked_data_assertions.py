#!/usr/bin/env python3
"""Backfill URI lookup identities using read-only Neo4j queries, in bounded batches."""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from CM.assertion_index import backfill_complete, remember_assertions
from CM.bulk_rdf import _batched, _stream_query
from CM.utils import getDriver

QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
RETURN d.CMID AS datasetCmid, c.CMID AS conceptCmid, r.Key AS key
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", choices=("sociomap", "archamap", "all"))
    parser.add_argument("--if-missing", action="store_true", help="Skip databases with a completed backfill")
    args = parser.parse_args()
    for database in (("sociomap", "archamap") if args.database == "all" else (args.database,)):
        if args.if_missing and backfill_complete(database):
            print(json.dumps({"database": database, "backfill": "already complete"}), flush=True)
            continue
        count = 0
        for batch in _batched(_stream_query(getDriver(database), QUERY), size=25000):
            remember_assertions(database, batch)
            count += len(batch)
            if count % 100000 == 0:
                print(json.dumps({"database": database, "relationshipsProcessed": count}), flush=True)
        backfill_complete(database, mark=True)
        print(json.dumps({"database": database, "relationshipsIndexed": count}), flush=True)


if __name__ == "__main__":
    main()
