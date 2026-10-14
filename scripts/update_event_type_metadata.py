#!/usr/bin/env python
"""Preflight or initialize eventType.allowedValues in the two category databases."""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from CM.utils import getDriver, getQuery

# Bootstrap only: runtime validation always reads PROPERTY.allowedValues.
INITIAL_ALLOWED_VALUES = ["SPLIT", "MERGED", "SPLITMERGE", "HIERARCHY", "FOLLOWS"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    targets = []
    for database in ("SocioMap", "ArchaMap"):
        driver = getDriver(database)
        rows = getQuery("""
            MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship'})
            RETURN elementId(p) AS id, p.CMID AS CMID, p.allowedValues AS allowedValues
        """, driver)
        if len(rows) != 1 or rows[0]["CMID"] != "CP117":
            raise RuntimeError(f"Unexpected eventType metadata in {database}: {rows}")
        existing = rows[0]["allowedValues"]
        if existing is not None and existing != INITIAL_ALLOWED_VALUES:
            raise RuntimeError(f"Refusing to overwrite configured allowedValues in {database}")
        targets.append((database, driver, rows[0]))
    for database, driver, row in targets:
        print(json.dumps({"database": database, "before": row,
                          "requestedAllowedValues": INITIAL_ALLOWED_VALUES}))
        if args.apply and row["allowedValues"] is None:
            timestamp = datetime.now(timezone.utc).isoformat()
            result = getQuery("""
                MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship', CMID: 'CP117'})
                WHERE elementId(p) = $id AND p.allowedValues IS NULL
                SET p.allowedValues = $values,
                    p.log = coalesce(p.log, []) + [$log]
                RETURN p.allowedValues AS allowedValues
            """, driver, params={"id": row["id"], "values": INITIAL_ALLOWED_VALUES,
                "log": f"{timestamp} user 0 configured metadata-backed eventType allowedValues"})
            if result != [{"allowedValues": INITIAL_ALLOWED_VALUES}]:
                raise RuntimeError(f"Metadata update failed for {database}")
        if args.apply:
            verified = getQuery("""
                MATCH (p:PROPERTY {CMName: 'eventType', type: 'relationship'})
                RETURN p.allowedValues AS allowedValues
            """, driver)
            if verified != [{"allowedValues": INITIAL_ALLOWED_VALUES}]:
                raise RuntimeError(f"Metadata verification failed for {database}")
            print(json.dumps({"database": database, "verifiedAllowedValues": verified[0]["allowedValues"]}))


if __name__ == "__main__":
    main()
