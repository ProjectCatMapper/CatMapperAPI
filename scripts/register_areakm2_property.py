#!/usr/bin/env python
"""Register surface area as editable USES metadata; preview unless --apply is set."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from CM.utils import getDriver, getQuery

DATABASES = ("SocioMap", "ArchaMap")
FIELDS = {
    "CMName": "areakm2", "property": "areakm2", "type": "relationship",
    "description": "Surface area (km2)",
    # USES numeric fields use string tokens with numeric upload validation.
    "metaType": "string", "translation": "areakm2", "public": "TRUE",
    "display": "TRUE", "internal": False, "editable": True,
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    targets = []
    highest_id = 0
    existing_ids = set()
    for database in DATABASES:
        driver = getDriver(database)
        existing = getQuery(
            "MATCH (p:PROPERTY {CMName: 'areakm2'}) RETURN properties(p) AS property", driver
        )
        if len(existing) > 1:
            raise RuntimeError(f"Duplicate areakm2 metadata in {database}")
        if existing:
            prop = existing[0]["property"]
            if (any(prop.get(key) != value for key, value in FIELDS.items())
                    or prop.get("relationship") is not None):
                raise RuntimeError(f"Conflicting areakm2 metadata in {database}")
            existing_ids.add(prop["CMID"])
        rows = getQuery(
            "MATCH (p:PROPERTY) RETURN max(toInteger(substring(p.CMID, 2))) AS highest", driver
        )
        highest_id = max(highest_id, rows[0]["highest"] or 0)
        targets.append((database, driver, existing))
    if len(existing_ids) > 1:
        raise RuntimeError("areakm2 property IDs differ between databases")
    cmid = next(iter(existing_ids), f"CP{highest_id + 1}")
    for database, driver, existing in targets:
        collision = getQuery(
            "MATCH (p:PROPERTY {CMID: $cmid}) WHERE p.CMName <> 'areakm2' RETURN p.CMName AS name",
            driver, params={"cmid": cmid},
        )
        if collision:
            raise RuntimeError(f"Property ID collision in {database}")
    for database, driver, existing in targets:
        print(json.dumps({"database": database, "existing": existing, "requested": {**FIELDS, "CMID": cmid}}))
        if args.apply:
            getQuery(
                "MERGE (p:PROPERTY {CMName: 'areakm2'}) "
                "ON CREATE SET p = $fields, p.log = [$log] RETURN p.CMID AS CMID",
                driver, params={"fields": {**FIELDS, "CMID": cmid},
                    "log": f"{datetime.now(timezone.utc).isoformat()} registered USES surface area property"},
            )
            verified = getQuery(
                "MATCH (p:PROPERTY {CMName: 'areakm2'}) RETURN properties(p) AS property", driver
            )
            if len(verified) != 1 or verified[0]["property"].get("relationship") is not None or any(
                verified[0]["property"].get(key) != value
                for key, value in {**FIELDS, "CMID": cmid}.items()
            ):
                raise RuntimeError(f"Metadata verification failed in {database}")
            print(json.dumps({"database": database, "verified": True, "CMID": cmid}))


if __name__ == "__main__":
    main()
