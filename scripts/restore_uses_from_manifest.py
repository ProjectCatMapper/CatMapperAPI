#!/usr/bin/env python
"""Preview or restore missing USES assertions from a reviewed recovery manifest.

Existing relationships are never overwritten. Every endpoint, logical key, and
property is checked before writes and again on an independent read afterward.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from CM.utils import config, getDriver
from neo4j import GraphDatabase


READ = """
UNWIND $rows AS row
OPTIONAL MATCH (d:DATASET {CMID: row.sourceCMID})
OPTIONAL MATCH (c:CATEGORY {CMID: row.targetCMID})
OPTIONAL MATCH (d)-[r:USES {Key: row.properties.Key}]->(c)
RETURN row.sourceCMID AS sourceCMID, row.targetCMID AS targetCMID,
       row.properties.Key AS Key, d IS NOT NULL AS sourceExists,
       c IS NOT NULL AND NOT c:DELETED AS targetExists,
       count(DISTINCT d) AS sourceCount, count(DISTINCT c) AS targetCount,
       collect(properties(r)) AS existing
"""


def identity(row):
    return row["sourceCMID"], row["targetCMID"], row["properties"]["Key"]


def validate_manifest(rows):
    if not rows:
        raise ValueError("The recovery manifest is empty.")
    seen = set()
    for row in rows:
        locator = identity(row)
        if not all(isinstance(part, str) and part.strip() for part in locator):
            raise ValueError("Every restored tie needs dataset CMID, category CMID, and Key.")
        if locator in seen:
            raise ValueError(f"Duplicate recovery locator: {locator}")
        seen.add(locator)
        if not row.get("deletionTxId") or not row.get("deletedAt"):
            raise ValueError(f"Missing deletion provenance: {locator}")


def check_current(rows, current):
    expected = {identity(row): row for row in rows}
    if len(current) != len(expected):
        raise ValueError("Recovery read returned an unexpected number of endpoints.")
    missing = []
    for found in current:
        locator = found["sourceCMID"], found["targetCMID"], found["Key"]
        row = expected[locator]
        if not found["sourceExists"] or not found["targetExists"]:
            raise ValueError(f"Missing or deleted recovery endpoint: {locator}")
        if found["sourceCount"] != 1 or found["targetCount"] != 1:
            raise ValueError(f"Ambiguous recovery endpoint: {locator}")
        if not found["existing"]:
            missing.append(row)
        elif found["existing"] != [row["properties"]]:
            raise ValueError(f"Existing USES tie conflicts with recovery evidence: {locator}")
    return missing


def assert_safe_metadata(tx):
    rows = tx.run("""
        MATCH (p:PROPERTY {CMName:'areakm2'})
        RETURN p.relationship AS relationship
    """).data()
    if any(row["relationship"] is not None for row in rows):
        raise ValueError("Correct areakm2 metadata before restoring source USES ties.")


def restore_batch(tx, rows):
    assert_safe_metadata(tx)
    missing = check_current(rows, tx.run(READ, rows=rows).data())
    result = tx.run("""
        UNWIND $rows AS row
        MATCH (d:DATASET {CMID:row.sourceCMID})
        MATCH (c:CATEGORY {CMID:row.targetCMID})
        WHERE NOT c:DELETED
        MERGE (d)-[r:USES {Key:row.properties.Key}]->(c)
        ON CREATE SET r = row.properties
        RETURN row.sourceCMID AS sourceCMID, row.targetCMID AS targetCMID,
               r.Key AS Key, elementId(r) AS restoredRelID
    """, rows=missing).data()
    if len(result) != len(missing):
        raise ValueError("A recovery endpoint changed during restoration; batch rolled back.")
    if check_current(rows, tx.run(READ, rows=rows).data()):
        raise ValueError("Restored properties differ from evidence; batch rolled back.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--database", required=True, choices=["sociomap", "archamap"])
    parser.add_argument("--uri", help="Explicit recovery target, without credentials")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    content = args.manifest.read_bytes()
    rows = json.loads(content)
    validate_manifest(rows)
    driver = (GraphDatabase.driver(args.uri, auth=(config["DB"]["user"], config["DB"]["pwd"]))
              if args.uri else getDriver(args.database))
    batches = [rows[i:i + 250] for i in range(0, len(rows), 250)]
    with driver.session() as session:
        assert_safe_metadata(session)
        missing = []
        for batch in batches:
            missing.extend(check_current(batch, session.run(READ, rows=batch).data()))
    summary = {"database": args.database, "manifestSHA256": hashlib.sha256(content).hexdigest(),
               "ties": len(rows), "categories": len({r["targetCMID"] for r in rows}),
               "missing": len(missing), "alreadyPresent": len(rows) - len(missing),
               "apply": args.apply}
    print(json.dumps(summary), flush=True)
    with args.report.open("a") as report:
        report.write(json.dumps({"preflight": summary}) + "\n")
        report.flush()
        if args.apply:
            for batch in batches:
                with driver.session() as session:
                    restored = session.execute_write(restore_batch, batch)
                report.write(json.dumps({"restored": restored}) + "\n")
                report.flush()
            with driver.session() as session:
                for batch in batches:
                    if check_current(batch, session.run(READ, rows=batch).data()):
                        raise ValueError("Independent read-back found missing restored ties.")
            report.write(json.dumps({"verified": len(rows)}) + "\n")
            print(json.dumps({"independentlyVerified": len(rows)}), flush=True)
    driver.close()


if __name__ == "__main__":
    main()
