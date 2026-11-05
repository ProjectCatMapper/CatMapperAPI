#!/usr/bin/env python3
"""Preview, then explicitly apply a guarded collision-identity repair.

Run from CatMapperAPI in api_env. Preview is read-only in Neo4j. Applying a
saved plan adds assertionId to its exact source ties and converts only reviewed
internal CATEGORY singleton labels to strings, in one transaction.
"""

import argparse
import copy
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from CM.assertion_identity import (
    COLLISION_GROUP_QUERY, apply_assertion_id_plan, plan_assertion_ids,
    validate_collision_groups,
)
from CM.utils import closeAllDrivers, getDriver, getQuery, query_transaction


LABEL_DRIFT_QUERY = """
MATCH (d:DATASET {CMID: $dataset})-[r:USES]->(c:CATEGORY)
WHERE r.label IS NOT NULL AND valueType(r.label) <> 'STRING NOT NULL'
RETURN d.CMID AS datasetCmid, c.CMID AS conceptCmid, elementId(r) AS relId,
       properties(r) AS properties
ORDER BY relId
"""


def validate_labels(rows):
    for row in rows:
        props = row["properties"]
        if props.get("label") != ["CATEGORY"] or props.get("Key") != "Key == " + row["conceptCmid"]:
            raise ValueError("Unexpected internal label drift requires separate review.")


def expected_after(groups, changes):
    expected = copy.deepcopy(groups)
    by_id = {change["relId"]: change for change in changes}
    if len(by_id) != len(changes):
        raise ValueError("Repair plan repeats a relationship.")
    originals = {row["relId"]: row["properties"] for group in groups for row in group["identities"]}
    if any(change["relId"] not in originals or change["before"] != originals[change["relId"]]
           or "assertionId" in change["before"] for change in changes):
        raise ValueError("Repair plan does not match its original unpinned source ties.")
    for group in expected:
        for row in group["identities"]:
            if row["relId"] in by_id:
                row["assertionId"] = by_id[row["relId"]]["assertionId"]
                row["properties"]["assertionId"] = row["assertionId"]
    validate_collision_groups(expected)
    return expected


def save(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    # Contains source before-images and private IDs; keep evidence restricted.
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def run(database, path, *, apply=False):
    driver = getDriver(database)

    def snapshot():
        return getQuery(COLLISION_GROUP_QUERY, driver=driver, params={"cmids": None}, type="dict")

    def labels():
        return getQuery(LABEL_DRIFT_QUERY, driver=driver,
                        params={"dataset": "AD941" if database == "archamap" else "SD11"}, type="dict")

    if not apply:
        before = snapshot()
        changes = plan_assertion_ids(before)
        label_before = labels()
        validate_labels(label_before)
        plan = {"database": database, "before": before, "changes": changes, "labelBefore": label_before}
        expected_after(before, changes)
        save(path, plan)
        return {"status": "preview", "groups": len(before), "changes": len(changes), "labels": len(label_before)}

    plan = json.loads(path.read_text())
    if plan["database"] != database:
        raise ValueError("Saved plan belongs to another database.")
    validate_labels(plan["labelBefore"])
    expected = expected_after(plan["before"], plan["changes"])
    verification_path = path.with_name(path.stem + "-verified.json")
    if verification_path.exists():
        raise ValueError("Verification evidence already exists; refusing to replay this plan.")
    with query_transaction(database, driver):
        # Lock endpoints before reading or writing source ties. Application moves
        # and creation also acquire endpoint locks, so the scoped groups stay fixed.
        cmids = sorted({g[field] for g in plan["before"] + plan["labelBefore"]
                        for field in ("datasetCmid", "conceptCmid")})
        getQuery("""MATCH (n) WHERE n.CMID IN $cmids
                    WITH collect(n) AS nodes CALL apoc.lock.nodes(nodes)
                    RETURN size(nodes) AS locked""", driver=driver, params={"cmids": cmids}, type="dict")
        if snapshot() != plan["before"] or labels() != plan["labelBefore"]:
            raise ValueError("Live collision groups changed; create and review a fresh preview.")
        apply_assertion_id_plan(driver, plan["changes"])
        # Normalize only the reviewed singleton list, preserving its literal
        # value and every other property. All changes share the transaction.
        if plan["labelBefore"]:
            result = getQuery("""
                UNWIND $rows AS row
                MATCH ()-[r:USES]->() WHERE elementId(r) = row.relId
                WITH collect({r: r, row: row}) AS matches
                CALL apoc.lock.rels([m IN matches | m.r])
                CALL apoc.util.validate(size(matches) <> size($rows) OR
                  any(m IN matches WHERE properties(m.r) <> m.row.properties),
                  'Internal label preflight changed', [])
                WITH matches UNWIND matches AS m WITH m.r AS r
                SET r.label = 'CATEGORY'
                RETURN count(r) AS changed
                """, driver=driver, params={"rows": plan["labelBefore"]}, type="dict")
            if result != [{"changed": len(plan["labelBefore"])}]:
                raise RuntimeError("Internal label repair count differs from the plan.")
        after = snapshot()
        verify_labels(driver, plan["labelBefore"])
        if after != expected or labels():
            raise RuntimeError("Transactional read-back differs from the exact repair plan.")
    # This query runs in a separate session after commit.
    after = snapshot()
    verify_labels(driver, plan["labelBefore"])
    if after != expected or plan_assertion_ids(after) or labels():
        raise RuntimeError("Independent read-back or idempotency verification failed.")
    save(verification_path, {"database": database, "after": after,
         "changed": len(plan["changes"]), "labels": len(plan["labelBefore"]), "secondPassChanges": 0})
    return {"status": "verified", "groups": len(after), "changes": len(plan["changes"]),
            "labels": len(plan["labelBefore"])}


def verify_labels(driver, before):
    if not before:
        return
    rows = getQuery("""UNWIND $rows AS row MATCH ()-[r:USES]->()
                       WHERE elementId(r) = row.relId
                       RETURN elementId(r) AS relId, properties(r) AS properties""",
                    driver=driver, params={"rows": before}, type="dict")
    expected = {row["relId"]: {**row["properties"], "label": "CATEGORY"} for row in before}
    if {row["relId"]: row["properties"] for row in rows} != expected:
        raise RuntimeError("Internal label read-back changed or lost a source property.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", choices=("sociomap", "archamap"))
    parser.add_argument("plan", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.database, args.plan, apply=args.apply)))
    finally:
        closeAllDrivers()


if __name__ == "__main__":
    main()
