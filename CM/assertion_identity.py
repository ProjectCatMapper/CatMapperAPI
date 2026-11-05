"""Persistent, private identities for repeated USES source assertions.

Audit histories are optional, shared by imports, and change on edits. They are
retained only as a compatibility fallback for previously published URLs.
"""

import copy
import hashlib
import json
import re
import secrets
from collections import Counter


ASSERTION_ID_PATTERN = re.compile(r"[0-9a-f]{16}")
NEW_ASSERTION_ID = "substring(replace(randomUUID(), '-', ''), 0, 16)"

COLLISION_GROUP_QUERY = """
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
WHERE $cmids IS NULL OR c.CMID IN $cmids
WITH d, c, r.Key AS key, collect(r) AS ties
WHERE size(ties) > 1
UNWIND ties AS r
WITH d, c, key, r ORDER BY elementId(r)
WITH d, c, key, collect(r) AS ties
RETURN d.CMID AS datasetCmid, key, c.CMID AS conceptCmid,
       size(ties) AS multiplicity,
       [r IN ties | {relId: elementId(r), assertionId: properties(r)['assertionId'],
                    stableDiscriminator: r.logID, properties: properties(r)}] AS identities
ORDER BY datasetCmid, key, conceptCmid
"""


def legacy_assertion_id(discriminator):
    if discriminator in (None, "", []):
        return None
    encoded = json.dumps(discriminator, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def assertion_token(row):
    token = row.get("assertionId")
    if token is not None:
        if not isinstance(token, str) or not ASSERTION_ID_PATTERN.fullmatch(token):
            raise ValueError("USES assertion has an invalid persistent assertionId.")
        return token
    token = legacy_assertion_id(row.get("stableDiscriminator"))
    if token is None:
        raise ValueError("Duplicate USES assertion lacks a stable public identity discriminator.")
    return token


def validate_collision_groups(groups):
    """Reject ambiguity before pagination or streaming can hide sibling ties."""
    for group in groups:
        rows = group["identities"]
        tokens = [assertion_token(row) for row in rows]
        if len(tokens) != int(group["multiplicity"]) or len(set(tokens)) != len(tokens):
            raise ValueError("Duplicate USES assertions have indistinguishable public identities.")


def plan_assertion_ids(groups):
    """Preserve usable published suffixes; allocate IDs only for unpinned ties.

    A shared legacy suffix was already ambiguous, so neither tie can safely
    retain it. Existing persistent IDs are immutable and never silently repaired.
    """
    changes = []
    for group in groups:
        rows = group["identities"]
        pinned = [assertion_token(row) for row in rows if row.get("assertionId") is not None]
        if len(pinned) != len(set(pinned)):
            raise ValueError("Persistent assertion IDs collide; explicit review is required.")
        legacy = Counter(legacy_assertion_id(row.get("stableDiscriminator"))
                         for row in rows if row.get("assertionId") is None)
        used = set(pinned)
        for row in rows:
            if row.get("assertionId") is not None:
                continue
            token = legacy_assertion_id(row.get("stableDiscriminator"))
            if token is None or legacy[token] != 1 or token in used:
                token = secrets.token_hex(8)
                while token in used:
                    token = secrets.token_hex(8)
            used.add(token)
            changes.append({"relId": row["relId"], "before": copy.deepcopy(row["properties"]),
                            "assertionId": token})
    return changes


def ensure_assertion_ids(driver, cmids=None):
    """Pin collision identities during normal USES reconciliation.

    All guarded assignments happen in one query and participate in an outer
    merge transaction through getQuery. No source ties or log entries are removed.
    """
    from .utils import getQuery

    groups = getQuery(COLLISION_GROUP_QUERY, driver=driver, params={"cmids": cmids}, type="dict")
    changes = plan_assertion_ids(groups)
    apply_assertion_id_plan(driver, changes)
    after = getQuery(COLLISION_GROUP_QUERY, driver=driver, params={"cmids": cmids}, type="dict") if changes else groups
    validate_collision_groups(after)
    return len(changes)


def apply_assertion_id_plan(driver, changes):
    """Apply an exact property-guarded plan in a single transaction/query."""
    from .utils import getQuery

    if len({change["relId"] for change in changes}) != len(changes):
        raise ValueError("Assertion identity plan repeats a relationship.")
    for change in changes:
        assertion_token(change)
        if "assertionId" in change["before"]:
            raise ValueError("An existing assertionId must never be rewritten.")
    if changes:
        result = getQuery("""
        UNWIND $changes AS row
        MATCH ()-[r:USES]->() WHERE elementId(r) = row.relId
        WITH collect({r: r, row: row}) AS matches
        CALL apoc.lock.rels([m IN matches | m.r])
        CALL apoc.util.validate(
          size(matches) <> size($changes) OR
          any(m IN matches WHERE properties(m.r) <> m.row.before),
          'Assertion identity preflight changed; retry with a fresh snapshot', [])
        WITH matches
        UNWIND matches AS m
        WITH m.r AS r, m.row AS row
        SET r.assertionId = row.assertionId
        RETURN count(r) AS changed
        """, driver=driver, params={"changes": changes}, type="dict")
        if result != [{"changed": len(changes)}]:
            raise RuntimeError("Assertion identity assignment count did not match its plan.")
