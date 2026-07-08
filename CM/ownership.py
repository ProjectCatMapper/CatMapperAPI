"""Owner-scoped write authorization helpers."""

from datetime import datetime, timezone
import uuid

import pandas as pd

from .utils import getDriver, getQuery


OWNER_SCOPED_UPLOAD_OPTIONS = {"node_add", "node_replace", "update_add", "update_replace"}


class OwnershipError(PermissionError):
    """Raised when an authenticated non-admin user targets unowned graph data."""


def is_admin_claims(claims):
    return str((claims or {}).get("role") or "").strip().lower() == "admin"


def normalize_actor_claims(claims):
    claims = claims or {}
    userid = str(claims.get("userid") or "").strip()
    role = str(claims.get("role") or "user").strip().lower() or "user"
    if not userid:
        raise Exception("Missing credentials")
    return {"userid": userid, "role": role}


def new_contribution_id():
    return f"contribution_{uuid.uuid4().hex}"


def ownership_metadata(claims, contribution_id=None):
    actor = normalize_actor_claims(claims)
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "createdByUserId": actor["userid"],
        "ownerUserId": actor["userid"],
        "createdAt": now,
        "contributionId": contribution_id or new_contribution_id(),
    }


def _dedupe_nonempty(values):
    seen = set()
    out = []
    for value in values or []:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            out.append(text)
    return out


def _ownership_failure(object_type, values):
    sample = ", ".join(values[:10])
    suffix = "..." if len(values) > 10 else ""
    raise OwnershipError(
        f"User is not authorized to edit unowned {object_type}: {sample}{suffix}"
    )


def _owned_count_expr(alias):
    return (
        f"CASE WHEN toString(coalesce({alias}.ownerUserId, '')) = $userid "
        f"OR toString(coalesce({alias}.createdByUserId, '')) = $userid "
        "THEN 1 ELSE 0 END"
    )


def assert_owned_nodes(database, cmids, claims):
    actor = normalize_actor_claims(claims)
    cmids = _dedupe_nonempty(cmids)
    if not cmids or is_admin_claims(actor):
        return True

    driver = getDriver(database)
    query = f"""
    UNWIND $cmids AS cmid
    OPTIONAL MATCH (n {{CMID: toString(cmid)}})
    WHERE NOT n:DELETED
    WITH toString(cmid) AS cmid,
         count(n) AS targetCount,
         sum({_owned_count_expr("n")}) AS ownedCount
    RETURN cmid, targetCount, ownedCount
    """
    rows = getQuery(query=query, driver=driver, params={"cmids": cmids, "userid": actor["userid"]}, type="dict")
    missing = [row["cmid"] for row in rows if int(row.get("targetCount") or 0) == 0]
    if missing:
        raise ValueError(f"Node not found: {', '.join(missing)}")

    unowned = [
        row["cmid"]
        for row in rows
        if int(row.get("targetCount") or 0) != int(row.get("ownedCount") or 0)
    ]
    if unowned:
        _ownership_failure("node", unowned)
    return True


def assert_owner_scoped_node_removal_allowed(database, cmid, claims):
    """Allow a non-admin to remove/merge away only an owned, isolated target node."""
    actor = normalize_actor_claims(claims)
    cmids = _dedupe_nonempty([cmid])
    if not cmids or is_admin_claims(actor):
        return True

    assert_owned_nodes(database, cmids, actor)
    target_cmid = cmids[0]
    driver = getDriver(database)

    incident_query = f"""
    MATCH (n {{CMID: $cmid}})
    OPTIONAL MATCH (n)-[r:USES]-()
    WITH $cmid AS cmid,
         count(r) AS incidentUses,
         sum(
           CASE
             WHEN r IS NULL THEN 0
             WHEN {_owned_count_expr("r")} = 1 THEN 0
             ELSE 1
           END
         ) AS unownedIncidentUses
    RETURN cmid, incidentUses, unownedIncidentUses
    """
    incident_rows = getQuery(
        query=incident_query,
        driver=driver,
        params={"cmid": target_cmid, "userid": actor["userid"]},
        type="dict",
    )
    incident_row = (incident_rows or [{}])[0]
    if int(incident_row.get("unownedIncidentUses") or 0) > 0:
        raise OwnershipError(
            f"User is not authorized to merge or delete {target_cmid}; "
            "the node has USES ties not owned by this user"
        )

    reference_query = """
    MATCH (n {CMID: $cmid})
    MATCH (:DATASET)-[r:USES]-(:CATEGORY)
    WHERE NOT elementId(startNode(r)) = elementId(n)
      AND NOT elementId(endNode(r)) = elementId(n)
      AND any(k IN keys(r) WHERE toString(r[k]) = $cmid OR toString(r[k]) CONTAINS $cmid)
    RETURN elementId(r) AS relID, r.Key AS Key
    LIMIT 10
    """
    reference_rows = getQuery(
        query=reference_query,
        driver=driver,
        params={"cmid": target_cmid},
        type="dict",
    )
    if reference_rows:
        rels = [
            str(row.get("relID") or row.get("Key") or "unknown")
            for row in reference_rows
        ]
        raise OwnershipError(
            f"User is not authorized to merge or delete {target_cmid}; "
            "the CMID is referenced in other USES ties: "
            + ", ".join(rels)
        )

    return True


def assert_owned_uses_by_relids(database, relids, claims):
    actor = normalize_actor_claims(claims)
    relids = _dedupe_nonempty(relids)
    if not relids or is_admin_claims(actor):
        return True

    driver = getDriver(database)
    query = f"""
    UNWIND $relids AS relID
    OPTIONAL MATCH ()-[r:USES]->()
    WHERE elementId(r) = toString(relID)
    WITH toString(relID) AS relID,
         count(r) AS targetCount,
         sum({_owned_count_expr("r")}) AS ownedCount
    RETURN relID, targetCount, ownedCount
    """
    rows = getQuery(query=query, driver=driver, params={"relids": relids, "userid": actor["userid"]}, type="dict")
    missing = [row["relID"] for row in rows if int(row.get("targetCount") or 0) == 0]
    if missing:
        raise ValueError(f"USES relationship not found: {', '.join(missing)}")

    unowned = [
        row["relID"]
        for row in rows
        if int(row.get("targetCount") or 0) != int(row.get("ownedCount") or 0)
    ]
    if unowned:
        _ownership_failure("USES relationship", unowned)
    return True


def owned_uses_relids(database, relids, claims):
    actor = normalize_actor_claims(claims)
    relids = _dedupe_nonempty(relids)
    if not relids:
        return set()
    if is_admin_claims(actor):
        return set(relids)

    driver = getDriver(database)
    query = f"""
    UNWIND $relids AS relID
    MATCH ()-[r:USES]->()
    WHERE elementId(r) = toString(relID)
    WITH toString(relID) AS relID, {_owned_count_expr("r")} AS owned
    WHERE owned = 1
    RETURN relID
    """
    rows = getQuery(query=query, driver=driver, params={"relids": relids, "userid": actor["userid"]}, type="dict")
    return {str(row.get("relID")) for row in rows or [] if row.get("relID")}


def assert_owned_uses_by_triplets(database, rows, claims):
    actor = normalize_actor_claims(claims)
    if is_admin_claims(actor):
        return True

    frame = pd.DataFrame(rows or [])
    required = {"CMID", "datasetID", "Key"}
    if frame.empty or not required.issubset(set(frame.columns)):
        return True

    triplets = (
        frame[["CMID", "datasetID", "Key"]]
        .fillna("")
        .astype(str)
        .drop_duplicates()
        .to_dict(orient="records")
    )
    triplets = [
        row for row in triplets
        if row.get("CMID", "").strip()
        and row.get("datasetID", "").strip()
        and row.get("Key", "").strip()
    ]
    if not triplets:
        return True

    driver = getDriver(database)
    query = f"""
    UNWIND $rows AS row
    OPTIONAL MATCH (:DATASET {{CMID: row.datasetID}})-[r:USES {{Key: row.Key}}]->(:CATEGORY {{CMID: row.CMID}})
    WITH row, count(r) AS targetCount, sum({_owned_count_expr("r")}) AS ownedCount
    RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.Key AS Key, targetCount, ownedCount
    """
    result = getQuery(query=query, driver=driver, params={"rows": triplets, "userid": actor["userid"]}, type="dict")
    missing = [
        f"{row.get('datasetID')} / {row.get('CMID')} / {row.get('Key')}"
        for row in result or []
        if int(row.get("targetCount") or 0) == 0
    ]
    if missing:
        raise ValueError(f"USES relationship not found: {', '.join(missing)}")

    unowned = [
        f"{row.get('datasetID')} / {row.get('CMID')} / {row.get('Key')}"
        for row in result or []
        if int(row.get("targetCount") or 0) != int(row.get("ownedCount") or 0)
    ]
    if unowned:
        _ownership_failure("USES relationship", unowned)
    return True


def validate_upload_ownership_scope(database, upload_option, rows, claims):
    if str(upload_option or "").strip() not in OWNER_SCOPED_UPLOAD_OPTIONS:
        return True
    if not claims:
        return True
    actor = normalize_actor_claims(claims)
    if is_admin_claims(actor):
        return True

    frame = pd.DataFrame(rows or [])
    if str(upload_option or "").strip() in {"node_add", "node_replace"}:
        if "CMID" not in frame.columns:
            return True
        assert_owned_nodes(database, frame["CMID"].fillna("").astype(str).tolist(), actor)
        return True

    assert_owned_uses_by_triplets(database, frame.to_dict(orient="records"), actor)
    return True
