"""Owner-scoped write authorization helpers."""

import pandas as pd

from .utils import getDriver, getQuery


OWNER_SCOPED_UPLOAD_OPTIONS = {"node_add", "node_replace", "update_add", "update_replace"}
SYSTEM_MODIFICATION_USER_IDS = {"0"}
INTERNAL_OWNER_PROPERTY_METADATA = (
    {
        "CMID": "CP188",
        "CMName": "ownerUserId",
        "property": "ownerUserId",
        "type": "node",
        "nodeType": "DATASET; CATEGORY",
        "metaType": "string",
        "description": "Internal user ID that exclusively owns this node for owner-scoped editing.",
        "internal": True,
        "editable": False,
        "display": False,
        "search": False,
        "public": False,
    },
    {
        "CMID": "CP189",
        "CMName": "ownerUserId",
        "property": "ownerUserId",
        "type": "relationship",
        "metaType": "string",
        "description": "Internal user ID that exclusively owns this USES relationship for owner-scoped editing.",
        "internal": True,
        "editable": False,
        "display": False,
        "search": False,
        "public": False,
    },
    {
        "CMID": "CP190",
        "CMName": "modifiedByOtherUser",
        "property": "modifiedByOtherUser",
        "type": "node",
        "nodeType": "DATASET; CATEGORY",
        "metaType": "boolean",
        "description": "Internal permanent lock set when a human other than the owner modifies this node.",
        "internal": True,
        "editable": False,
        "display": False,
        "search": False,
        "public": False,
    },
    {
        "CMID": "CP191",
        "CMName": "modifiedByOtherUser",
        "property": "modifiedByOtherUser",
        "type": "relationship",
        "metaType": "boolean",
        "description": "Internal permanent lock set when a human other than the owner modifies this USES relationship.",
        "internal": True,
        "editable": False,
        "display": False,
        "search": False,
        "public": False,
    },
)


class OwnershipError(PermissionError):
    """Raised when an authenticated non-admin user targets unowned graph data."""


class OwnerScopedAdminReviewRequired(OwnershipError):
    """Raised when owner-scoped node removal needs an admin review workflow."""

    def __init__(self, message, *, cmid=None, reason_code=None, details=None):
        super().__init__(message)
        self.cmid = cmid
        self.reason_code = reason_code
        self.details = details or {}

    def to_dict(self):
        return {
            "cmid": self.cmid,
            "reasonCode": self.reason_code,
            "details": self.details,
            "message": str(self),
        }


def is_admin_claims(claims):
    return str((claims or {}).get("role") or "").strip().lower() == "admin"


def normalize_actor_claims(claims):
    claims = claims or {}
    userid = str(claims.get("userid") or "").strip()
    role = str(claims.get("role") or "user").strip().lower() or "user"
    if not userid:
        raise Exception("Missing credentials")
    return {"userid": userid, "role": role}


def ownership_metadata(claims):
    actor = normalize_actor_claims(claims)
    return {
        "ownerUserId": actor["userid"],
        "modifiedByOtherUser": False,
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
        "THEN 1 ELSE 0 END"
    )


def _owner_editable_count_expr(alias):
    return (
        f"CASE WHEN toString(coalesce({alias}.ownerUserId, '')) = $userid "
        f"AND coalesce({alias}.modifiedByOtherUser, false) = false "
        "THEN 1 ELSE 0 END"
    )


def assert_owned_nodes(database, cmids, claims, require_incident_uses=True):
    actor = normalize_actor_claims(claims)
    cmids = _dedupe_nonempty(cmids)
    if not cmids or is_admin_claims(actor):
        return True

    driver = getDriver(database)
    query = f"""
    UNWIND $cmids AS cmid
    OPTIONAL MATCH (n {{CMID: toString(cmid)}})
    WHERE NOT n:DELETED
    OPTIONAL MATCH (n)-[r:USES]-()
    WITH toString(cmid) AS cmid,
         collect(DISTINCT n) AS nodes,
         collect(DISTINCT r) AS incidentUses
    RETURN cmid,
           size(nodes) AS targetCount,
           size([n IN nodes WHERE {_owner_editable_count_expr("n")} = 1]) AS ownedCount,
           size([
             r IN incidentUses
             WHERE {_owned_count_expr("r")} = 0
           ]) AS unownedIncidentUses
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
    if require_incident_uses:
        nodes_with_unowned_uses = [
            row["cmid"]
            for row in rows
            if int(row.get("unownedIncidentUses") or 0) > 0
        ]
        if nodes_with_unowned_uses:
            _ownership_failure(
                "node with incident USES relationships not owned by this user",
                nodes_with_unowned_uses,
            )
    return True


def assert_owner_scoped_node_removal_allowed(database, cmid, claims):
    """Allow a non-admin to remove/merge away only an owned, isolated target node."""
    actor = normalize_actor_claims(claims)
    cmids = _dedupe_nonempty([cmid])
    if not cmids or is_admin_claims(actor):
        return True

    assert_owned_nodes(
        database,
        cmids,
        actor,
        require_incident_uses=False,
    )
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
             WHEN {_owner_editable_count_expr("r")} = 1 THEN 0
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
        raise OwnerScopedAdminReviewRequired(
            f"User is not authorized to merge or delete {target_cmid}; "
            "the node has USES ties not owned by this user or modified by another user",
            cmid=target_cmid,
            reason_code="unowned_incident_uses",
            details={
                "incidentUses": int(incident_row.get("incidentUses") or 0),
                "unownedIncidentUses": int(incident_row.get("unownedIncidentUses") or 0),
            },
        )

    reference_query = """
    MATCH (n {CMID: $cmid})
    MATCH (:DATASET)-[r:USES]-(:CATEGORY)
    WHERE NOT elementId(startNode(r)) = elementId(n)
      AND NOT elementId(endNode(r)) = elementId(n)
      AND r.ownerUserId IS NOT NULL
      AND toString(r.ownerUserId) <> $userid
      AND any(k IN keys(r) WHERE toString(r[k]) = $cmid OR toString(r[k]) CONTAINS $cmid)
    RETURN elementId(r) AS relID, r.Key AS Key, r.ownerUserId AS ownerUserId
    LIMIT 10
    """
    reference_rows = getQuery(
        query=reference_query,
        driver=driver,
        params={"cmid": target_cmid, "userid": actor["userid"]},
        type="dict",
    )
    if reference_rows:
        rels = [
            str(row.get("relID") or row.get("Key") or "unknown")
            for row in reference_rows
        ]
        raise OwnerScopedAdminReviewRequired(
            f"User is not authorized to merge or delete {target_cmid}; "
            "the CMID is referenced in USES ties owned by another user: "
            + ", ".join(rels),
            cmid=target_cmid,
            reason_code="cmid_referenced_elsewhere",
            details={"references": rels},
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
         sum({_owner_editable_count_expr("r")}) AS ownedCount
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
    WITH toString(relID) AS relID, {_owner_editable_count_expr("r")} AS owned
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
    WITH row, count(r) AS targetCount, sum({_owner_editable_count_expr("r")}) AS ownedCount
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


def _result_count(rows, key="count"):
    if not rows:
        return 0
    return int((rows[0] or {}).get(key) or 0)


def ensure_internal_owner_property_metadata(database):
    """Create or repair the four internal PROPERTY definitions."""
    driver = getDriver(database)
    definitions = [dict(row) for row in INTERNAL_OWNER_PROPERTY_METADATA]
    conflicts = getQuery(
        query="""
        // OWNER_METADATA_CONFLICT_CHECK
        UNWIND $definitions AS definition
        OPTIONAL MATCH (p:METADATA {CMID: definition.CMID})
        WITH definition, p
        WHERE p IS NOT NULL
          AND (
            coalesce(p.CMName, '') <> definition.CMName
            OR coalesce(p.type, '') <> definition.type
          )
        RETURN definition.CMID AS CMID,
               definition.CMName AS expectedName,
               definition.type AS expectedType,
               p.CMName AS actualName,
               p.type AS actualType
        """,
        driver=driver,
        params={"definitions": definitions},
        type="dict",
    )
    if conflicts:
        raise ValueError(
            "Internal ownership PROPERTY CMID conflict: "
            + ", ".join(str(row.get("CMID")) for row in conflicts)
        )

    rows = getQuery(
        query="""
        // OWNER_METADATA_DEFINITION_UPSERT
        UNWIND $definitions AS definition
        MERGE (p:METADATA:PROPERTY {CMID: definition.CMID})
        SET p += definition
        RETURN count(p) AS count
        """,
        driver=driver,
        params={"definitions": definitions},
        type="dict",
    )
    return _result_count(rows)


def reconcile_owner_edit_metadata(database, return_type="data"):
    """Reconcile simplified owner-edit metadata and remove legacy fields.

    Existing true locks are monotonic. Objects with no usable history fail
    closed. User ``0`` is treated as an automated system actor.
    """
    driver = getDriver(database)
    system_users = sorted(SYSTEM_MODIFICATION_USER_IDS)
    counts = {}

    counts["propertyDefinitions"] = ensure_internal_owner_property_metadata(database)

    rows = getQuery(
        query="""
        // OWNER_METADATA_NODE_OWNER_BACKFILL
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET)
          AND NOT n:DELETED
          AND (n.ownerUserId IS NOT NULL OR n.createdByUserId IS NOT NULL)
        WITH n, toString(coalesce(n.ownerUserId, n.createdByUserId)) AS owner
        WHERE owner <> ''
          AND (n.ownerUserId IS NULL OR toString(n.ownerUserId) <> owner)
        SET n.ownerUserId = owner
        RETURN count(n) AS count
        """,
        driver=driver,
        type="dict",
    )
    counts["nodeOwnersBackfilled"] = _result_count(rows)

    rows = getQuery(
        query="""
        // OWNER_METADATA_USES_OWNER_BACKFILL
        MATCH ()-[r:USES]->()
        WHERE r.ownerUserId IS NOT NULL OR r.createdByUserId IS NOT NULL
        WITH r, toString(coalesce(r.ownerUserId, r.createdByUserId)) AS owner
        WHERE owner <> ''
          AND (r.ownerUserId IS NULL OR toString(r.ownerUserId) <> owner)
        SET r.ownerUserId = owner
        RETURN count(r) AS count
        """,
        driver=driver,
        type="dict",
    )
    counts["usesOwnersBackfilled"] = _result_count(rows)

    rows = getQuery(
        query="""
        // OWNER_METADATA_NODE_LOCK_RECONCILE
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET)
          AND NOT n:DELETED
          AND n.ownerUserId IS NOT NULL
        OPTIONAL MATCH (n)-[:HAS_LOG]->(l:LOG)
        WITH n, collect(DISTINCT toString(l.user)) AS users
        WITH n, users, coalesce(toBooleanOrNull(toString(n.modifiedByOtherUser)), false) AS currentLock
        WITH n,
             CASE
               WHEN currentLock = true THEN true
               WHEN size(users) = 0 THEN true
               WHEN any(
                 user IN users
                 WHERE NOT user IN $systemUsers
                   AND user <> toString(n.ownerUserId)
               ) THEN true
               ELSE false
             END AS desired
        WHERE n.modifiedByOtherUser IS NULL
           OR n.modifiedByOtherUser <> desired
        SET n.modifiedByOtherUser = desired
        RETURN count(n) AS count
        """,
        driver=driver,
        params={"systemUsers": system_users},
        type="dict",
    )
    counts["nodeLocksUpdated"] = _result_count(rows)

    rows = getQuery(
        query="""
        // OWNER_METADATA_USES_LOCK_RECONCILE
        MATCH ()-[r:USES]->()
        WHERE r.ownerUserId IS NOT NULL
        WITH r, [
          id IN apoc.coll.flatten([r.logID], true)
          WHERE id IS NOT NULL | toString(id)
        ] AS logIds
        OPTIONAL MATCH (l:LOG)
        WHERE elementId(l) IN logIds
        WITH r, collect(DISTINCT toString(l.user)) AS users
        WITH r, users, coalesce(toBooleanOrNull(toString(r.modifiedByOtherUser)), false) AS currentLock
        WITH r,
             CASE
               WHEN currentLock = true THEN true
               WHEN size(users) = 0 THEN true
               WHEN any(
                 user IN users
                 WHERE NOT user IN $systemUsers
                   AND user <> toString(r.ownerUserId)
               ) THEN true
               ELSE false
             END AS desired
        WHERE r.modifiedByOtherUser IS NULL
           OR r.modifiedByOtherUser <> desired
        SET r.modifiedByOtherUser = desired
        RETURN count(r) AS count
        """,
        driver=driver,
        params={"systemUsers": system_users},
        type="dict",
    )
    counts["usesLocksUpdated"] = _result_count(rows)

    rows = getQuery(
        query="""
        // OWNER_METADATA_NODE_LEGACY_REMOVE
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET)
          AND n.ownerUserId IS NOT NULL
          AND (
            n.createdByUserId IS NOT NULL
            OR n.createdAt IS NOT NULL
            OR n.contributionId IS NOT NULL
          )
        REMOVE n.createdByUserId, n.createdAt, n.contributionId
        RETURN count(n) AS count
        """,
        driver=driver,
        type="dict",
    )
    counts["nodeLegacyPropertiesRemoved"] = _result_count(rows)

    rows = getQuery(
        query="""
        // OWNER_METADATA_USES_LEGACY_REMOVE
        MATCH ()-[r:USES]->()
        WHERE r.ownerUserId IS NOT NULL
          AND (
            r.createdByUserId IS NOT NULL
            OR r.createdAt IS NOT NULL
            OR r.contributionId IS NOT NULL
          )
        REMOVE r.createdByUserId, r.createdAt, r.contributionId
        RETURN count(r) AS count
        """,
        driver=driver,
        type="dict",
    )
    counts["usesLegacyPropertiesRemoved"] = _result_count(rows)

    verification = getQuery(
        query="""
        // OWNER_METADATA_VERIFY
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET)
          AND NOT n:DELETED
          AND n.ownerUserId IS NOT NULL
        WITH count(n) AS ownedNodes,
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
        WHERE r.ownerUserId IS NOT NULL
        RETURN ownedNodes,
               nodesMissingLock,
               nodesWithLegacy,
               count(r) AS ownedUses,
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
    verification_row = (verification or [{}])[0]
    counts["verification"] = {
        key: int(verification_row.get(key) or 0)
        for key in (
            "ownedNodes",
            "nodesMissingLock",
            "nodesWithLegacy",
            "ownedUses",
            "usesMissingLock",
            "usesWithLegacy",
        )
    }

    failures = {
        key: counts["verification"][key]
        for key in (
            "nodesMissingLock",
            "nodesWithLegacy",
            "usesMissingLock",
            "usesWithLegacy",
        )
        if counts["verification"][key] != 0
    }
    if failures:
        raise RuntimeError(f"Owner metadata reconciliation incomplete: {failures}")

    info = (
        f"Owned nodes: {counts['verification']['ownedNodes']}; "
        f"owned USES ties: {counts['verification']['ownedUses']}; "
        f"node locks updated: {counts['nodeLocksUpdated']}; "
        f"USES locks updated: {counts['usesLocksUpdated']}; "
        f"legacy node properties removed: {counts['nodeLegacyPropertiesRemoved']}; "
        f"legacy USES properties removed: {counts['usesLegacyPropertiesRemoved']}."
    )
    if return_type == "info":
        return {"info": info, "data": counts}
    if return_type == "data":
        return counts
    raise ValueError("return_type must be 'data' or 'info'")
