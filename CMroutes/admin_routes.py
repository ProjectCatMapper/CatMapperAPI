from flask import Blueprint, request, jsonify, render_template, make_response
from CM import *
from CM.ownership import (
    OwnershipError,
    OwnerScopedAdminReviewRequired,
    assert_owner_scoped_node_removal_allowed,
    assert_owned_nodes,
    assert_owned_uses_by_relids,
    assert_owned_uses_by_triplets,
    is_admin_claims,
    normalize_actor_claims,
)
import json
import os
from datetime import datetime, timezone
from uuid import uuid4
from .auth_utils import verify_request_auth, classify_auth_error_status
from .extensions import mail
from .task_queue import enqueue_change_review_approval, is_rq_enabled

admin_bp = Blueprint('admin', __name__)

_ADMIN_USES_NON_ADDABLE_COMPONENT_PROPERTIES = {
    "eventdate",
    "eventtype",
    "latitude",
    "longitude",
}


def _admin_uses_property_addable(property_name):
    normalized = str(property_name or "").strip().lower()
    return normalized not in _ADMIN_USES_NON_ADDABLE_COMPONENT_PROPERTIES


def _admin_uses_property_reltype_addable(reltype):
    if reltype is None:
        return True

    if isinstance(reltype, (list, tuple, set)):
        values = reltype
    else:
        values = str(reltype).replace("||", "|").replace(",", "|").split("|")

    normalized_values = {
        str(value or "").strip().upper()
        for value in values
        if str(value or "").strip()
    }
    return "MERGING" not in normalized_values


def _parse_credentials(raw_value):
    value = unlist(raw_value)
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return None
    return None


OWNER_SCOPED_ADMIN_EDIT_FUNCTIONS = {
    "add/edit/delete node property",
    "add/edit/delete USES property",
    "delete USES relation",
    "move USES tie",
    "merge nodes",
    "delete node",
}

OWNER_SCOPED_FORBIDDEN_PROPERTY_NAMES = {"log", "logid"}
OWNER_SCOPED_USES_EDITABLE_PROPERTY_NAMES = {"key", "label", "name"}


def _require_admin_claims(claims):
    if not is_admin_claims(claims):
        raise OwnershipError("User is not authorized for this admin function")
    return True


def _selected_uses_relid(input_payload):
    input_payload = input_payload or {}
    raw_selection = input_payload.get("s1_7")

    relations = input_payload.get("s1_4") or []
    try:
        selected_index = int(raw_selection) - 1
        if isinstance(relations, list) and 0 <= selected_index < len(relations):
            selected_relation = relations[selected_index]
            if isinstance(selected_relation, list) and len(selected_relation) > 1:
                rel_props = selected_relation[1] if isinstance(selected_relation[1], dict) else {}
                rel_id = rel_props.get("id")
                if rel_id:
                    return str(rel_id)
    except (TypeError, ValueError):
        pass

    try:
        parsed = json.loads(raw_selection) if isinstance(raw_selection, str) else raw_selection
    except Exception:
        parsed = None
    if isinstance(parsed, list) and len(parsed) > 1 and isinstance(parsed[1], dict):
        rel_id = parsed[1].get("id")
        if rel_id:
            return str(rel_id)

    raise ValueError("Selected USES tie is invalid or no longer available.")


def _authorize_admin_edit_function(fun, database, input_payload, tabledata, dataset_id, claims):
    if is_admin_claims(claims):
        return True

    if fun not in OWNER_SCOPED_ADMIN_EDIT_FUNCTIONS:
        raise OwnershipError("User is not authorized for this admin function")

    input_payload = input_payload or {}
    if fun == "add/edit/delete node property":
        prop = str(input_payload.get("s1_7") or "").strip().lower()
        if prop in OWNER_SCOPED_FORBIDDEN_PROPERTY_NAMES:
            raise OwnershipError("User is not authorized to edit log properties")
        assert_owned_nodes(database, [input_payload.get("s1_2")], claims)
        return True

    if fun == "merge nodes":
        assert_owner_scoped_node_removal_allowed(database, input_payload.get("s1_3"), claims)
        return True

    if fun == "delete node":
        assert_owner_scoped_node_removal_allowed(database, input_payload.get("s1_2"), claims)
        return True

    if fun == "add/edit/delete USES property":
        prop = str(input_payload.get("s1_8") or "").strip().lower()
        if prop not in OWNER_SCOPED_USES_EDITABLE_PROPERTY_NAMES:
            raise OwnershipError("User is not authorized to edit this USES property")

    rel_id = _selected_uses_relid(input_payload)
    assert_owned_uses_by_relids(database, [rel_id], claims)

    if fun == "move USES tie":
        additional_rows = []
        for row in tabledata or []:
            if not isinstance(row, dict):
                continue
            if row.get("optionA") == "From":
                continue
            additional_rows.append({
                "CMID": row.get("CMID"),
                "Key": row.get("Key"),
                "datasetID": dataset_id,
            })
        assert_owned_uses_by_triplets(database, additional_rows, claims)

    return True


def _node_removal_review_target(fun, input_payload):
    input_payload = input_payload or {}
    if fun == "merge nodes":
        return str(input_payload.get("s1_3") or "").strip()
    if fun == "delete node":
        return str(input_payload.get("s1_2") or "").strip()
    return ""


@admin_bp.route('/admin/node-removal-review-request', methods=['POST'])
def request_node_removal_admin_review():
    try:
        data = request.get_json(silent=True) or {}
        database = unlist(data.get("database"))
        action = unlist(data.get("fun") or data.get("action"))
        input_payload = unlist(data.get("input")) or {}
        reason = str(unlist(data.get("reason")) or "").strip()
        credentials = _parse_credentials(data.get("cred"))

        if database is None:
            raise Exception("Database not specified")
        if action not in {"merge nodes", "delete node"}:
            raise Exception("Review requests are only supported for merge nodes and delete node")
        if not reason:
            raise Exception("A reason is required for admin review")

        claims = normalize_actor_claims(verify_request_auth(credentials=credentials, req=request))
        if is_admin_claims(claims):
            raise OwnershipError("Admin users can complete this action directly")

        target_cmid = _node_removal_review_target(action, input_payload)
        if not target_cmid:
            raise Exception("Target CMID is required")

        try:
            assert_owner_scoped_node_removal_allowed(database, target_cmid, claims)
        except OwnerScopedAdminReviewRequired as review_error:
            review = review_error.to_dict()
        except Exception:
            raise
        else:
            raise Exception("This action is eligible for user completion and does not require admin review")

        saved_review = _create_change_review(
            database=database,
            action=action,
            input_payload=input_payload,
            tabledata=[],
            dataset_id="",
            claims=claims,
            reason=(
                f"{review.get('message') or ''}\n"
                f"Reason code: {review.get('reasonCode') or ''}\n"
                f"Details: {json.dumps(review.get('details') or {}, sort_keys=True, default=str)}\n"
                f"User reason: {reason}"
            ),
        )
        return jsonify({
            "message": "Admin review request sent.",
            "review": review,
            "changeReview": saved_review,
        }), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _change_review_database_key(database):
    normalized = str(database or "").strip().lower()
    if normalized not in {"sociomap", "archamap"}:
        raise ValueError("Change review is supported only for SocioMap and ArchaMap")
    return normalized


def _change_review_email_delivery_enabled():
    return str(os.getenv("CATMAPPER_CHANGE_REVIEW_EMAIL_ENABLED", "0")).strip().lower() in {
        "1", "true", "yes", "on"
    }


def _change_review_default_recipient_emails():
    configured = os.getenv(
        "CATMAPPER_CHANGE_REVIEW_DEFAULT_RECIPIENTS",
        "rbischoff@asu.edu,dhruschk@asu.edu",
    )
    return {
        email.strip().lower()
        for email in str(configured or "").split(",")
        if email.strip()
    }


def _change_review_email_preference(row, database_key):
    pref = row.get("socioPref") if database_key == "sociomap" else row.get("archaPref")
    if pref is not None:
        return pref is True
    email = str(row.get("email") or "").strip().lower()
    return email in _change_review_default_recipient_emails()


def _change_review_target(action, input_payload):
    input_payload = input_payload or {}
    if action == "merge nodes":
        return str(input_payload.get("s1_3") or "").strip()
    return str(input_payload.get("s1_2") or "").strip()


def _serialize_change_review(row):
    def load_json(value, fallback):
        if not value:
            return fallback
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            return fallback

    return {
        "requestId": str(row.get("requestId") or ""),
        "database": str(row.get("database") or ""),
        "action": str(row.get("action") or ""),
        "targetCmid": str(row.get("targetCmid") or ""),
        "submittedBy": str(row.get("submittedBy") or ""),
        "submitterName": str(row.get("submitterName") or ""),
        "submittedAt": row.get("submittedAt") or "",
        "status": str(row.get("status") or "pending"),
        "authorizationReason": str(row.get("authorizationReason") or ""),
        "input": load_json(row.get("inputJson"), {}),
        "tabledata": load_json(row.get("tabledataJson"), []),
        "datasetID": row.get("datasetID") or "",
        "notifyRequester": bool(row.get("notifyRequester", False)),
        "decisionNote": str(row.get("decisionNote") or ""),
        "decidedBy": str(row.get("decidedBy") or ""),
        "decidedAt": row.get("decidedAt") or "",
        "startedAt": row.get("startedAt") or "",
        "backgroundJobId": str(row.get("backgroundJobId") or ""),
        "lastError": str(row.get("lastError") or ""),
    }


def _change_review_admin_recipients(database):
    database_key = _change_review_database_key(database)
    rows = getQuery(
        query="""
        MATCH (u:USER)
        WHERE toLower(coalesce(u.role, '')) = 'admin'
          AND toLower(coalesce(u.access, '')) = 'enabled'
          AND trim(coalesce(u.email, '')) <> ''
        RETURN u.email AS email, u.database AS database,
               properties(u)['changeReviewEmailSocioMap'] AS socioPref,
               properties(u)['changeReviewEmailArchaMap'] AS archaPref
        """,
        driver=getDriver("userdb"),
        type="dict",
    )
    recipients = []
    for row in rows or []:
        databases = _normalize_userdb_database(row.get("database"))
        if databases and database_key not in databases:
            continue
        if not _change_review_email_preference(row, database_key):
            continue
        email = str(row.get("email") or "").strip()
        if email and email not in recipients:
            recipients.append(email)
    return recipients


def _notify_admins_of_change_review(review):
    if not _change_review_email_delivery_enabled():
        return {"recipients": 0, "result": "Change-review email delivery is paused"}
    recipients = _change_review_admin_recipients(review["database"])
    if not recipients:
        return {"recipients": 0, "result": "No opted-in administrators"}

    body = (
        "A CatMapper change is waiting for review.\n\n"
        f"Request: {review['requestId']}\n"
        f"Database: {review['database']}\n"
        f"Action: {review['action']}\n"
        f"Target CMID: {review['targetCmid']}\n"
        f"Submitted by user: {review['submittedBy']}\n"
        f"Submitted at: {review['submittedAt']}\n\n"
        "Open the Admin page for this database and choose Review proposed changes."
    )
    result = sendEmail(
        mail=mail,
        subject=f"CatMapper change awaiting review: {review['database']} {review['targetCmid']}",
        recipients=recipients,
        body=body,
        sender=get_default_sender() or "admin@catmapper.org",
    )
    return {"recipients": len(recipients), "result": result}


def _create_change_review(database, action, input_payload, tabledata, dataset_id, claims, reason):
    database_key = _change_review_database_key(database)
    access_rows = getQuery(
        query="""
        MATCH (u:USER {userid: $userid})
        RETURN u.database AS database
        """,
        driver=getDriver("userdb"),
        params={"userid": str(claims.get("userid") or "")},
        type="dict",
    )
    if not access_rows:
        raise OwnershipError("User account was not found")
    allowed_databases = _normalize_userdb_database(access_rows[0].get("database"))
    if allowed_databases and database_key not in allowed_databases:
        raise OwnershipError("User is not authorized for this database")

    request_id = f"change_{uuid4().hex}"
    submitted_at = _now_iso()
    safe_input = dict(input_payload or {})
    safe_input.pop("_actorClaims", None)
    safe_tabledata = tabledata if isinstance(tabledata, list) else []
    params = {
        "requestId": request_id,
        "database": database_key,
        "action": str(action or ""),
        "targetCmid": _change_review_target(action, safe_input),
        "submittedBy": str(claims.get("userid") or ""),
        "submittedAt": submitted_at,
        "authorizationReason": str(reason or ""),
        "inputJson": json.dumps(safe_input, sort_keys=True, default=str),
        "tabledataJson": json.dumps(safe_tabledata, sort_keys=True, default=str),
        "datasetID": str(dataset_id or ""),
    }
    rows = getQuery(
        query="""
        MATCH (u:USER {userid: $submittedBy})
        CREATE (r:CHANGE_REVIEW {
          requestId: $requestId,
          database: $database,
          action: $action,
          targetCmid: $targetCmid,
          submittedBy: $submittedBy,
          submittedAt: $submittedAt,
          authorizationReason: $authorizationReason,
          inputJson: $inputJson,
          tabledataJson: $tabledataJson,
          datasetID: $datasetID,
          status: 'pending',
          notifyRequester: false
        })
        CREATE (u)-[:SUBMITTED_CHANGE]->(r)
        RETURN r.requestId AS requestId, r.database AS database, r.action AS action,
               r.targetCmid AS targetCmid, r.submittedBy AS submittedBy,
               coalesce(u.username, u.email, u.userid) AS submitterName,
               r.submittedAt AS submittedAt, r.status AS status,
               r.authorizationReason AS authorizationReason,
               r.inputJson AS inputJson, r.tabledataJson AS tabledataJson,
               r.datasetID AS datasetID, r.notifyRequester AS notifyRequester
        """,
        driver=getDriver("userdb"),
        params=params,
        type="dict",
    )
    if not rows:
        raise ValueError("Unable to save the change for review")
    review = _serialize_change_review(rows[0])
    try:
        review["adminNotification"] = _notify_admins_of_change_review(review)
    except Exception as exc:
        review["adminNotification"] = {"recipients": 0, "result": f"Notification failed: {exc}"}
    return review


def _normalize_userdb_database(value):
    if isinstance(value, list):
        return [
            str(item).strip().lower()
            for item in value
            if str(item).strip()
        ]
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    parts = []
    for chunk in text.replace(",", "|").split("|"):
        cleaned = str(chunk).strip().lower()
        if cleaned:
            parts.append(cleaned)
    return parts


def _join_userdb_database(value):
    return "|".join(_normalize_userdb_database(value))


def _ensure_label_fulltext_index(driver, label_name):
    safe_label = sanitize_cypher_identifier(label_name, "label CMName")
    query = f"""
    CREATE FULLTEXT INDEX {safe_label}
    IF NOT EXISTS
    FOR (n:{safe_label})
    ON EACH [n.normNames]
    """
    getQuery(query=query, driver=driver)


def _password_meets_policy(password):
    if not isinstance(password, str):
        return False
    return len(password) >= 6


def _serialize_user_lookup_row(row):
    databases = row.get("database") if isinstance(row.get("database"), list) else []
    return {
        "userid": str(row.get("userid", "") or ""),
        "first": str(row.get("first", "") or ""),
        "last": str(row.get("last", "") or ""),
        "username": str(row.get("username", "") or ""),
        "email": str(row.get("email", "") or ""),
        "database": "|".join(str(item) for item in databases if str(item).strip()),
        "intendedUse": str(row.get("intendedUse", "") or ""),
        "access": str(row.get("access", "") or ""),
        "role": str(row.get("role", "") or ""),
        "createdAt": row.get("createdAt") or "",
        "updatedAt": row.get("updatedAt") or "",
        "logCount": int(row.get("logCount") or 0),
    }


def _send_admin_password_change_email(target_user, acting_claims):
    email = str(target_user.get("email") or "").strip()
    if not email:
        return None

    acting_label = (
        str(acting_claims.get("username") or "").strip()
        or str(acting_claims.get("userid") or "").strip()
        or "an administrator"
    )
    username = str(target_user.get("username") or "").strip() or str(target_user.get("userid") or "").strip()
    support_email = get_support_email()
    sender = get_default_sender()
    body = (
        "Hello,\n\n"
        f"Your CatMapper password was changed by admin user {acting_label}.\n\n"
        "For security, this email does not include the new password.\n"
        "Use the temporary password shared with you through a secure admin channel to sign in.\n"
        "After signing in, use the Change Password feature in your profile to set a new secure password.\n"
        "If you cannot sign in, use the Forgot Password option on the login page to reset it.\n\n"
        f"Account: {username}\n"
        f"Support: {support_email}\n\n"
        "CatMapper Team"
    )

    return sendEmail(
        mail=mail,
        subject="CatMapper password changed by admin",
        recipients=[email],
        body=body,
        sender=sender,
    )


def _build_activity_stats_for_userids(userids):
    ids = [str(uid).strip() for uid in (userids or []) if str(uid).strip()]
    if not ids:
        return {}

    def summarize(database_name):
        driver = getDriver(database_name)
        query = """
        UNWIND $userids AS uid
        OPTIONAL MATCH (l:LOG)
        WHERE toString(l.user) = toString(uid)
        WITH uid, collect(l) AS logs
        RETURN
          toString(uid) AS userid,
          size(logs) AS totalActions,
          size([x IN logs WHERE toLower(coalesce(x.action, '')) CONTAINS 'created node']) AS createdNodes,
          size([x IN logs WHERE toLower(coalesce(x.action, '')) CONTAINS 'created relationship']) AS createdRelationships,
          size([x IN logs WHERE toLower(coalesce(x.action, '')) CONTAINS 'changed' AND toLower(coalesce(x.action, '')) CONTAINS 'relationship']) AS updatedRelationships,
          size([x IN logs WHERE toLower(coalesce(x.action, '')) CONTAINS 'changed' AND NOT toLower(coalesce(x.action, '')) CONTAINS 'relationship']) AS updatedNodes,
          size([x IN logs WHERE toLower(coalesce(x.action, '')) CONTAINS 'deleted']) AS deletedObjects,
          reduce(lastSeen = '', x IN logs |
            CASE
              WHEN coalesce(x.timestamp, '') > lastSeen THEN coalesce(x.timestamp, '')
              ELSE lastSeen
            END
          ) AS lastActionAt
        """
        rows = getQuery(query, driver=driver, params={"userids": ids}, type="dict")
        out = {}
        for row in rows or []:
            uid = str(row.get("userid", "") or "")
            out[uid] = {
                "totalActions": int(row.get("totalActions") or 0),
                "createdNodes": int(row.get("createdNodes") or 0),
                "createdRelationships": int(row.get("createdRelationships") or 0),
                "updatedNodes": int(row.get("updatedNodes") or 0),
                "updatedRelationships": int(row.get("updatedRelationships") or 0),
                "deletedObjects": int(row.get("deletedObjects") or 0),
                "lastActionAt": row.get("lastActionAt") or "",
            }
        return out

    stats_s = summarize("sociomap")
    stats_a = summarize("archamap")
    combined = {}
    for uid in ids:
        socio = stats_s.get(uid, {
            "totalActions": 0,
            "createdNodes": 0,
            "createdRelationships": 0,
            "updatedNodes": 0,
            "updatedRelationships": 0,
            "deletedObjects": 0,
            "lastActionAt": "",
        })
        archa = stats_a.get(uid, {
            "totalActions": 0,
            "createdNodes": 0,
            "createdRelationships": 0,
            "updatedNodes": 0,
            "updatedRelationships": 0,
            "deletedObjects": 0,
            "lastActionAt": "",
        })
        total = {
            "totalActions": socio["totalActions"] + archa["totalActions"],
            "createdNodes": socio["createdNodes"] + archa["createdNodes"],
            "createdRelationships": socio["createdRelationships"] + archa["createdRelationships"],
            "updatedNodes": socio["updatedNodes"] + archa["updatedNodes"],
            "updatedRelationships": socio["updatedRelationships"] + archa["updatedRelationships"],
            "deletedObjects": socio["deletedObjects"] + archa["deletedObjects"],
            "lastActionAt": max([socio.get("lastActionAt") or "", archa.get("lastActionAt") or ""]),
        }
        combined[uid] = {
            "SocioMap": socio,
            "ArchaMap": archa,
            "total": total,
        }
    return combined


@admin_bp.route('/admin/users/lookup', methods=['POST'])
def admin_user_lookup():
    try:
        data = request.get_json(silent=True) or {}
        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        query_text = str(unlist(data.get("query")) or "").strip()
        limit = unlist(data.get("limit")) if isinstance(data, dict) else None
        try:
            limit = int(limit) if limit is not None else 50
        except Exception:
            limit = 50
        limit = max(1, min(limit, 250))

        driver = getDriver("userdb")
        query = """
        WITH trim(toString($query)) AS q
        MATCH (u:USER)
        WITH
          u,
          q,
          toLower(q) AS ql,
          toLower(coalesce(u.first, '')) AS first_lower,
          toLower(coalesce(u.last, '')) AS last_lower,
          toLower(coalesce(u.username, '')) AS username_lower,
          toLower(coalesce(u.email, '')) AS email_lower
        WHERE q = ''
           OR toString(u.userid) = q
           OR username_lower CONTAINS ql
           OR email_lower CONTAINS ql
           OR first_lower CONTAINS ql
           OR last_lower CONTAINS ql
           OR (first_lower + ' ' + last_lower) CONTAINS ql
        RETURN
          toString(u.userid) AS userid,
          coalesce(u.first, '') AS first,
          coalesce(u.last, '') AS last,
          coalesce(u.username, '') AS username,
          coalesce(u.email, '') AS email,
          coalesce(u.database, []) AS database,
          coalesce(u.intendedUse, '') AS intendedUse,
          coalesce(u.access, '') AS access,
          coalesce(u.role, '') AS role,
          coalesce(u.createdAt, '') AS createdAt,
          coalesce(u.updatedAt, '') AS updatedAt,
          size(coalesce(u.log, [])) AS logCount
        ORDER BY
          toInteger(coalesce(toString(u.userid), '0')) ASC,
          username ASC
        LIMIT $limit
        """
        rows = getQuery(query, driver=driver, params={"query": query_text, "limit": limit}, type="dict")
        serialized = [_serialize_user_lookup_row(row) for row in (rows or [])]
        stats_map = _build_activity_stats_for_userids([row.get("userid") for row in serialized])
        for row in serialized:
            row["updateStats"] = stats_map.get(row["userid"], {
                "SocioMap": {},
                "ArchaMap": {},
                "total": {},
            })
        return jsonify({"users": serialized}), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/users/status-summary', methods=['GET'])
def admin_user_status_summary():
    try:
        verify_request_auth(credentials=None, required_role="admin", req=request)

        driver = getDriver("userdb")
        query = """
        MATCH (u:USER)
        RETURN coalesce(nullif(trim(coalesce(u.access, '')), ''), 'missing') AS status, count(u) AS count
        ORDER BY status
        """
        rows = getQuery(query, driver=driver, type="dict")
        summary = {str(row.get("status") or "missing"): int(row.get("count") or 0) for row in (rows or [])}
        total = sum(summary.values())
        return jsonify({"summary": summary, "totalUsers": total}), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/users/update', methods=['POST'])
def admin_user_update():
    try:
        data = request.get_json(silent=True) or {}
        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        claims = verify_request_auth(credentials=credentials, required_role="admin", req=request)
        acting_userid = str(claims.get("userid") or "")

        userid = str(unlist(data.get("userid")) or "").strip()
        updates = data.get("updates") if isinstance(data, dict) else None
        if not userid:
            raise Exception("userid is required")
        if not isinstance(updates, dict) or not updates:
            raise Exception("updates must be a non-empty object")

        allowed = {"first", "last", "username", "email", "database", "intendedUse", "access", "role", "password"}
        incoming = {str(k): v for k, v in updates.items() if str(k) in allowed}
        if not incoming:
            raise Exception("No editable fields provided")

        driver = getDriver("userdb")
        current_query = """
        MATCH (u:USER {userid: toString($userid)})
        RETURN
          toString(u.userid) AS userid,
          coalesce(u.first, '') AS first,
          coalesce(u.last, '') AS last,
          coalesce(u.username, '') AS username,
          coalesce(u.email, '') AS email,
          coalesce(u.database, []) AS database,
          coalesce(u.intendedUse, '') AS intendedUse,
          coalesce(u.access, '') AS access,
          coalesce(u.role, '') AS role,
          coalesce(u.createdAt, '') AS createdAt,
          coalesce(u.updatedAt, '') AS updatedAt,
          size(coalesce(u.log, [])) AS logCount
        """
        rows = getQuery(current_query, driver=driver, params={"userid": userid}, type="dict")
        if not rows:
            raise Exception("User not found")
        current = rows[0]

        resolved = {
            "first": str(current.get("first") or ""),
            "last": str(current.get("last") or ""),
            "username": str(current.get("username") or ""),
            "email": str(current.get("email") or ""),
            "database": _normalize_userdb_database(current.get("database")),
            "intendedUse": str(current.get("intendedUse") or ""),
            "access": str(current.get("access") or ""),
            "role": str(current.get("role") or ""),
        }

        changed = {}
        for field, value in incoming.items():
            if field == "database":
                new_value = _normalize_userdb_database(value)
            elif field == "password":
                new_value = str(value or "")
                if not _password_meets_policy(new_value):
                    raise Exception("Password must be at least 6 characters")
                changed[field] = {"old": "[hidden]", "new": "[updated]"}
            else:
                new_value = str(value or "").strip()
            if field != "password":
                old_value = resolved.get(field)
                if new_value != old_value:
                    changed[field] = {"old": old_value, "new": new_value}
                    resolved[field] = new_value

        if not changed:
            payload = _serialize_user_lookup_row(current)
            payload["updateStats"] = _build_activity_stats_for_userids([userid]).get(userid, {})
            return jsonify({"message": "No changes detected", "user": payload, "changedFields": []}), 200

        if "username" in changed:
            username_check = """
            MATCH (u:USER)
            WHERE toLower(coalesce(u.username, '')) = toLower($username)
              AND toString(u.userid) <> toString($userid)
            RETURN count(u) AS count
            """
            count_rows = getQuery(username_check, driver=driver, params={"username": resolved["username"], "userid": userid}, type="dict")
            if count_rows and int(count_rows[0].get("count") or 0) > 0:
                raise Exception("Username already exists")

        if "email" in changed:
            email_check = """
            MATCH (u:USER)
            WHERE toLower(coalesce(u.email, '')) = toLower($email)
              AND toString(u.userid) <> toString($userid)
            RETURN count(u) AS count
            """
            count_rows = getQuery(email_check, driver=driver, params={"email": resolved["email"], "userid": userid}, type="dict")
            if count_rows and int(count_rows[0].get("count") or 0) > 0:
                raise Exception("Email already exists")

        timestamp = _now_iso()
        change_bits = []
        for field in sorted(changed.keys()):
            old_value = changed[field]["old"]
            new_value = changed[field]["new"]
            old_text = "|".join(old_value) if isinstance(old_value, list) else str(old_value)
            new_text = "|".join(new_value) if isinstance(new_value, list) else str(new_value)
            change_bits.append(f"{field}: '{old_text}' -> '{new_text}'")
        log_entry = f"{timestamp}: admin {acting_userid} updated user {userid}: " + "; ".join(change_bits)
        password_hash_value = password_hash(str(incoming.get("password") or "")) if "password" in changed else None

        update_query = """
        MATCH (u:USER {userid: toString($userid)})
        SET
          u.first = $first,
          u.last = $last,
          u.username = $username,
          u.email = $email,
          u.database = $database,
          u.intendedUse = $intendedUse,
          u.access = $access,
          u.role = $role,
          u.password = CASE WHEN $passwordProvided THEN $password ELSE u.password END,
          u.passwordLastChangedAt = CASE WHEN $passwordProvided THEN $passwordChangedAt ELSE u.passwordLastChangedAt END,
          u.updatedAt = $updatedAt,
          u.log = coalesce(u.log, []) + $logEntries
        RETURN
          toString(u.userid) AS userid,
          coalesce(u.first, '') AS first,
          coalesce(u.last, '') AS last,
          coalesce(u.username, '') AS username,
          coalesce(u.email, '') AS email,
          coalesce(u.database, []) AS database,
          coalesce(u.intendedUse, '') AS intendedUse,
          coalesce(u.access, '') AS access,
          coalesce(u.role, '') AS role,
          coalesce(u.createdAt, '') AS createdAt,
          coalesce(u.updatedAt, '') AS updatedAt,
          size(coalesce(u.log, [])) AS logCount
        """
        saved_rows = getQuery(
            update_query,
            driver=driver,
            params={
                "userid": userid,
                "first": resolved["first"],
                "last": resolved["last"],
                "username": resolved["username"],
                "email": resolved["email"],
                "database": resolved["database"],
                "intendedUse": resolved["intendedUse"],
                "access": resolved["access"],
                "role": resolved["role"],
                "passwordProvided": "password" in changed,
                "password": password_hash_value,
                "passwordChangedAt": timestamp if "password" in changed else None,
                "updatedAt": timestamp,
                "logEntries": [log_entry],
            },
            type="dict",
        )
        if not saved_rows:
            raise Exception("User not found")

        email_status = None
        if "password" in changed:
            try:
                email_status = _send_admin_password_change_email(current, claims)
            except Exception as email_error:
                email_status = f"Error sending email: {email_error}"

        payload = _serialize_user_lookup_row(saved_rows[0])
        payload["updateStats"] = _build_activity_stats_for_userids([userid]).get(userid, {})
        response_payload = {
            "message": "User updated",
            "user": payload,
            "changedFields": sorted(changed.keys()),
            "logEntry": log_entry,
        }
        if email_status:
            response_payload["emailStatus"] = email_status
        return jsonify(response_payload), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/users/create', methods=['POST'])
def admin_user_create():
    try:
        data = request.get_json(silent=True) or {}
        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        claims = verify_request_auth(credentials=credentials, required_role="admin", req=request)
        acting_userid = str(claims.get("userid") or "")

        username = str(data.get("username") or "").strip()
        first = str(data.get("first") or "").strip()
        last = str(data.get("last") or "").strip()
        email = str(data.get("email") or "").strip()
        database = _normalize_userdb_database(data.get("database"))
        role = str(data.get("role") or "").strip().lower()
        raw_credential = str(data.get("password") or "")

        missing = [
            field for field, value in {
                "username": username,
                "first": first,
                "last": last,
                "email": email,
                "database": database,
                "role": role,
                "password": raw_credential,
            }.items() if not value
        ]
        if missing:
            raise Exception(f"Missing required fields: {', '.join(missing)}")
        if not _password_meets_policy(raw_credential):
            raise Exception("Password must be at least 6 characters")
        if role not in {"user", "admin"}:
            raise Exception("Role must be 'user' or 'admin'")
        if not database or any(value not in {"sociomap", "archamap"} for value in database):
            raise Exception("Database must contain only 'sociomap' or 'archamap'")

        hashed_credential = password_hash(raw_credential)
        if not isinstance(hashed_credential, str) or hashed_credential.startswith("password hash failed"):
            raise Exception("Unable to process password")

        driver = getDriver("userdb")
        duplicate_query = """
        MATCH (u:USER)
        WHERE toLower(coalesce(u.username, '')) = toLower($username)
           OR toLower(coalesce(u.email, '')) = toLower($email)
        RETURN
          any(row IN collect(u) WHERE toLower(coalesce(row.username, '')) = toLower($username)) AS usernameExists,
          any(row IN collect(u) WHERE toLower(coalesce(row.email, '')) = toLower($email)) AS emailExists
        """
        duplicate_rows = getQuery(
            duplicate_query,
            driver=driver,
            params={"username": username, "email": email},
            type="dict",
        )
        duplicate = duplicate_rows[0] if duplicate_rows else {}
        if duplicate.get("usernameExists"):
            raise Exception("Username already exists")
        if duplicate.get("emailExists"):
            raise Exception("Email already exists")

        timestamp = _now_iso()
        log_entry = f"{timestamp}: admin {acting_userid} created user"
        create_query = """
        MATCH (p:USER)
        WITH coalesce(max(toInteger(p.userid)), 0) + 1 AS id
        CREATE (u:USER {
          userid: toString(id),
          username: $username,
          first: $first,
          last: $last,
          email: $email,
          database: $database,
          intendedUse: $intendedUse,
          access: 'enabled',
          role: $role,
          password: $password,
          createdAt: $timestamp,
          updatedAt: $timestamp,
          passwordLastChangedAt: $timestamp,
          log: [$logEntry]
        })
        RETURN
          toString(u.userid) AS userid,
          u.first AS first,
          u.last AS last,
          u.username AS username,
          u.email AS email,
          u.database AS database,
          u.intendedUse AS intendedUse,
          u.access AS access,
          u.role AS role,
          u.createdAt AS createdAt,
          u.updatedAt AS updatedAt,
          size(u.log) AS logCount
        """
        created_rows = getQuery(
            create_query,
            driver=driver,
            params={
                "username": username,
                "first": first,
                "last": last,
                "email": email,
                "database": database,
                "intendedUse": "Created by administrator",
                "role": role,
                "password": hashed_credential,
                "timestamp": timestamp,
                "logEntry": log_entry,
            },
            type="dict",
        )
        if not created_rows:
            raise Exception("Unable to create user")

        return jsonify({
            "message": "User created",
            "user": _serialize_user_lookup_row(created_rows[0]),
        }), 201
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route("/admin_add_edit_delete_nodeproperties", methods=['GET'])
def admin_nodeproperties():
    CMID = request.args.get('CMID')
    database = request.args.get('database')
    option = request.args.get('option')
    credentials = _parse_credentials(request.args.get("cred"))
    try:
        verify_request_auth(credentials=credentials, req=request)
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message, "r": {}, "r1": []}), status_code

    driver = getDriver(database)

    # q captures the actual properties of a node
    q = "MATCH (n) WHERE n.CMID = $cmid return properties(n) AS props"

    # q1 captures relevant properties of node
    if "CP" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND coalesce(p.internal, false) = false AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'PROPERTY' RETURN p.CMName as property"
    elif "CL" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND coalesce(p.internal, false) = false AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'LABEL' RETURN p.CMName as property"
    elif "D" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND coalesce(p.internal, false) = false AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'DATASET' RETURN p.CMName as property"
    else:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND coalesce(p.internal, false) = false AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'CATEGORY' RETURN p.CMName as property"

    with driver.session() as session:
        r = session.run(q, cmid=CMID).data()

        if r == []:
            return jsonify({"error": "Invalid CMID"})
        node_rows = session.run(
            "MATCH (n {CMID: $cmid}) RETURN labels(n) AS labels",
            cmid=CMID,
        ).data()
        labels = set(node_rows[0].get("labels") or []) if node_rows else set()
        if "DELETED" in labels:
            return jsonify({"error": f"{CMID} is a deleted node and cannot be edited."})

        props = [k for k in r[0]['props'].keys()] if r else []

        # Run q1 to get allowed properties
        allowed = session.run(q1).data()
        allowed_props = {
            row['property']
            for row in allowed
            if node_property_allowed_for_labels(row.get('property'), labels, driver)
        }

        r = {k: v for k, v in r[0]['props'].items() if k in allowed_props}

        if option != "add" and r == {}:
            return jsonify({"error": "No editable features on this node."})

        # Filter props to only include allowed keys
        r1 = [k for k in allowed_props if k not in props]

    return jsonify({
        "r": r,
        "r1": r1,
        "error": ""
    })


@admin_bp.route("/admin_add_edit_delete_usesproperties", methods=['GET'])
def admin_usesproperties():
    CMID = request.args.get('CMID')
    database = request.args.get('database')
    func = request.args.get("func")
    credentials = _parse_credentials(request.args.get("cred"))
    try:
        claims = normalize_actor_claims(verify_request_auth(credentials=credentials, req=request))
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message, "r": [], "r1": []}), status_code

    driver = getDriver(database)

    q = """
    MATCH (n:CATEGORY)<-[r:USES]-(d:DATASET)
    WHERE n.CMID = $cmid
    RETURN {CMName: n.CMName, CMID: n.CMID, elementId: elementId(n), labels: labels(n)} AS n, r, d
    """
    
    q1 = """
    MATCH (p:PROPERTY)
    WHERE p.type='relationship'
      AND coalesce(p.internal, false) = false
    RETURN p.CMName as property, p.groupLabel as groupLabel, p.relationship as relationship,
           p.reltype as reltype
    """

    with driver.session() as session:
        result = session.run(q, cmid=CMID)

        records_list = []
        temp_list = []
        for record in result:
            n = dict(record["n"].items())
            r = dict(record["r"].items())
            r["id"] = record["r"].element_id
            d = dict(record["d"].items())
            temp_list.append((n, r, d))
        
        temp_list.sort(key=lambda x: (x[2].get("CMName", ""), x[1].get("Key", "")))
        records_list.extend(temp_list)

        if not records_list:
            node_rows = session.run(
                "MATCH (n {CMID: $cmid}) RETURN n.CMID AS CMID, n.CMName AS CMName, labels(n) AS labels",
                cmid=CMID,
            ).data()
            if not node_rows:
                return jsonify({"error": "Invalid CMID", "r": [], "r1": []})

            labels = set(node_rows[0].get("labels") or [])
            if "DELETED" in labels:
                return jsonify({
                    "error": f"{CMID} is a deleted node and has no editable USES ties.",
                    "r": [],
                    "r1": [],
                })
            if "DATASET" in labels:
                return jsonify({
                    "error": (
                        "USES properties belong to category nodes. "
                        "Use add/edit/delete node property to edit dataset parent values."
                    ),
                    "r": [],
                    "r1": [],
                })

            return jsonify({
                "error": f"No USES ties found for {CMID}.",
                "r": [],
                "r1": [],
            })

        category_labels = records_list[0][0].get("labels", []) if records_list else []
        allowed = session.run(q1).data()
        category_domains = None
        category_group_label = None
        allowed_props = []
        for row in allowed:
            property_name = row.get('property')
            if not property_name:
                continue
            if not _admin_uses_property_addable(property_name):
                continue
            if not _admin_uses_property_reltype_addable(row.get('reltype')):
                continue

            allowed_domains = get_node_property_domain_restriction(property_name)
            if allowed_domains:
                if category_domains is None:
                    category_domains = resolve_domains_from_node_labels(category_labels, driver)
                if not category_domains.intersection(allowed_domains):
                    continue

            if uses_contextual_property_needs_category_group(
                property_name,
                row.get('groupLabel'),
                row.get('relationship'),
            ):
                if category_group_label is None:
                    category_group_label = get_uses_contextual_category_group(CMID, driver)
                if not uses_contextual_property_allowed_for_group(
                    category_group_label,
                    property_name,
                    row.get('groupLabel'),
                    row.get('relationship'),
                ):
                    continue

            allowed_props.append(property_name)

        allowed_props = sorted(set(allowed_props))
        
    return {
        "r": records_list,
        "r1": allowed_props,
        "error": ""
    }


@admin_bp.route("/admin_add_edit_delete_category_merging_properties", methods=['GET'])
def admin_category_merging_properties():
    CMID = request.args.get('CMID')
    database = request.args.get('database')
    credentials = _parse_credentials(request.args.get("cred"))
    try:
        verify_request_auth(credentials=credentials, required_role="admin", req=request)
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message, "r": [], "r1": []}), status_code

    driver = getDriver(database)

    q = """
        MATCH (d:DATASET)-[r:MERGING]->(n:CATEGORY {CMID: $cmid})
        RETURN {CMName: n.CMName, CMID: n.CMID, elementId: elementId(n)} AS n, r,
               {CMName: d.CMName, CMID: d.CMID, elementId: elementId(d)} AS d
    """

    allowed_props = ["stack", "Key"]

    with driver.session() as session:
        result = session.run(q, cmid=CMID)

        records_list = []
        temp_list = []
        for record in result:
            n = dict(record["n"].items())
            r = dict(record["r"].items())
            r["id"] = record["r"].element_id
            d = dict(record["d"].items())
            temp_list.append((n, r, d))

        temp_list.sort(
            key=lambda x: (
                x[2].get("CMName", ""),
                x[1].get("stack", ""),
                x[1].get("Key", ""),
            )
        )
        records_list.extend(temp_list)

    return {
        "r": records_list,
        "r1": allowed_props,
        "error": ""
    }



@admin_bp.route('/create_label_helper', methods=['GET'])
def create_label():
    database = request.args.get('database')
    driver = getDriver(database)

    q = "MATCH (p:LABEL) WHERE p.groupLabel=p.CMName RETURN p.CMName"

    with driver.session() as session:
        result = session.run(q)

        values = [record["p.CMName"] for record in result]

        final_values = [v for v in values if v not in (
            "ALL NODES", "ANY DOMAIN")]

    return {"res": final_values}


@admin_bp.route('/admin/nodeSummary', methods=['GET'])
def admin_node_summary():
    try:
        cmid = request.args.get('CMID')
        database = request.args.get('database')
        if not cmid:
            return jsonify({"error": "CMID is required"}), 400
        if not database:
            return jsonify({"error": "Database is required"}), 400

        driver = getDriver(database)
        summary = getNodeMergeSummary(cmid, driver)
        return jsonify(summary), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@admin_bp.route('/check_ambiguous_usesties', methods=['POST'])
def check_ambiguous_usesties():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get('database'))
        credentials = unlist(data.get("cred"))
        input = unlist(data.get("input"))
        CMID_from = input.get('s1_2')
        CMID_to = input.get('s1_3')
        USES_property = json.loads(input.get('s1_7'))
        rel_id = USES_property[1]["id"]
        driver = getDriver(database)
        verify_request_auth(credentials=credentials, req=request)

        result = check_ambiguous_ties_moveUSESties(driver,CMID_from,CMID_to,rel_id)
        return result
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code

@admin_bp.route('/admin', methods=['GET'])
def getAdmin():
    """
    Retrieve the 'admin.html' template and return it as a response.

    Returns:
    - Response: A Flask response containing the 'admin.html' template.

    Example:
    ```python
    from flask import Flask

    app = Flask(__name__)

    @admin_bp.route('/admin')
    def admin_route():
        return getAdmin()
    ```
    """
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('admin.html'), 200, headers)


def _execute_admin_edit(database, fun, acting_user, input_payload, data):
    result = "Nothing returned"
    if fun == "mergeNodes":
        keepcmid = unlist(data.get('keepcmid').strip())
        deletecmid = unlist(data.get('deletecmid').strip())
        result = mergeNodes(keepcmid, deletecmid, acting_user, database)
    elif fun == "processUSES":
        CMID = cleanCMID(data.get('CMID'))
        result = processUSES(database=database, CMID=CMID)
    elif fun == "replaceProperty":
        cmid = unlist(data.get('cmid'))
        property = unlist(data.get('property'))
        old = unlist(data.get('old'))
        new = unlist(data.get('new'))
        result = replaceProperty(cmid, property, old, new, database)
    elif fun == "add/edit/delete node property":
        result = add_edit_delete_Node(database, acting_user, input_payload)
    elif fun == "add/edit/delete USES property":
        result = add_edit_delete_USES(database, acting_user, input_payload)
    elif fun == "add/edit/delete CATEGORY MERGING property":
        result = add_edit_delete_CATEGORY_MERGING(database, acting_user, input_payload)
    elif fun == "merge nodes":
        result = mergeNodes(input_payload.get('s1_2'), input_payload.get('s1_3'), acting_user, database)
    elif fun == "create new label":
        result = createLabel(database, acting_user, input_payload)
    elif fun == "delete node":
        result = deleteNode(database, acting_user, input_payload)
    elif fun == "delete USES relation":
        result = deleteUSES(database, acting_user, input_payload)
    elif fun == "delete CATEGORY MERGING relation":
        result = deleteCATEGORYMERGING(database, acting_user, input_payload)
    elif fun == "move USES tie":
        result = moveUSESties(
            database,
            acting_user,
            input_payload,
            data.get("datasetID"),
            data.get("tabledata"),
        )
    elif fun == "move CATEGORY MERGING tie":
        result = moveCATEGORYMERGINGties(database, acting_user, input_payload)
    else:
        raise Exception("Function does not exist")
    return result


def _load_change_review(request_id):
    rows = getQuery(
        query="""
        MATCH (r:CHANGE_REVIEW {requestId: $requestId})
        OPTIONAL MATCH (u:USER {userid: r.submittedBy})
        RETURN r.requestId AS requestId, r.database AS database, r.action AS action,
               r.targetCmid AS targetCmid, r.submittedBy AS submittedBy,
               coalesce(u.username, u.email, u.userid) AS submitterName,
               r.submittedAt AS submittedAt, r.status AS status,
               r.authorizationReason AS authorizationReason,
               r.inputJson AS inputJson, r.tabledataJson AS tabledataJson,
               r.datasetID AS datasetID, coalesce(r.notifyRequester, false) AS notifyRequester,
               r.decisionNote AS decisionNote, r.decidedBy AS decidedBy,
               r.decidedAt AS decidedAt, r.startedAt AS startedAt,
               r.backgroundJobId AS backgroundJobId, r.lastError AS lastError
        """,
        driver=getDriver("userdb"),
        params={"requestId": str(request_id)},
        type="dict",
    )
    return _serialize_change_review(rows[0]) if rows else None


@admin_bp.route('/admin/change-reviews', methods=['GET'])
def list_change_reviews():
    try:
        verify_request_auth(required_role="admin", req=request)
        database = _change_review_database_key(request.args.get("database"))
        status = str(request.args.get("status") or "pending").strip().lower()
        if status not in {"pending", "processing", "approved", "rejected", "open", "all"}:
            raise ValueError("Invalid review status")
        rows = getQuery(
            query="""
            MATCH (r:CHANGE_REVIEW {database: $database})
            WHERE $status = 'all'
               OR ($status = 'open' AND r.status IN ['pending', 'processing'])
               OR r.status = $status
            OPTIONAL MATCH (u:USER {userid: r.submittedBy})
            RETURN r.requestId AS requestId, r.database AS database, r.action AS action,
                   r.targetCmid AS targetCmid, r.submittedBy AS submittedBy,
                   coalesce(u.username, u.email, u.userid) AS submitterName,
                   r.submittedAt AS submittedAt, r.status AS status,
                   r.authorizationReason AS authorizationReason,
                   r.inputJson AS inputJson, r.tabledataJson AS tabledataJson,
                   r.datasetID AS datasetID, coalesce(r.notifyRequester, false) AS notifyRequester,
                   r.decisionNote AS decisionNote, r.decidedBy AS decidedBy,
                   r.decidedAt AS decidedAt, r.startedAt AS startedAt,
                   r.backgroundJobId AS backgroundJobId, r.lastError AS lastError
            ORDER BY r.submittedAt ASC
            """,
            driver=getDriver("userdb"),
            params={"database": database, "status": status},
            type="dict",
        )
        reviews = [_serialize_change_review(row) for row in (rows or [])]
        return jsonify({"reviews": reviews, "count": len(reviews)}), 200
    except Exception as exc:
        message = str(exc)
        return jsonify({"error": message}), classify_auth_error_status(message) or 400


@admin_bp.route('/admin/change-reviews/<request_id>/notification', methods=['PATCH'])
def update_change_review_requester_notification(request_id):
    try:
        claims = normalize_actor_claims(verify_request_auth(req=request))
        payload = request.get_json(silent=True) or {}
        notify = payload.get("notifyRequester")
        if not isinstance(notify, bool):
            raise ValueError("notifyRequester must be true or false")
        rows = getQuery(
            query="""
            MATCH (r:CHANGE_REVIEW {requestId: $requestId, submittedBy: $userid})
            WHERE r.status = 'pending'
            SET r.notifyRequester = $notifyRequester
            RETURN r.requestId AS requestId
            """,
            driver=getDriver("userdb"),
            params={
                "requestId": str(request_id),
                "userid": str(claims.get("userid") or ""),
                "notifyRequester": notify,
            },
            type="dict",
        )
        if not rows:
            raise ValueError("Pending change review request not found")
        return jsonify({
            "message": "Approval email preference saved.",
            "requestId": str(request_id),
            "notifyRequester": notify,
        }), 200
    except Exception as exc:
        message = str(exc)
        return jsonify({"error": message}), classify_auth_error_status(message) or 400


def _send_change_review_decision_email(review, decision):
    decision_key = str(decision or "").strip().lower()
    if decision_key not in {"approved", "rejected"}:
        raise ValueError("Change-review email decision must be approved or rejected")
    if not _change_review_email_delivery_enabled():
        return None
    if decision_key == "approved" and not review.get("notifyRequester"):
        return None
    rows = getQuery(
        query="""
        MATCH (u:USER {userid: $userid})
        RETURN trim(coalesce(u.email, '')) AS email
        """,
        driver=getDriver("userdb"),
        params={"userid": review.get("submittedBy")},
        type="dict",
    )
    email = str((rows[0] if rows else {}).get("email") or "").strip()
    if not email:
        return None
    decision_note = str(review.get("decisionNote") or "").strip()
    note_block = f"\nReviewer comment: {decision_note}\n" if decision_note else ""
    return sendEmail(
        mail=mail,
        subject=f"Your CatMapper change was {decision_key}: {review.get('targetCmid')}",
        recipients=[email],
        body=(
            f"Your submitted CatMapper change has been {decision_key}.\n\n"
            f"Request: {review.get('requestId')}\n"
            f"Database: {review.get('database')}\n"
            f"Action: {review.get('action')}\n"
            f"Target CMID: {review.get('targetCmid')}\n"
            f"{note_block}"
        ),
        sender=get_default_sender() or "admin@catmapper.org",
    )


def _send_change_review_approval_email(review):
    return _send_change_review_decision_email(review, "approved")


def _send_change_review_rejection_email(review):
    # Rejection notices are always sent when delivery is enabled; they are not
    # governed by the requester's optional approval-notification preference.
    return _send_change_review_decision_email(review, "rejected")


def _finalize_change_review_approval(request_id, actor_claims):
    """Apply one claimed review and run its existing integrity reconciliation."""
    review = _load_change_review(request_id)
    if not review or review.get("status") != "processing":
        return {"skipped": True, "reason": "Review request is no longer processing."}

    getQuery(
        query="""
        MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'processing'})
        SET r.startedAt = $startedAt
        RETURN r.requestId AS requestId
        """,
        driver=getDriver("userdb"),
        params={"requestId": str(request_id), "startedAt": _now_iso()},
        type="dict",
    )

    claims = normalize_actor_claims(actor_claims or {})
    if not is_admin_claims(claims):
        raise OwnershipError("Only an administrator can finalize a change review")

    input_payload = dict(review.get("input") or {})
    input_payload["_actorClaims"] = dict(claims)
    action_data = {
        "tabledata": review.get("tabledata") or [],
        "datasetID": review.get("datasetID") or "",
    }
    try:
        result = _execute_admin_edit(
            review["database"],
            review["action"],
            claims.get("userid"),
            input_payload,
            action_data,
        )
    except Exception as execute_error:
        getQuery(
            query="""
            MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'processing'})
            SET r.status = 'pending', r.lastError = $lastError
            REMOVE r.startedAt, r.backgroundJobId
            RETURN r.requestId AS requestId
            """,
            driver=getDriver("userdb"),
            params={"requestId": str(request_id), "lastError": str(execute_error)},
            type="dict",
        )
        raise

    getQuery(
        query="""
        MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'processing'})
        SET r.status = 'approved', r.appliedResult = $appliedResult,
            r.completedAt = $completedAt
        REMOVE r.lastError
        RETURN r.requestId AS requestId
        """,
        driver=getDriver("userdb"),
        params={
            "requestId": str(request_id),
            "appliedResult": str(result),
            "completedAt": _now_iso(),
        },
        type="dict",
    )
    approved_review = _load_change_review(request_id)
    try:
        email_result = _send_change_review_approval_email(approved_review)
    except Exception as email_error:
        email_result = f"Notification failed: {email_error}"
    return {
        "review": approved_review,
        "result": str(result),
        "requesterEmailResult": email_result,
    }


def run_change_review_approval_job(request_id, actor_claims):
    """RQ entry point; initialize Flask extensions before sending any email."""
    from app import app

    with app.app_context():
        return _finalize_change_review_approval(request_id, actor_claims)


@admin_bp.route('/admin/change-reviews/<request_id>/decision', methods=['POST'])
def decide_change_review(request_id):
    try:
        claims = normalize_actor_claims(verify_request_auth(required_role="admin", req=request))
        payload = request.get_json(silent=True) or {}
        decision = str(payload.get("decision") or "").strip().lower()
        note = str(payload.get("note") or "").strip()
        if decision not in {"approve", "reject"}:
            raise ValueError("Decision must be approve or reject")
        if len(note) > 2000:
            raise ValueError("Decision comment must be 2000 characters or fewer")

        review = _load_change_review(request_id)
        if not review or review.get("status") != "pending":
            raise ValueError("Pending change review request not found")
        now = _now_iso()

        if decision == "reject":
            rows = getQuery(
                query="""
                MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'pending'})
                SET r.status = 'rejected', r.decisionNote = $note,
                    r.decidedBy = $decidedBy, r.decidedAt = $decidedAt
                RETURN r.requestId AS requestId
                """,
                driver=getDriver("userdb"),
                params={
                    "requestId": str(request_id),
                    "note": note,
                    "decidedBy": str(claims.get("userid") or ""),
                    "decidedAt": now,
                },
                type="dict",
            )
            if not rows:
                raise ValueError("The review request was already decided")
            rejected_review = _load_change_review(request_id)
            try:
                email_result = _send_change_review_rejection_email(rejected_review)
            except Exception as email_error:
                email_result = f"Notification failed: {email_error}"
            return jsonify({
                "message": "Change rejected and requester notified.",
                "review": rejected_review,
                "requesterEmailResult": email_result,
            }), 200

        claimed = getQuery(
            query="""
            MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'pending'})
            SET r.status = 'processing', r.decidedBy = $decidedBy, r.decidedAt = $decidedAt,
                r.decisionNote = $note, r.startedAt = '', r.backgroundJobId = ''
            RETURN r.requestId AS requestId
            """,
            driver=getDriver("userdb"),
            params={
                "requestId": str(request_id),
                "decidedBy": str(claims.get("userid") or ""),
                "decidedAt": now,
                "note": note,
            },
            type="dict",
        )
        if not claimed:
            raise ValueError("The review request was already decided")

        actor_claims = {
            "userid": str(claims.get("userid") or ""),
            "role": "admin",
        }
        try:
            if is_rq_enabled():
                job = enqueue_change_review_approval(str(request_id), actor_claims)
                job_id = str(getattr(job, "id", "") or "")
                if not job_id:
                    raise RuntimeError("Change-review job was not accepted by the queue")
                getQuery(
                    query="""
                    MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'processing'})
                    SET r.backgroundJobId = $jobId
                    RETURN r.requestId AS requestId
                    """,
                    driver=getDriver("userdb"),
                    params={"requestId": str(request_id), "jobId": job_id},
                    type="dict",
                )
                return jsonify({
                    "message": "Approval started. The change is being finalized.",
                    "review": _load_change_review(request_id),
                    "queued": True,
                }), 202

            # Local development without RQ retains the old synchronous behavior.
            completed = _finalize_change_review_approval(request_id, actor_claims)
            return jsonify({
                "message": "Change approved and applied.",
                **completed,
                "queued": False,
            }), 200
        except Exception as enqueue_error:
            getQuery(
                query="""
                MATCH (r:CHANGE_REVIEW {requestId: $requestId, status: 'processing'})
                SET r.status = 'pending', r.lastError = $lastError
                REMOVE r.startedAt, r.backgroundJobId
                RETURN r.requestId AS requestId
                """,
                driver=getDriver("userdb"),
                params={"requestId": str(request_id), "lastError": str(enqueue_error)},
                type="dict",
            )
            raise
    except Exception as exc:
        message = str(exc)
        return jsonify({"error": message}), classify_auth_error_status(message) or 400


@admin_bp.route('/admin/change-review-preferences', methods=['GET', 'PATCH'])
def change_review_preferences():
    try:
        claims = normalize_actor_claims(verify_request_auth(required_role="admin", req=request))
        userid = str(claims.get("userid") or "")
        driver = getDriver("userdb")
        if request.method == 'PATCH':
            payload = request.get_json(silent=True) or {}
            database = _change_review_database_key(payload.get("database"))
            enabled = payload.get("enabled")
            if not isinstance(enabled, bool):
                raise ValueError("enabled must be true or false")
            property_name = "changeReviewEmailSocioMap" if database == "sociomap" else "changeReviewEmailArchaMap"
            query = f"""
            MATCH (u:USER {{userid: $userid}})
            SET u.{property_name} = $enabled
            RETURN u.email AS email,
                   properties(u)['changeReviewEmailSocioMap'] AS socioPref,
                   properties(u)['changeReviewEmailArchaMap'] AS archaPref
            """
            rows = getQuery(
                query=query,
                driver=driver,
                params={"userid": userid, "enabled": enabled},
                type="dict",
            )
        else:
            rows = getQuery(
                query="""
                MATCH (u:USER {userid: $userid})
                RETURN u.email AS email,
                       properties(u)['changeReviewEmailSocioMap'] AS socioPref,
                       properties(u)['changeReviewEmailArchaMap'] AS archaPref
                """,
                driver=driver,
                params={"userid": userid},
                type="dict",
            )
        if not rows:
            raise ValueError("Admin user not found")
        row = rows[0]
        return jsonify({
            "sociomap": _change_review_email_preference(row, "sociomap"),
            "archamap": _change_review_email_preference(row, "archamap"),
            "deliveryEnabled": _change_review_email_delivery_enabled(),
        }), 200
    except Exception as exc:
        message = str(exc)
        return jsonify({"error": message}), classify_auth_error_status(message) or 400


@admin_bp.route('/admin/edit', methods=['GET', 'POST'])
def getAdminEdit():
    from configparser import ConfigParser
    config = ConfigParser()
    config.read('config.ini')
    apikeyEnv = config.get('DB', 'apikey', fallback=None)
    # will not be documented in swagger at this point
    try:
        if request.method == 'GET':
            data = request.args
        elif request.method == "POST":
            data = request.get_data()
            data = json.loads(data)
        else:
            raise Exception("invalid request method")
        database = unlist(data.get('database'))
        if database is None:
            raise Exception("Database not specified")
        fun = unlist(data.get('fun'))
        user = unlist(data.get('user'))
        pwd = unlist(data.get('pwd'))
        apikey = unlist(data.get('apikey'))
        credentials = _parse_credentials(data.get("cred"))
        input = unlist(data.get("input"))
        claims = None
        auth_header = request.headers.get("Authorization", "")
        request_api_key = request.headers.get("X-API-Key", "").strip()
        auth_lower = auth_header.lower()
        has_api_key_auth = bool(request_api_key) or auth_lower.startswith("apikey ") or auth_lower.startswith("api-key ")
        if credentials or auth_header.startswith("Bearer ") or has_api_key_auth:
            claims = normalize_actor_claims(verify_request_auth(credentials=credentials, req=request))
        else:
            validated = False
            if apikeyEnv and apikey and apikey == apikeyEnv:
                validated = True
                claims = {"userid": str(user or "legacy-admin"), "role": "admin"}
            if not validated:
                credentials = login(user, pwd)
                if isinstance(credentials, dict) and credentials.get('role') == "admin":
                    validated = True
                    claims = {
                        "userid": str(credentials.get("userid") or user or "legacy-admin"),
                        "role": "admin",
                    }
            if not validated:
                raise Exception("User not authorized")
        claims = normalize_actor_claims(claims)
        acting_user = claims.get("userid")
        if not acting_user:
            acting_user = user
        if not isinstance(input, dict):
            input = {}
        input = dict(input)
        try:
            _authorize_admin_edit_function(
                fun=fun,
                database=database,
                input_payload=input,
                tabledata=data.get("tabledata"),
                dataset_id=data.get("datasetID"),
                claims=claims,
            )
        except (OwnerScopedAdminReviewRequired, OwnershipError) as authorization_error:
            if is_admin_claims(claims):
                raise
            review = _create_change_review(
                database=database,
                action=fun,
                input_payload=input,
                tabledata=data.get("tabledata"),
                dataset_id=data.get("datasetID"),
                claims=claims,
                reason=str(authorization_error),
            )
            return jsonify({
                "message": "Your changes have been submitted for review.",
                "submittedForReview": True,
                "review": review,
            }), 202

        input["_actorClaims"] = dict(claims)
        result = _execute_admin_edit(database, fun, acting_user, input, data)
        return result
    except OwnerScopedAdminReviewRequired as e:
        if claims and not is_admin_claims(claims):
            review = _create_change_review(
                database=database,
                action=fun,
                input_payload=input,
                tabledata=data.get("tabledata"),
                dataset_id=data.get("datasetID"),
                claims=claims,
                reason=str(e),
            )
            return jsonify({
                "message": "Your changes have been submitted for review.",
                "submittedForReview": True,
                "review": review,
            }), 202
        return jsonify({"error": str(e)}), 403
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)
        status_code = classify_auth_error_status(data) or 500
        return data, status_code


@admin_bp.route('/createNodes', methods=['POST'])
def createNodesapi():
    try:
        import pandas as pd
        data = request.get_data()
        data = json.loads(data)
        df = data.get('df')
        database = unlist(data.get('database'))
        user = unlist(data.get('user'))
        pwd = unlist(data.get('password'))
        credentials = {"userid": user, "key": pwd}
        claims = verify_request_auth(credentials=credentials, req=request)
        acting_user = claims.get("userid")

        if not df or len(df) == 0:
            return jsonify({"error": "Data is empty"}), 400

        df = pd.DataFrame(df)

        results = createNodes(df, database, acting_user)

        return results

    except Exception as e:
        result = str(e)
        return result, 500
    
@admin_bp.route('/updateWaitingUSES', methods=['POST'])
def getUpdateWaitingUSES():
    try:
        data = request.get_json(silent=True)
        if data is None:
            data = {}
        credentials = unlist(data.get("cred")) if isinstance(data, dict) else None
        claims = verify_request_auth(credentials=credentials, req=request)
        acting_user = claims.get("userid")

        requested_user = unlist(data.get("user")) if isinstance(data, dict) else None
        if requested_user and str(requested_user).strip() != str(acting_user):
            raise Exception("User does not match authenticated API key/token owner")

        database = unlist(data.get("database")) if isinstance(data, dict) else None
        if not database:
            raise Exception("Database not specified")

        result = waitingUSES(database)
        return result
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code

@admin_bp.route('/mergeUSESties', methods=['GET','POST'])
def getMergeUSESties():
    try:
        if request.method == 'GET':
            database = request.args.get('database')
            CMID = request.args.get('CMID')
            Key = request.args.get('Key')
            datasetID = request.args.get('datasetID')
            result = mergeUSESties(database, CMID, Key, datasetID)
            return jsonify(result)

        data = request.get_json(silent=True)
        if data is None:
            data = json.loads(request.get_data() or "{}")

        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        database = data.get("database")
        rows = data.get("rows")
        if isinstance(rows, list):
            merged = []
            failed = []
            for row in rows:
                row_context = {
                    "CMID": row.get("CMID"),
                    "Key": row.get("Key"),
                    "datasetID": row.get("datasetID"),
                }
                try:
                    merged.append(mergeUSESties(
                        database,
                        row_context["CMID"],
                        row_context["Key"],
                        row_context["datasetID"],
                    ))
                except Exception as merge_error:
                    failed.append({
                        **row_context,
                        "error": str(merge_error),
                        "details": getattr(merge_error, "details", None),
                    })
            return jsonify({
                "ok": not failed,
                "merged": merged,
                "failed": failed,
                "count": len(merged),
            }), 200

        result = mergeUSESties(
            database,
            data.get("CMID"),
            data.get("Key"),
            data.get("datasetID"),
        )
        return jsonify(result)
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code

@admin_bp.route('/admin/saveMetadata', methods=['POST'])
def saveMetadata():
    try:
        data = request.get_json(silent=True)
        if data is None:
            data = {}
        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        updates = data if isinstance(data, list) else data.get("updates")
        if not isinstance(updates, list):
            raise Exception("Invalid payload: 'updates' must be a list")

        # 1. Initialize separate lists for each database
        updatesS = []
        updatesA = []

        for item in updates:
            if not isinstance(item, dict):
                raise Exception("Invalid update item: each update must be an object")

            node_id = item.get('id')
            props = item.get('properties', {})
            db_target = item.get('database')  # Check which DB this item belongs to

            if not node_id or not isinstance(node_id, str):
                raise Exception("Invalid update item: missing or invalid node id")
            if db_target not in {"SocioMap", "ArchaMap"}:
                raise Exception(f"Invalid database target '{db_target}'")
            if not isinstance(props, dict):
                raise Exception("Invalid update item: properties must be an object")

            # Clean properties
            clean_props = props.copy()
            clean_props.pop('CMID', None)
            clean_props.pop('id', None)
            clean_props.pop('labels', None)
            clean_props.pop('database', None)

            # Create the update object
            update_packet = {
                "id": node_id,
                "props": clean_props
            }

            # 2. Sort into the correct list
            if db_target == "SocioMap":
                updatesS.append(update_packet)
            elif db_target == "ArchaMap":
                updatesA.append(update_packet)

        # 3. Define the Query (Same for both)
        query = """
        UNWIND $updates AS item
        MATCH (n:METADATA)
        WHERE elementId(n) = item.id
        SET n += item.props
        RETURN count(n) as updated_count
        """

        # 4. Execute conditionally based on lists
        total_count = 0

        def extract_updated_count(result):
            if result is None:
                return 0
            # getQuery(..., type="list") may return:
            # - [{'updated_count': N}]
            # - [N]
            # - N
            if isinstance(result, list):
                if not result:
                    return 0
                first = result[0]
                if isinstance(first, dict):
                    return int(first.get('updated_count', 0) or 0)
                if isinstance(first, (int, float)):
                    return int(first)
                return 0
            if isinstance(result, dict):
                return int(result.get('updated_count', 0) or 0)
            if isinstance(result, (int, float)):
                return int(result)
            return 0

        # Only run SocioMap query if we have SocioMap updates
        if updatesS:
            driverS = getDriver("sociomap")
            resultS = getQuery(query=query, driver=driverS, params={"updates": updatesS}, type="list")
            total_count += extract_updated_count(resultS)

        # Only run ArchaMap query if we have ArchaMap updates
        if updatesA:
            driverA = getDriver("archamap")
            resultA = getQuery(query=query, driver=driverA, params={"updates": updatesA}, type="list")
            total_count += extract_updated_count(resultA)

        if total_count:
            clear_metadata_caches()

        return jsonify({
            "message": f"Updated {total_count} nodes.",
            "updatedCount": total_count,
            "byDatabase": {
                "SocioMap": len(updatesS),
                "ArchaMap": len(updatesA)
            }
        }), 200

    except Exception as e:
        error_message = str(e)
        print(f"Error saving metadata: {error_message}")
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/metadata/create', methods=['POST'])
def create_metadata_node():
    try:
        data = request.get_json(silent=True)
        if data is None:
            data = {}

        credentials = _parse_credentials(data.get("cred")) if isinstance(data, dict) else None
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        cmname = str(data.get("CMName", "")).strip()
        group_label = str(data.get("groupLabel", "")).strip()
        description = str(data.get("description", "")).strip()
        color = str(data.get("color", "")).strip()
        dynamic_props = data.get("properties", {})
        database_target = str(data.get("databaseTarget", "both")).strip().lower()
        node_label = str(data.get("nodeLabel", "")).strip().upper()

        if not cmname:
            raise ValueError("CMName is required")
        if dynamic_props is None:
            dynamic_props = {}
        if not isinstance(dynamic_props, dict):
            raise ValueError("properties must be an object")
        if not node_label:
            raw_labels = data.get("labels", [])
            if isinstance(raw_labels, str):
                raw_labels = [x.strip() for x in raw_labels.split(",") if str(x).strip()]
            if isinstance(raw_labels, list) and raw_labels:
                node_label = str(raw_labels[0]).strip().upper()
        if not node_label:
            raise ValueError("nodeLabel is required")

        prefix_map = {
            "PROPERTY": "CP",
            "LABEL": "CL",
            "TRANSLATION": "CT",
        }
        cmid_prefix = prefix_map.get(node_label)
        if not cmid_prefix:
            raise ValueError("nodeLabel must be one of: PROPERTY, LABEL, TRANSLATION")

        labels = ["METADATA", node_label]
        deduped_labels = []
        for label in labels:
            if label not in deduped_labels:
                deduped_labels.append(label)
        safe_labels = [sanitize_cypher_identifier(label, "label") for label in deduped_labels]

        if database_target == "both":
            targets = ["sociomap", "archamap"]
        elif database_target in {"sociomap", "archamap"}:
            targets = [database_target]
        else:
            raise ValueError("databaseTarget must be one of: sociomap, archamap, both")

        cmids_by_db = {}
        for db_name in ("sociomap", "archamap"):
            driver = getDriver(db_name)
            rows = getQuery(
                "MATCH (n:METADATA) WHERE n.CMID STARTS WITH $prefix RETURN n.CMID AS CMID",
                driver=driver,
                params={"prefix": cmid_prefix},
                type="list",
            )
            cmids_by_db[db_name] = rows if isinstance(rows, list) else []

        max_number = 0
        for rows in cmids_by_db.values():
            for row in rows:
                candidate = ""
                if isinstance(row, dict):
                    candidate = str(row.get("CMID") or "")
                elif isinstance(row, str):
                    candidate = row
                if not candidate.startswith(cmid_prefix):
                    continue
                suffix = candidate[len(cmid_prefix):]
                if suffix.isdigit():
                    max_number = max(max_number, int(suffix))

        generated_cmid = f"{cmid_prefix}{max_number + 1}"

        props = {
            "CMID": generated_cmid,
            "CMName": cmname,
        }
        if group_label:
            props["groupLabel"] = group_label
        if description:
            props["description"] = description
        if color:
            props["color"] = color

        blocked_prop_keys = {
            "cmid",
            "cmname",
            "id",
            "labels",
            "database",
            "databaseTarget".lower(),
            "nodeLabel".lower(),
        }
        for key, value in dynamic_props.items():
            cleaned_key = str(key or "").strip()
            if not cleaned_key:
                continue
            if cleaned_key.lower() in blocked_prop_keys:
                continue
            props[cleaned_key] = value

        check_query = "MATCH (n:METADATA {CMID: $CMID}) RETURN count(n) AS count"
        labels_clause = ":" + ":".join(safe_labels)
        create_query = f"""
        CREATE (n{labels_clause})
        SET n = $props
        RETURN elementId(n) AS id, labels(n) AS labels, properties(n) AS props
        """

        def extract_count(result):
            if result is None:
                return 0
            if isinstance(result, list):
                if not result:
                    return 0
                first = result[0]
                if isinstance(first, dict):
                    return int(first.get("count", 0) or 0)
                if isinstance(first, (int, float)):
                    return int(first)
                return 0
            if isinstance(result, dict):
                return int(result.get("count", 0) or 0)
            if isinstance(result, (int, float)):
                return int(result)
            return 0

        created_in = []
        node_results = {}

        for target in targets:
            driver = getDriver(target)
            existing = getQuery(check_query, driver=driver, params={"CMID": generated_cmid}, type="list")
            if extract_count(existing) > 0:
                raise ValueError(f"Metadata node with CMID {generated_cmid} already exists in {target}")

            created = getQuery(create_query, driver=driver, params={"props": props}, type="list")
            if node_label == "LABEL":
                _ensure_label_fulltext_index(driver, cmname)
            created_row = created[0] if isinstance(created, list) and created else {}
            db_name = "SocioMap" if target == "sociomap" else "ArchaMap"
            node_results[db_name] = created_row
            created_in.append(db_name)

        clear_metadata_caches()

        return jsonify({
            "message": f"Created metadata node {generated_cmid} in {', '.join(created_in)}.",
            "generatedCMID": generated_cmid,
            "createdIn": created_in,
            "node": node_results,
        }), 200
    except Exception as e:
        err = str(e)
        status = classify_auth_error_status(err) or 400
        if status == 400 and "already exists" in err.lower():
            status = 409
        return jsonify({"error": err}), status


@admin_bp.route('/admin/metadata/properties/<node_label>', methods=['GET'])
def metadata_properties_by_label(node_label):
    try:
        credentials = _parse_credentials(request.args.get("cred"))
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        safe_label = sanitize_cypher_identifier(str(node_label or "").strip().upper(), "nodeLabel")
        if safe_label not in {"PROPERTY", "LABEL", "TRANSLATION"}:
            raise ValueError("nodeLabel must be one of: PROPERTY, LABEL, TRANSLATION")

        database_target = str(request.args.get("databaseTarget", "both")).strip().lower()
        if database_target == "both":
            targets = ["sociomap", "archamap"]
        elif database_target in {"sociomap", "archamap"}:
            targets = [database_target]
        else:
            raise ValueError("databaseTarget must be one of: sociomap, archamap, both")

        query = f"""
        MATCH (n:METADATA:{safe_label})
        UNWIND keys(n) AS prop
        RETURN DISTINCT prop
        ORDER BY prop
        """

        all_props = set()
        for target in targets:
            result = getQuery(query=query, driver=getDriver(target), type="list")
            rows = result if isinstance(result, list) else []
            for row in rows:
                if isinstance(row, dict):
                    prop = row.get("prop")
                    if prop:
                        all_props.add(str(prop))
                elif isinstance(row, str):
                    all_props.add(row)

        return jsonify({
            "nodeLabel": safe_label,
            "properties": sorted(all_props),
        }), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/metadata/nodes', methods=['GET'])
def list_metadata_nodes():
    try:
        credentials = _parse_credentials(request.args.get("cred"))
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        query = """
        MATCH (n:METADATA)
        WITH n, [label IN labels(n) WHERE label <> 'METADATA'] AS nodeLabels
        WHERE n.CMID IS NOT NULL
           OR n.CMName IS NOT NULL
           OR n.groupLabel IS NOT NULL
           OR size(nodeLabels) > 0
        RETURN elementId(n) AS id,
               n.CMID AS CMID,
               n.CMName AS CMName,
               n.groupLabel AS groupLabel,
               n.color AS color,
               nodeLabels AS labels,
               properties(n) AS props
        ORDER BY n.CMName
        """

        result_s = getQuery(query=query, driver=getDriver("sociomap"), type="list")
        result_a = getQuery(query=query, driver=getDriver("archamap"), type="list")

        def sanitize_rows(rows):
            if not isinstance(rows, list):
                return []
            clean = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                props = row.get("props") if isinstance(row.get("props"), dict) else {}
                cmid = row.get("CMID") or props.get("CMID") or props.get("cmid") or ""
                cmname = row.get("CMName") or props.get("CMName") or props.get("Name") or props.get("name") or ""
                labels = row.get("labels") if isinstance(row.get("labels"), list) else []
                group_label = (
                    row.get("groupLabel")
                    or props.get("groupLabel")
                    or props.get("groupDomain")
                    or (labels[0] if labels else "UNMAPPED")
                )
                color = row.get("color") or props.get("color") or props.get("hexColor")

                if not cmid and not cmname:
                    continue

                clean.append({
                    "id": row.get("id"),
                    "CMID": cmid,
                    "CMName": cmname,
                    "groupLabel": group_label,
                    "color": color,
                    "labels": labels
                })
            return clean

        return jsonify({
            "SocioMap": sanitize_rows(result_s),
            "ArchaMap": sanitize_rows(result_a)
        }), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code


@admin_bp.route('/admin/metadata/node/<CMID>', methods=['GET'])
def get_metadata_node_admin(CMID):
    try:
        credentials = _parse_credentials(request.args.get("cred"))
        verify_request_auth(credentials=credentials, required_role="admin", req=request)

        if not isinstance(CMID, str) or not CMID:
            raise Exception("CMID must be a non-empty string")

        query = "MATCH (n:METADATA {CMID: $CMID}) RETURN n"
        resultS = getQuery(query=query, driver=getDriver("sociomap"), params={"CMID": CMID}, type="records")
        resultA = getQuery(query=query, driver=getDriver("archamap"), params={"CMID": CMID}, type="records")

        nodes = []
        if resultS:
            nodes.append({"SocioMap": serialize_node(resultS[0]['n'])})
        if resultA:
            nodes.append({"ArchaMap": serialize_node(resultA[0]['n'])})

        return jsonify(nodes), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 500
        return jsonify({"error": error_message}), status_code
