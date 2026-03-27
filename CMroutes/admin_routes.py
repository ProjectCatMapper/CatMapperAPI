from flask import Blueprint, request, jsonify, render_template, make_response
from CM import *
import json
from datetime import datetime, timezone
from .auth_utils import verify_request_auth, classify_auth_error_status

admin_bp = Blueprint('admin', __name__)


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


def _now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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

        allowed = {"first", "last", "username", "email", "database", "intendedUse", "access", "role"}
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
            else:
                new_value = str(value or "").strip()
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
                "updatedAt": timestamp,
                "logEntries": [log_entry],
            },
            type="dict",
        )
        if not saved_rows:
            raise Exception("User not found")

        payload = _serialize_user_lookup_row(saved_rows[0])
        payload["updateStats"] = _build_activity_stats_for_userids([userid]).get(userid, {})
        return jsonify({
            "message": "User updated",
            "user": payload,
            "changedFields": sorted(changed.keys()),
            "logEntry": log_entry,
        }), 200
    except Exception as e:
        error_message = str(e)
        status_code = classify_auth_error_status(error_message) or 400
        return jsonify({"error": error_message}), status_code


@admin_bp.route("/admin_add_edit_delete_nodeproperties", methods=['GET'])
def admin_nodeproperties():
    CMID = request.args.get('CMID')
    database = request.args.get('database')
    option = request.args.get('option')

    driver = getDriver(database)

    # q captures the actual properties of a node
    q = "MATCH (n) WHERE n.CMID = $cmid return properties(n) AS props"

    # q1 captures relevant properties of node
    if "CP" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'PROPERTY' RETURN p.CMName as property"
    elif "CL" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'LABEL' RETURN p.CMName as property"
    elif "D" in CMID:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'DATASET' RETURN p.CMName as property"
    else:
        q1 = "MATCH (p:PROPERTY) WHERE p.type='node' AND p.nodeType IS NOT NULL AND p.nodeType CONTAINS 'CATEGORY' RETURN p.CMName as property"

    with driver.session() as session:
        r = session.run(q, cmid=CMID).data()

        if r == []:
            return jsonify({"error": "Invalid CMID"})
        props = [k for k in r[0]['props'].keys()] if r else []

        # Run q1 to get allowed properties
        allowed = session.run(q1).data()
        allowed_props = {row['property'] for row in allowed}

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

    driver = getDriver(database)

    q = "MATCH (n:CATEGORY)<-[r:USES]-(d:DATASET) WHERE n.CMID = $cmid RETURN {CMName: n.CMName, CMID: n.CMID,elementId: elementId(n)} AS n,r,d"
    
    q1 = "MATCH (p:PROPERTY) WHERE p.type='relationship' RETURN p.CMName as property"

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
        
        allowed = session.run(q1).data()
        allowed_props = list({row['property'] for row in allowed})
        
    return {
        "r": records_list,
        "r1": allowed_props,
        "error": ""
    }


@admin_bp.route("/admin_add_edit_delete_equivalentproperties", methods=['GET'])
def admin_equivalentproperties():
    CMID = request.args.get('CMID')
    database = request.args.get('database')

    driver = getDriver(database)

    q = """
        MATCH (n:CATEGORY {CMID: $cmid})-[r:EQUIVALENT]-(d:CATEGORY)
        RETURN {CMName: n.CMName, CMID: n.CMID, elementId: elementId(n)} AS n, r,
               {CMName: d.CMName, CMID: d.CMID, elementId: elementId(d)} AS d
    """

    allowed_props = ["stack", "dataset", "Key"]

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
                x[1].get("dataset", ""),
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
    verify_request_auth(credentials=credentials, required_role="admin", req=request)

    result = check_ambiguous_ties_moveUSESties(driver,CMID_from,CMID_to,rel_id)
    return result

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
        acting_user = None
        auth_header = request.headers.get("Authorization", "")
        request_api_key = request.headers.get("X-API-Key", "").strip()
        auth_lower = auth_header.lower()
        has_api_key_auth = bool(request_api_key) or auth_lower.startswith("apikey ") or auth_lower.startswith("api-key ")
        if credentials or auth_header.startswith("Bearer ") or has_api_key_auth:
            claims = verify_request_auth(credentials=credentials, required_role="admin", req=request)
            acting_user = claims.get("userid")
        else:
            validated = False
            if apikeyEnv and apikey and apikey == apikeyEnv:
                validated = True
                acting_user = user
            if not validated:
                credentials = login(user, pwd)
                if isinstance(credentials, dict) and credentials.get('role') == "admin":
                    validated = True
                    acting_user = credentials.get('userid')
            if not validated:
                raise Exception("User not authorized")
        if not acting_user:
            acting_user = user
        
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
            result = add_edit_delete_Node(
                database, acting_user, input)
        elif fun == "add/edit/delete USES property":
            result = add_edit_delete_USES(
                database, acting_user, input)
        elif fun == "add/edit/delete EQUIVALENT property":
            result = add_edit_delete_EQUIVALENT(
                database, acting_user, input)
        elif fun == "merge nodes":
            result = mergeNodes(input.get('s1_2'), input.get(
                's1_3'), acting_user, database)
        elif fun == "create new label":
            result = createLabel(database, acting_user, input)
        elif fun == "delete node":
            result = deleteNode(database, acting_user, input)
        elif fun == "delete USES relation":
            result = deleteUSES(database, acting_user, input)
        elif fun == "delete EQUIVALENT relation":
            result = deleteEQUIVALENT(database, acting_user, input)
        elif fun == "move USES tie":
            tabledata = data.get("tabledata")
            dataset = data.get("datasetID")
            result = moveUSESties(database, acting_user, input,dataset,tabledata)
        elif fun == "move EQUIVALENT tie":
            result = moveEQUIVALENTties(database, acting_user, input)
        else:
            raise Exception("Function does not exist")
        return result
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)
        return data, 500


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
    if request.method == 'GET':
        database = request.args.get('database')
        CMID = request.args.get('CMID')
        Key = request.args.get('Key')
        datasetID = request.args.get('datasetID')
    else:
        data = request.get_data()
        data = json.loads(data)
        database = data.get("database")
        CMID = data.get("CMID")
        Key = data.get("Key")
        datasetID = data.get("datasetID")
    
    result = mergeUSESties(database, CMID, Key, datasetID)
    return result

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
            created_row = created[0] if isinstance(created, list) and created else {}
            db_name = "SocioMap" if target == "sociomap" else "ArchaMap"
            node_results[db_name] = created_row
            created_in.append(db_name)

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
