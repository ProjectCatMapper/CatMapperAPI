from flask import Blueprint, request, jsonify, render_template, make_response
from CM import *
import json
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
        MATCH (n:CATEGORY)-[r:EQUIVALENT]->(d:CATEGORY)
        WHERE n.CMID = $cmid
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
