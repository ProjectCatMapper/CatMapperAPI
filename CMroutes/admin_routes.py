from flask import Blueprint, request, jsonify, render_template, make_response
from CM import *
import json

admin_bp = Blueprint('admin', __name__)


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
    apikeyEnv = config['DB']['apikey']
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
        driver = getDriver(database)
        fun = unlist(data.get('fun'))
        user = unlist(data.get('user'))
        pwd = unlist(data.get('pwd'))
        apikey = unlist(data.get('apikey'))
        credentials = unlist(data.get("cred"))
        input = unlist(data.get("input"))
        if credentials:
            verified = verifyUser(credentials.get(
                "userid"), credentials.get("key"), "admin")
            if verified != "verified":
                raise Exception("Error: User is not verified")
        else:
            validated = False
            if apikey == apikeyEnv:
                validated = True
            if not validated:
                credentials = login(user, pwd)
                if isinstance(credentials, dict) and credentials.get('role') == "admin":
                    validated = True
                    user = credentials.get('userid')
            if not validated:
                raise Exception("User not authorized")
        
        result = "Nothing returned"
        if fun == "mergeNodes":
            keepcmid = unlist(data.get('keepcmid').strip())
            deletecmid = unlist(data.get('deletecmid').strip())
            result = mergeNodes(keepcmid, deletecmid, user, database)
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
                database, credentials.get("userid"), input)
        elif fun == "add/edit/delete USES property":
            result = add_edit_delete_USES(
                database, credentials.get("userid"), input)
        elif fun == "merge nodes":
            result = mergeNodes(input.get('s1_2'), input.get(
                's1_3'), credentials.get("userid"), database)
        elif fun == "create new label":
            result = createLabel(database, credentials.get("userid"), input)
        elif fun == "delete node":
            result = deleteNode(database, credentials.get("userid"), input)
        elif fun == "delete USES relation":
            result = deleteUSES(database, credentials.get("userid"), input)
        elif fun == "move USES tie":
            tabledata = data.get("tabledata")
            dataset = data.get("datasetID")
            result = moveUSESties(database, credentials.get("userid"), input,dataset,tabledata)
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

        verify = verifyUser(user, pwd)

        if not verify == "verified":
            raise Exception("User is not verified.")

        if not df or len(df) == 0:
            return jsonify({"error": "Data is empty"}), 400

        df = pd.DataFrame(df)

        results = createNodes(df, database, user)

        return results

    except Exception as e:
        result = str(e)
        return result, 500
    
@admin_bp.route('/updateWaitingUSES', methods=['POST'])
def getUpdateWaitingUSES():
    data = request.get_data()
    data = json.loads(data)
    database = data.get("database")
    result = waitingUSES(database)
    return result

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

