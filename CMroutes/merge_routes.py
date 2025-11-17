import os
from flask import request, Blueprint, jsonify
from CM import proposeMerge, joinDatasets, getDriver, unlist, getQuery
import json

merge_bp = Blueprint('merge', __name__)

@merge_bp.route('/merge/syntax/<database>', methods=['POST'])
def get_merge_syntax_route(database):
    try:
        from CM.merge import createSyntax
        data = request.get_data()
        data = json.loads(data)
        template = data.get("template")
        result = createSyntax(template=template, database=database)

        if result.get("hash") != "":
            return {"msg": "Syntax created successfully", "download": result}, 200
        else:
            return {"msg": "Syntax creation failed"}, 500
    except Exception as e:
        result = str(e)
        return result, 500

@merge_bp.route('/merge/template/<database>/<datasetID>', methods=['GET'])
def get_merge_template(database, datasetID):
    try:
        from CM.merge import getMergingTemplate
        template = getMergingTemplate(datasetID, database)
        return template
    except Exception as e:
        result = str(e)
        return result, 500
    
    
# what about calling this createLinkfile internally? # do we want to?
@merge_bp.route('/proposeMergeSubmit', methods=['POST'])
def submit_merge():
    data = request.get_data()
    data = json.loads(data)
    dataset_choices = data.get("datasetChoices")
    dataset_choices = [choice.strip() for choice in dataset_choices.split(",")]
    ncontains = data.get("mergelevel")
    category_label = unlist(data.get("categoryLabel", ""))
    intersection = unlist(data.get("intersection", False))
    database = unlist(data.get('database'))
    criteria = str.lower(unlist(data.get('equivalence')))
    resultFormat = unlist(data.get('resultFormat', 'key-to-key'))
    selectedKeyvariables = data.get('selectedKeyvariable')
    print(selectedKeyvariables)
    if category_label == "ANY DOMAIN":
        category_label = "CATEGORY"
    elif category_label == "AREA":
        category_label = "DISTRICT"

    result = proposeMerge(dataset_choices=dataset_choices, category_label=category_label,
                          criteria=criteria, database=database, intersection=intersection, selectedKeyvariables=selectedKeyvariables, ncontains=ncontains,resultFormat = resultFormat)

    return result


@merge_bp.route('/downloadMergeCode', methods=['POST'])
def get_merge_code():
    data = request.get_data()
    data = json.loads(data)


@merge_bp.route('/joinDatasets', methods=['POST'])
def submitjoinDatasets():
    data = request.get_data()
    data = json.loads(data)
    # print(data)
    database = unlist(data.get("database", ""))
    joinLeft = data.get("joinLeft")
    joinRight = data.get("joinRight")

    result = joinDatasets(database, joinLeft, joinRight)

    return jsonify(result)


@merge_bp.route('/validateDatasets', methods=['POST'])
def submitvalidateDatasets():
    data = request.get_data()
    data = json.loads(data)
    database = unlist(data.get("database", ""))
    names = data.get("names").split(",")

    driver = getDriver(database)

    with driver.session() as session:
        for i in names:
            q = """
            MATCH (n:DATASET)
            WHERE n.CMID = $prop
            RETURN COUNT(n) > 0 AS nodeExists
            """
            result = session.run(q, prop=i.strip())
            node_exists = result.single()["nodeExists"]
            if not node_exists:
                return jsonify({"success": False, "message": "Check your Dataset IDs."})
    driver.close()
    return jsonify({"success": True, "message": "All IDs exist."})

@merge_bp.route('/getKeys', methods=['POST'])
def getvalidKeysForDataset():
    data = request.get_data()
    data = json.loads(data)
    database = unlist(data.get("database", ""))
    subdomain = unlist(data.get("subdomain", ""))
    names = data.get("names").split(",")

    driver = getDriver(database)
    
    result_map={}
    
    if subdomain == "ANY DOMAIN":
        subdomain = "CATEGORY"
        
    for i in names:
        q = f"""
        MATCH (c:{subdomain})<-[r:USES]-(d:DATASET {{CMID: $datasetID}})
        RETURN r.Key as Key
        """
        result = getQuery(q,driver,params={
                              'datasetID': i.strip()})
                
        if not result:
            result_map[i] = []
            continue
            #return jsonify({"success": False, "message": f"{i} does not have ties to nodes with the selected subdomain","keysByDataset": result_map})
        
        keys = [row["Key"] for row in result]

        first_parts = []

        for key in keys:
            # Split by ';' in case there are multiple pairs
            pairs = key.split(";")
            for pair in pairs:
                # Split by ':' and take the first part, stripping whitespace
                first_part = pair.split(":")[0].strip()
                first_parts.append(first_part)

        # Remove duplicates if needed
        key_variables = list(set(first_parts))

        result_map[i] = key_variables
            
    return jsonify({"success": True, "message": "All Keys exist.","keysByDataset": result_map})        

@merge_bp.route('/linkfile', methods=['GET'])
def getLinkFile():
    try:
        database = request.args.get('database')
        datasets = request.args.get('datasets')
        intersection = request.args.get('intersection')
        domain = request.args.get('domain')

        if not isinstance(datasets, list):
            raise Exception("datasets must be a list")

        if not isinstance(domain, str):
            raise Exception("domain must be a string")

        if not isinstance(intersection, bool):
            raise Exception("intersection must be a boolean")

        driver = getDriver(database)

        query = f"""
match (c:{domain})<-[r:USES]-(d:DATASET) where d.CMID in $datasets
return distinct d.CMName as DatasetName, r.Key as Key, c.CMName as CMName, c.CMID as CMID, apoc.text.join(r.Name,'; ') as Name order by CMName
"""

        with driver.session() as session:
            result = session.run(query, datasets=datasets)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@merge_bp.route('/mergeDatasets', methods=['GET'])
def getMergeDatasets():

    database = request.args.get('database')

    driver = getDriver(database)
    query = "match (d:DATASET) return d.CMID as CMID order by CMID"
    data = getQuery(query, driver)

    return data
