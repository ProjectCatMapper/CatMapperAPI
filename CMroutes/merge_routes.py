from flask import request, Blueprint, jsonify
from CM import proposeMerge, joinDatasets, getDriver, unlist, getQuery, validate_domain_label
import json
import re

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
    dataset_choices = [choice.strip() for choice in dataset_choices.split(",") if choice.strip()]
    ncontains = data.get("mergelevel")
    category_label = unlist(data.get("categoryLabel", ""))
    intersection = unlist(data.get("intersection", False))
    database = unlist(data.get('database'))
    criteria = str.lower(unlist(data.get('equivalence')))
    resultFormat = unlist(data.get('resultFormat', 'key-to-key'))
    selectedKeyvariables = data.get('selectedKeyvariable')
    print(selectedKeyvariables)

    invalid_dataset_ids = [cmid for cmid in dataset_choices if re.match(r"^(SD|AD)\d+$", cmid, re.IGNORECASE) is None]
    if invalid_dataset_ids:
        return jsonify({"error": f"Only DATASET CMIDs are allowed: {', '.join(invalid_dataset_ids)}"}), 400

    if criteria == "extended" and len(dataset_choices) > 2:
        return jsonify({"error": "Extended merge supports at most two dataset CMIDs."}), 400

    if category_label == "ANY DOMAIN":
        category_label = "CATEGORY"
    elif category_label == "AREA":
        category_label = "DISTRICT"

    category_label = validate_domain_label(category_label, driver=getDriver(database))

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
    domain = unlist(data.get("domain", ""))

    result = joinDatasets(database, joinLeft, joinRight, domain)

    return jsonify(result)


@merge_bp.route('/validateDatasets', methods=['POST'])
def submitvalidateDatasets():
    data = request.get_data()
    data = json.loads(data)
    database = unlist(data.get("database", ""))
    names_raw = data.get("names", "")
    names = [name.strip() for name in names_raw.split(",") if name.strip()]

    driver = getDriver(database)

    q = """
    UNWIND $names AS cmid
    MATCH (n:DATASET {CMID: cmid})
    RETURN
      n.CMID as CMID,
      n.CMName as CMName,
      n.shortName as shortName,
      n.DatasetCitation as DatasetCitation
    ORDER BY n.CMID
    """
    rows = getQuery(q, driver, params={'names': names}) if names else []

    found = {row.get("CMID") for row in rows}
    missing = [cmid for cmid in names if cmid not in found]

    if missing:
        return jsonify({
            "success": False,
            "message": "Check your Dataset IDs.",
            "missing": missing,
            "datasets": rows
        })

    return jsonify({
        "success": True,
        "message": "All IDs exist.",
        "datasets": rows
    })

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
    elif subdomain == "AREA":
        subdomain = "DISTRICT"

    subdomain = validate_domain_label(subdomain, driver=driver)
        
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
            # Split by ' && ' in case there are multiple pairs
            pairs = key.split(" && ")
            for pair in pairs:
                # Split by ' == ' and take the first part, stripping whitespace
                first_part = pair.split(" == ")[0].strip()
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

        if isinstance(datasets, str):
            if datasets.strip().startswith("["):
                datasets = json.loads(datasets)
            else:
                datasets = [d.strip() for d in datasets.split(",") if d.strip()]
        if not isinstance(datasets, list):
            raise Exception("datasets must be a list")

        if not isinstance(domain, str):
            raise Exception("domain must be a string")

        if isinstance(intersection, str):
            intersection = intersection.strip().lower() == "true"
        if not isinstance(intersection, bool):
            raise Exception("intersection must be a boolean")

        driver = getDriver(database)
        domain = validate_domain_label(domain, driver=driver)

        query = f"""
        match (c:{domain})<-[r:USES]-(d:DATASET) where d.CMID in $datasets
        return distinct d.CMName as DatasetName, r.Key as Key, c.CMName as CMName, c.CMID as CMID, apoc.text.join(r.Name,'; ') as Name order by CMName
        """

        result = getQuery(query, driver, params={'datasets': datasets})
        data = [dict(record) for record in result]

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


@merge_bp.route('/merge/template/summary/<database>/<cmid>', methods=['GET'])
def get_merge_template_summary(database, cmid):
    driver = getDriver(database)

    labels_query = """
    MATCH (n {CMID: $cmid})
    RETURN labels(n) AS labels
    LIMIT 1
    """
    label_rows = getQuery(labels_query, driver, params={"cmid": cmid}) or []
    if not label_rows:
        return jsonify({"error": "Node not found"}), 404

    labels = label_rows[0].get("labels", [])
    node_type = "OTHER"
    if "MERGING" in labels:
        node_type = "MERGING"
    elif "STACK" in labels:
        node_type = "STACK"

    stack_ids = []
    stack_summary = []
    dataset_summary = []
    merging_template_count = 0

    if node_type == "MERGING":
        stack_summary_query = """
        MATCH (m:MERGING {CMID: $cmid})-[:MERGING]->(s:STACK)
        OPTIONAL MATCH (s)-[:MERGING]->(d:DATASET)
        WHERE NOT d:STACK AND NOT d:MERGING
        WITH s, collect(DISTINCT d) AS datasets
        OPTIONAL MATCH (s)-[:MERGING]->(v:VARIABLE)
        WITH s, size(datasets) AS datasetCount, count(DISTINCT v) AS variableCount
        OPTIONAL MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: s.CMID}]->(c2:CATEGORY)
        WITH
          s,
          datasetCount,
          variableCount,
          count(DISTINCT CASE WHEN c1.CMID = c2.CMID THEN e END) AS equivalenceTieCount,
          count(DISTINCT CASE WHEN c1.CMID <> c2.CMID THEN e END) AS keyReassignmentCount
        RETURN
          s.CMID AS stackID,
          s.CMName AS stackCMName,
          datasetCount,
          equivalenceTieCount,
          keyReassignmentCount,
          variableCount
        ORDER BY s.CMID
        """
        stack_summary = getQuery(stack_summary_query, driver, params={"cmid": cmid}) or []
        stack_ids = [row.get("stackID") for row in stack_summary if row.get("stackID")]

    elif node_type == "STACK":
        stack_ids = [cmid]

        merging_count_query = """
        MATCH (m:MERGING)-[:MERGING]->(:STACK {CMID: $cmid})
        RETURN count(DISTINCT m) AS mergingTemplateCount
        """
        count_rows = getQuery(merging_count_query, driver, params={"cmid": cmid}) or []
        if count_rows:
            merging_template_count = count_rows[0].get("mergingTemplateCount", 0) or 0

        dataset_summary_query = """
        MATCH (:STACK {CMID: $cmid})-[:MERGING]->(d:DATASET)
        WHERE NOT d:STACK AND NOT d:MERGING
        OPTIONAL MATCH (d)-[:MERGING {stack: $cmid}]->(v:VARIABLE)
        WITH d, count(DISTINCT v) AS variableCount
        OPTIONAL MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: $cmid, dataset: d.CMID}]->(c2:CATEGORY)
        WITH
          d,
          variableCount,
          count(DISTINCT CASE WHEN c1.CMID = c2.CMID THEN e END) AS equivalenceTieCount,
          count(DISTINCT CASE WHEN c1.CMID <> c2.CMID THEN e END) AS keyReassignmentCount
        RETURN
          d.CMID AS datasetID,
          d.CMName AS datasetCMName,
          equivalenceTieCount,
          keyReassignmentCount,
          variableCount
        ORDER BY d.CMID
        """
        dataset_summary = getQuery(dataset_summary_query, driver, params={"cmid": cmid}) or []

    merging_ties = []
    equivalence_ties = []

    if stack_ids:
        merging_ties_query = """
        UNWIND $stack_ids AS stackID
        MATCH (s:STACK {CMID: stackID})-[r:MERGING]->(target)
        OPTIONAL MATCH (m:MERGING)-[:MERGING]->(s)
        RETURN
          m.CMID AS mergingID,
          m.CMName AS mergingCMName,
          s.CMID AS stackID,
          s.CMName AS stackCMName,
          type(r) AS relationship,
          labels(target) AS targetLabels,
          target.CMID AS targetCMID,
          target.CMName AS targetCMName,
          r.stack AS tieStackID,
          r.varName AS varName,
          r.summaryStatistic AS summaryStatistic
        ORDER BY stackID, targetCMID
        """
        merging_ties = getQuery(merging_ties_query, driver, params={"stack_ids": stack_ids}) or []

        equivalence_ties_query = """
        UNWIND $stack_ids AS stackID
        MATCH (c1:CATEGORY)-[e:EQUIVALENT {stack: stackID}]->(c2:CATEGORY)
        RETURN
          stackID AS stackID,
          e.dataset AS datasetID,
          e.Key AS `Key`,
          c1.CMID AS originalCMID,
          c1.CMName AS originalCMName,
          c2.CMID AS equivalentCMID,
          c2.CMName AS equivalentCMName,
          CASE WHEN c1.CMID = c2.CMID THEN true ELSE false END AS selfReference
        ORDER BY stackID, datasetID, originalCMID
        """
        equivalence_ties = getQuery(equivalence_ties_query, driver, params={"stack_ids": stack_ids}) or []

    totals = {
        "datasetCount": sum((row.get("datasetCount", 0) or 0) for row in stack_summary),
        "equivalenceTieCount": sum((row.get("equivalenceTieCount", 0) or 0) for row in stack_summary),
        "keyReassignmentCount": sum((row.get("keyReassignmentCount", 0) or 0) for row in stack_summary),
        "variableCount": sum((row.get("variableCount", 0) or 0) for row in stack_summary),
    }

    return jsonify({
        "nodeType": node_type,
        "stackSummary": stack_summary,
        "stackSummaryTotals": totals,
        "datasetSummary": dataset_summary,
        "mergingTemplateCount": merging_template_count,
        "mergingTies": merging_ties,
        "equivalenceTies": equivalence_ties,
    })
