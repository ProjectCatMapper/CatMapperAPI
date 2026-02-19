import json
from CM import *
from flask import jsonify, request, Blueprint

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/metadata/domains/<database>', methods=['GET'])
def getDomains1(database):
    domains = get_public_domains(database)
    return domains

@metadata_bp.route('/metadata/subdomains/<database>', methods=['GET'])
def getSubdomains(database):

    subdomains = get_public_subdomains(database)

    return subdomains

@metadata_bp.route('/metadata/domainDescriptions/<database>', methods=['GET'])
def getDomainDescriptions(database):
    driver = getDriver(database)
    descriptions = get_domain_descriptions(database)

    return descriptions

@metadata_bp.route('/metadata/CMIDProperties/<database>/<domain>', methods=['POST'])
def getProperties_route(database, domain):
    try:
        CMIDs = request.json.get('CMID', [])
        if domain not in ["DATASET", "CATEGORY"]:
            domain = "CATEGORY"
        result = getNodeProperties(database, domain, CMIDs)
        return {"data": result}
    except Exception as e:
        return {"error": str(e)}, 500

@metadata_bp.route("/getTranslatedomains", methods=['GET'])
def getTranslatedomains():
    database = request.args.get("database")
    result_list = get_metadata_groups(database)
    return jsonify(result_list)
    
@metadata_bp.route(f"/getDomains/<database>", methods=['GET'])
def getDomains(database):
    
    driver = getDriver(database)
    query = '''
        MATCH (g:LABEL)
        WHERE g.groupLabel = g.CMName and g.displayOrder IS NOT NULL
        RETURN distinct g.groupLabel as domain, g.displayName as display, toInteger(g.displayOrder) as order
        ORDER BY order
        '''
    domains = getQuery(query,driver, type = "df")
    query = '''
            MATCH (g:LABEL)
            RETURN g.groupLabel as domain, g.CMName as subdomain, g.displayName as subdisplay, g.description as description, toInteger(g.displayOrder) as suborder
            ORDER BY suborder, subdisplay
            '''
    
    subdomains = getQuery(query,driver, type = "df")
    
    result = domains.merge(subdomains, how='left', on=['domain'])
    
    # change nan to ""
    result = result.fillna("")
    
    return jsonify(result.to_dict(orient='records'))

@metadata_bp.route(f"/metadata/getCountries/<database>", methods=['GET'])
def getCountries(database):
    
    driver = getDriver(database)
    query = '''
        MATCH (c:ADM0)
        RETURN distinct c.CMName as name, c.CMID as code
        ORDER BY name
        '''
    result = getQuery(query,driver, type = "df")
    
    # change nan to ""
    result = result.fillna("")
    
    return jsonify(result.to_dict(orient='records'))

@metadata_bp.route('/datasetDomains', methods=['POST'])
def getdatasetDomains():
    try:
        data = request.get_data()
        data = json.loads(data)

        database = unlist(data.get('database'))
        cmid = unlist(data.get('cmid'))
        children = unlist(data.get('children'))

        driver = getDriver(database)

        # combine queries
        if children == True:
            query = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[:CONTAINS*..5]->(:DATASET)-[r:USES]->(c:CATEGORY)
with distinct apoc.coll.toSet(apoc.coll.flatten(collect(r.label), true)) as labels
unwind labels as label
return label
"""
        else:
            query = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY)
with distinct apoc.coll.toSet(apoc.coll.flatten(collect(r.label), true)) as labels
unwind labels as label
return label
"""

        data = getQuery(query=query, driver=driver, params={"cmid": cmid})

        return data

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@metadata_bp.route('/allDatasets', methods=['GET'])
def getAllDatasets():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query = """
match (d:DATASET)
return elementId(d) as nodeID,
d.CMName as CMName,
d.CMID as CMID,
d.shortName as shortName,
d.project as project,
d.Unit as Unit,
d.parent as parent,
d.ApplicableYears as ApplicableYears,
d.DatasetCitation as DatasetCitation,
d.District as District,
d.DatasetLocation as DatasetLocation,
d.DatasetVersion as DatasetVersion,
d.DatasetScope as DatasetScope,
d.Subnational as Subnational,
d.Note as Note
"""
        result = getQuery(query=query, driver=driver)

        return result

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
    
@metadata_bp.route('/linkfile', methods=['GET'])
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

        result = getQuery(query=query, driver=driver, params={"datasets": datasets})

        return result

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
    
@metadata_bp.route('/metadata/node/<CMID>', methods=['GET'])
def getMetdataProperties(CMID):
    try:
        if not isinstance(CMID, str):
            raise Exception("CMID must be a string")
        driverS = getDriver("sociomap")
        driverA = getDriver("archamap")
        query = "MATCH (n:METADATA {CMID: $CMID}) RETURN n"
        resultS = getQuery(query=query, driver=driverS, params={"CMID": CMID},type = "records")
        resultA = getQuery(query=query, driver=driverA, params={"CMID": CMID},type = "records")
        nodes = []
        nodes.append({"SocioMap": serialize_node(resultS[0]['n'])})
        nodes.append({"ArchaMap": serialize_node(resultA[0]['n'])})
        return nodes
    
    except Exception as e:
        return str(e), 500
    
@metadata_bp.route('/metadata/domaincount/<database>/<domain>', methods=['GET'])
def getDomainCount(database, domain):
    try:
        driver = getDriver(database)
        domain = validate_domain_label(domain, driver=driver)
        query1 = f"""
        return apoc.meta.nodes.count(["{domain}"]) AS count
        """
        df1 = getQuery(query=query1, driver=driver, type = "list")
        return df1
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
