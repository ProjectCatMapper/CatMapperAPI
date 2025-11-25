from CM import getDriver, getQuery, getNodeProperties
from flask import jsonify, request, Blueprint

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/metadata/domains/<database>', methods=['GET'])
def getDomains1(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    domains = getQuery(
        "MATCH (n:LABEL) where n.public = 'TRUE' and not n.CMName = 'CATEGORY' and n.groupLabel=n.CMName RETURN  n.groupLabel AS domain order by n.displayOrder,domain", driver, type="list")

    return domains

@metadata_bp.route('/metadata/subdomains/<database>', methods=['GET'])
def getSubdomains(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    # subdomains = getQuery("MATCH (n:LABEL) where n.public = 'TRUE' and not n.CMName = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.CMName as label order by domain, label WITH domain, collect(label) AS subdomains RETURN domain, [domain] + [x IN subdomains WHERE x <> domain] AS subdomains", driver, type="dict")

    subdomains = getQuery(
        "MATCH (n:LABEL) WHERE n.public = 'TRUE' AND NOT n.CMName = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.CMName AS label, n.displayOrder AS displayOrder ORDER BY domain, displayOrder WITH domain, collect(label) AS subdomains MATCH (d:LABEL {CMName: domain}) WITH domain, subdomains, d.displayOrder AS domainOrder RETURN domain, [domain] + [x IN subdomains WHERE x <> domain] AS subdomains ORDER BY domainOrder, domain", driver, type="dict")

    return subdomains

@metadata_bp.route('/metadata/domainDescriptions/<database>', methods=['GET'])
def getDomainDescriptions(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    descriptions = getQuery(
        "MATCH (n:LABEL) where n.CMName = n.groupLabel and n.public = 'TRUE' and not n.CMName = 'CATEGORY' RETURN DISTINCT n.CMName AS label, n.description AS description order by label", driver, type="dict")

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
    driver = getDriver(database)
    query = '''MATCH (m:METADATA)
            WHERE m.displayOrder IS NOT NULL
            AND NOT m.CMName IN ['ALL NODES']
            WITH m.groupLabel AS group, m.CMName AS node, m.displayOrder AS nodeOrder
            MATCH (g:METADATA {CMName: group})
            WHERE g.displayOrder IS NOT NULL
            WITH g.groupLabel AS group, g.displayOrder AS groupOrder, node, nodeOrder
            ORDER BY group, nodeOrder, node 
            WITH group, groupOrder, collect(node) AS nodes
            RETURN group, nodes
            ORDER BY groupOrder
            '''
    
    result = getQuery(query,driver)

    result_list = []
    for record in result:
        group = record["group"]
        members = record["nodes"]
        result_list.append({"group": group, "nodes": members})
    
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