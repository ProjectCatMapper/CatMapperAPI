from CM import getDriver, getQuery, getNodeProperties
from flask import request, Blueprint

metadata_bp = Blueprint('metadata', __name__)

@metadata_bp.route('/domains/<database>', methods=['GET'])
def getDomains(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    domains = getQuery(
        "MATCH (n:LABEL) where n.public = 'TRUE' and not n.CMName = 'CATEGORY' and n.groupLabel=n.CMName RETURN  n.groupLabel AS domain order by n.displayOrder,domain", driver, type="list")

    return domains

@metadata_bp.route('/subdomains/<database>', methods=['GET'])
def getSubdomains(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    # subdomains = getQuery("MATCH (n:LABEL) where n.public = 'TRUE' and not n.CMName = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.CMName as label order by domain, label WITH domain, collect(label) AS subdomains RETURN domain, [domain] + [x IN subdomains WHERE x <> domain] AS subdomains", driver, type="dict")

    subdomains = getQuery(
        "MATCH (n:LABEL) WHERE n.public = 'TRUE' AND NOT n.CMName = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.CMName AS label, n.displayOrder AS displayOrder ORDER BY domain, label WITH domain, collect(label) AS subdomains, min(displayOrder) AS displayOrder RETURN domain,[domain] + [x IN subdomains WHERE x <> domain] AS subdomains ORDER BY displayOrder, domain", driver, type="dict")

    return subdomains


def getDomainDescriptions(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    descriptions = getQuery(
        "MATCH (n:LABEL) where n.CMName = n.groupLabel and n.public = 'TRUE' and not n.CMName = 'CATEGORY' RETURN DISTINCT n.CMName AS label, n.description AS description order by label", driver, type="dict")

    return descriptions


def getProperties_route(database, domain):
    try:
        CMIDs = request.json.get('CMID', [])
        if domain not in ["DATASET", "CATEGORY"]:
            domain = "CATEGORY"
        result = getNodeProperties(database, domain, CMIDs)
        return {"data": result}
    except Exception as e:
        return {"error": str(e)}, 500
