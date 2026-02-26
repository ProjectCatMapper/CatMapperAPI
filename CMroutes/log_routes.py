from CM import getDriver, getQuery
from flask import Blueprint

logs_bp = Blueprint('logs', __name__)

@logs_bp.route('/logs/<database>/<CMID>', methods=['GET'])
def getLogs(database, CMID):
    if not isinstance(CMID, str):
        return "Invalid CMID format. It should be a string."
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    query = """
    CALL {
        MATCH (a:CATEGORY|DATASET {CMID: $CMID})-[:HAS_LOG]->(log:LOG)
        RETURN
            "node" AS log_type,
            elementId(log) AS ID,
            log.user AS user,
            log.action AS action,
            log.timestamp AS timestamp
        UNION ALL
        MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
        WHERE (c.CMID = $CMID OR d.CMID = $CMID)
          AND r.logID IS NOT NULL
        UNWIND apoc.coll.flatten([r.logID], true) AS relLogID
        MATCH (log:LOG)
        WHERE elementId(log) = relLogID
        RETURN
            "relationship: (Key) " + r.Key + " (datasetID) " + d.CMID AS log_type,
            elementId(log) AS ID,
            log.user AS user,
            log.action AS action,
            log.timestamp AS timestamp
    }
    RETURN log_type, ID, user, action, timestamp
    ORDER BY timestamp DESC
    """
    logs = getQuery(query=query, driver=driver, CMID=CMID)

    if isinstance(logs, dict):
        return logs
    else:
        return {"logs": logs}
