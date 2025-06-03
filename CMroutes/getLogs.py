from CM import getDriver, getQuery

def getLogs(database, CMID):
    if not isinstance(CMID, str):
        return "Invalid CMID format. It should be a string."
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."
    
    query = """
    MATCH (a {CMID: $CMID})-[:HAS_LOG]->(log:LOG)
    return "node" as log_type, elementId(log) as ID, log.user as user, log.action as action, log.timestamp as timestamp
    order by timestamp desc
    UNION ALL
    MATCH (:CATEGORY {CMID: $CMID})<-[r:USES]-(:DATASET)
    where not r.logID is null
    with r.logID as logID
    match (log:LOG) where elementId(log) in logID
    return "relationship" as log_type, elementId(log) as ID, log.user as user, log.action as action, log.timestamp as timestamp 
    order by timestamp desc
    """
    logs = getQuery(query=query, driver=driver, CMID=CMID)

    if isinstance(logs, dict):
        return logs
    else: 
        return {"logs": logs}