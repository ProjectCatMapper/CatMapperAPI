from .utils import getQuery
import re
from datetime import datetime


def createLog(id, type, log, user, driver,isDataset = False):
    print("inside create Log")
    # Ensure both id and log are lists of the same length
    if isinstance(log, str):
        log = [log]
    if isinstance(id, str):
        id = [id]
    if len(id) != len(log):
        raise ValueError(
            "If passing multiple IDs and logs, they must match 1-to-1")

    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Prepare list of dicts to UNWIND
    rows = [
        {
            "id": single_id,
            "action": re.sub(r"[\'\"]", "", str(single_log)).strip(),
            "timestamp": timestamp,
            "user": user
        }
        for single_id, single_log in zip(id, log)
    ]

    if isDataset:
        search_label = "DATASET"
    else:
        search_label = "CATEGORY"
    
    if type == "node":
        query = f"""
        UNWIND $rows AS row
        MATCH (l:{search_label}) WHERE elementId(l) = row.id
        CREATE (log:LOG {{timestamp: row.timestamp, action: row.action}})
        SET log.user = row.user
        CREATE (l)-[:HAS_LOG]->(log)
        """
    elif type == "relation":
        query = """
        UNWIND $rows AS row
        MATCH ()-[l:USES]->() WHERE elementId(l) = row.id
        CREATE (log:LOG {timestamp: row.timestamp, action: row.action})
        SET log.user = row.user
        SET l.logID = custom.formatProperties([l.logID, elementId(log)], 'list', ';')[0].prop
        """
    else:
        raise ValueError("error: type must be 'node' or 'relation'")

    getQuery(query, driver, params={"rows": rows})

    return "Completed"
