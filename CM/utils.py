''' utils.py '''

# general utility functions

import re
from datetime import datetime

def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l

def isValidCMID(cmid, driver):
    
    query = "unwind $cmid as cmid match (c) where c.CMID = cmid return c.CMID as cmid, true as exists"

    with driver.session() as session:
        result = session.run(query,cmid = cmid)
        result = [dict(record) for record in result]
        driver.close()

    return result

def createLog(id, type, log, user, driver):
    # Remove single and double quotes from the log message
    logQ = re.sub(r"[\'\"]", "", log)

    # Format the log message with current UTC time, user, and the cleaned log message
    logQ = f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: {logQ}"

    # Constructing the query string based on the type
    if type == "node":
        qs = "(l) where id(l) = toInteger(id)"
    elif type == "relation":
        qs = "()-[l]->() where id(l) = toInteger(id)"
    else:
        raise ValueError("error: type must be node or relation")

    # Final query construction with string interpolation
    q = f"unwind $ids as id match {qs} with l, apoc.coll.flatten(['{logQ}',coalesce(l.log,[])],true) as log set l.log = log"

    with driver.session() as session:
        session.run(q, user = user, ids = id)
        driver.close()

    return "Completed"