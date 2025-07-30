from CM import *

database = 'sociomap'
driver = getDriver(database)
CMID = ["AM1","AM2","AM3","AM4","AM5","AM6","AM7","AM8","AM9","AM10"]
domain = "CATEGORY"

if domain == "CATEGORY":
    query = """
    unwind $CMID as cmid
    match (c:CATEGORY {cmid: cmid})<-[r:USES]-(d:DATASET)
    return c.CMID as CMID, c.CMName as CMName, 
    """
elif domain == "DATASET":
    query = """
    unwind $CMID as cmid
    match (d:DATASET {cmid: cmid})
    return d.cmid, d.name, d.description
    """
else: 
    raise ValueError("Invalid domain specified")
