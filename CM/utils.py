''' utils.py '''

# general utility functions

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

