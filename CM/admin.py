''' admin.py '''

# This is a module for admin functions in CatMapper

import re
from neo4j import GraphDatabase

# function to merge nodes
def mergeNodes(request,driver):
    """
    Merges nodes in a Neo4j database based on specified CMIDs.

    Parameters:
    - request: Flask request object containing 'keepcmid' and 'deletecmid' as query parameters.
    - driver: Neo4j driver for database interaction.

    Returns:
    - str: A completion message indicating the success of the operation.

    Raises:
    - Exception: If invalid CMIDs are provided or in case of any unexpected errors.
    """
    try:
        keepcmid = request.args.get('keepcmid') 
        deletecmid = request.args.get('deletecmid')
        if re.search("^AM|^AD|^SM|^AD", keepcmid) is None:
            raise Exception("Invalid keepcmid")
        if re.search("^AM|^AD|^SM|^AD", deletecmid) is None:
            raise Exception("Invalid deletecmid")
        query = """
match (a) where a.CMID = $keepcmid
match (b) where b.CMID = $deletecmid 
WITH collect(a) + collect(b) AS nodes
CALL apoc.refactor.mergeNodes(nodes,{properties: {
CMID:'discard',
CMName:'discard',
`.*`: 'combine'} })
YIELD node
with node
unwind node as a
MATCH (a)<-[r:USES]-(:DATASET)
unwind keys(r) as property
return distinct property
"""
        with driver.session() as session:
            result = session.run(query,keepcmid = keepcmid,deletecmid = deletecmid)
            properties = [dict(record) for record in result]
            result = session.run("match (m:PROPERTY) where m.relationship is not null return m.property as property")
            contextProps = [dict(record) for record in result]
            contextProps = [item['property'] for item in contextProps]
            driver.close()
        for prop in properties:
            property = prop['property']
            if property in contextProps:
                print(f"updating {property} with new CMID")
                replaceProperty(cmid = keepcmid, property = property, old = deletecmid, new = keepcmid, driver = driver)    
        return "Completed"
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        return str(e)