''' admin.py '''

# This is a module for admin functions in CatMapper

import re
from neo4j import GraphDatabase

def replaceProperty(cmid, property, old, new, driver):
    """
    Replace a specified property value in relationships for a given CMID.

    Parameters:
    - cmid: The CMID for which the property replacement should occur.
    - property: The name of the property to be replaced.
    - old: The old value to be replaced.
    - new: The new value to replace the old value.
    - driver: Neo4j driver for database interaction.

    Returns:
    - str: A completion message indicating the success of the property replacement.

    Raises:
    - Exception: In case of any unexpected errors during property replacement.
    """
    try:
        query = f"""
match (c {{CMID: $cmid}})--(node)
match (node)<-[r:USES]-(d) 
where not r.{property} is null 
with r, case when apoc.meta.cypher.type(r.{property}) contains "LIST" 
then apoc.coll.toSet([x in [i in r.{property} | replace(i,$old,$new)] where not x = ""]) 
else replace(r.{property},$old,$new) end as prop 
with r, prop, case when isEmpty(prop) then NULL else prop end as valid 
set r.{property} = valid
"""
        with driver.session() as session:
            session.run(query,cmid = cmid, old = old, new = new)
            driver.close()
        return f"Completed {cmid} property {property}"
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        return str(e)

def getUSESrels(request, driver):
    """
    Retrieve relationships with the 'USES' type for a specified CMID.

    Parameters:
    - request: Flask request object containing 'cmid' as a query parameter.
    - driver: Neo4j driver for database interaction.

    Returns:
    - list: A list of dictionaries containing relationship information. Each dictionary includes 'relID' (relationship ID) and 'relationship' (relationship description).

    Raises:
    - Exception: If an invalid CMID is provided or in case of any unexpected errors.
    """
    try:
        cmid = request.args.get('cmid') 
        if re.search("^AM|^AD|^SM|^AD", cmid) is None:
            raise Exception("Invalid CMID")
        query = f"""
        unwind $cmid as cmid 
        match (a)-[r:USES]->(b) 
        where b.CMID = cmid 
        return distinct id(r) as relID, 
        a.CMName + '-' + type(r) + '-' + coalesce(r.Key,'') + '->' + b.CMName as relationship 
        order by relationship
        """
        with driver.session() as session:
            result = session.run(query,cmid = cmid)
            data = [dict(record) for record in result]
            driver.close()
        return data
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        return str(e), 500

def mergeRels(cmid, driver):
    """
    Identify and merge relationships for a specified CMID.

    Parameters:
    - cmid: The CMID for which relationship merging should occur.
    - driver: Neo4j driver for database interaction.

    Returns:
    - str: A string indicating the completion of the relationship merging.

    Raises:
    - Exception: In case of any unexpected errors during relationship merging.
    """
    try:
        query = """
unwind $cmid as cmid match (a)-[r]-(b) 
where a.CMID = cmid and not type(r) = 'USES' 
with id(a) as id1, id(b) as id2, type(r) as type, count(r) as c 
where c > 1 
return id1, id2, type
"""
        with driver.session() as session:
            results = session.run(query,cmid = cmid)
            driver.close()
        return "Completed"

    except Exception as e:
        return str(e)

# function to add CMName to Name property
def addCMNametoName(cmid, driver):
    try:
        if re.search("^SM", cmid) is None:
            dataset = "AD941"
        else:
            dataset = "SD11"
        query = """
match (c:CATEGORY)<-[r:USES]-(:DATASET) 
where c.CMID in $cmid
with c, collect(r) as rels, apoc.coll.toSet(apoc.coll.flatten(collect(r.Name),true)) as names 
where not c.CMName in names 
with c
match (d:DATASET {CMID: $dataset}) 
merge (c)<-[r:USES]-(d) 
on create set r.Key = "Key: " + c.CMID, r.Name = [c.CMName], r.log = [toString(date()) + ": automatically added CMName to USES relationship"]
on match set r.Name = apoc.coll.flatten([c.CMName,r.Name],true),
r.log = apoc.coll.flatten([r.log,toString(date()) + ": automatically added CMName to USES relationship"],true)  
return c,d
 """
        with driver.session() as session:
            results = session.run(query,cmid = cmid, dataset = dataset)
            driver.close()
        return "Completed"

    except Exception as e:
        return str(e)

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
        CM.addCMNametoName(keepcmid, driver)
        CM.addCMNametoName(deletecmid, driver)
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
    