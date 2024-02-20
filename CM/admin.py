''' admin.py '''

from .utils import * 

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
        return str(e), 500

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
        return str(e), 500

# function to add CMName to Name property
def addCMNametoName(cmid, driver):
    try:
        if re.search("^SM", cmid) is None:
            dataset = "AD941"
        else:
            dataset = "SD11"

        query = """
        unwind $cmid as cmid
        match (c:CATEGORY)<-[r:USES]-(:DATASET) 
        where c.CMID = cmid
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

        return f"Completed adding names to {cmid}"

    except Exception as e:
        return str(e), 500

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
        user = request.args.get("user")

        if keepcmid == deletecmid:
            raise Exception(f"keepcmid and deletecmid cannot be the same")

        results = [f"Started Combining {deletecmid} into {keepcmid}"]

        session = driver.session()

        validKeep = isValidCMID(keepcmid,driver)

        results = results + ["checking if keepcmid is valid",validKeep]

        if len(validKeep) > 0:
            if validKeep[0].get("exists") != True:
                raise Exception(f"{keepcmid} is invalid")
        else: 
            raise Exception(f"{keepcmid} is invalid")
        
        deleteKeep = isValidCMID(deletecmid,driver)

        results = results + ["checking if deletecmid is valid",deleteKeep]

        if len(deleteKeep) > 0:
            if deleteKeep[0].get("exists") != True:
                raise Exception(f"{deletecmid} is invalid")
            
        else: 
            raise Exception(f"{deletecmid} is invalid")
        
        results = results + [addCMNametoName(keepcmid, driver)]
        results = results + [addCMNametoName(deletecmid, driver)]

        # get EC relID
        query = """
        unwind $cmid as cmid match (c {CMID: cmid})<-[r:USES]-(d:DATASET {CMID: "SD11"}) return id(r) as relID
            """ 
        
        result = session.run(query,cmid = keepcmid)
        relID = [item['relID'] for item in result]

        results = results + ["relID to keep"]
        results = results + relID

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
        with a
        MATCH (a)<-[r:USES]-(:DATASET)
        unwind keys(r) as property
        return distinct property
        """

# add in properties for anything that is a parent type node
        result = session.run(query,keepcmid = keepcmid,deletecmid = deletecmid)
        properties = [dict(record) for record in result]
        result = session.run("match (m:PROPERTY) where m.relationship is not null return m.property as property")
        contextProps = [dict(record) for record in result]
        contextProps = [item['property'] for item in contextProps]

        # make sure the second EC tie is combined
        # call the updateUSES function
        for prop in properties:
            property = prop['property']
            if property in contextProps or property == "parentContext":
                results = results + [f"updating {property} with new CMID"]
                replaceProperty(cmid = keepcmid, property = property, old = deletecmid, new = keepcmid, driver = driver)    

        # create deleted node and relationship
        query = """
        unwind $keepcmid as keepcmid unwind $deletecmid as deletecmid 
        match (new {CMID: keepcmid}) 
        create (del:DELETED {CMID: deletecmid}) set del.log = [toString(date()) + ": merged " + deletecmid + " into " + keepcmid] 
        with new, del 
        create (del)-[:IS]->(new)
        """

        session.run(query,keepcmid = keepcmid,deletecmid = deletecmid)

        
        # combine EC USES ties
        
        if len(relID) > 0:
            query = """
            unwind $cmid as cmid 
            match (:DATASET {CMID: "SD11"})-[r:USES]->({CMID: cmid}) 
            with collect(r) as rels 
            call apoc.do.when(id(head(rels)) in $relID,"call apoc.refactor.mergeRelationships(rels,{properties: {Key: 'discard', language: 'discard',`.*`: 'combine'}}) yield rel 
            return count(*) as first","call apoc.refactor.mergeRelationships(rels,{properties: {Key: 'overwrite', language: 'overwrite',`.*`: 'combine'}}) yield rel 
            return count(*) as second",{rels:rels}) yield value 
            return value
            """

            session.run(query,cmid = keepcmid,relID = relID)


        # need to update USES ties
            
        id = session.run("unwind $keepcmid as cmid match (n {CMID: cmid}) return id(n) as id", keepcmid = keepcmid)
        id = [item['id'] for item in id]
        id = unlist(id)
        results = results + ["id is:",id]
        createLog(id = id, type = "node", log = f"merged {deletecmid} into {keepcmid}", user = user, driver = driver)

        results = results + [f"Completed combining {deletecmid} into {keepcmid}"]

        return results
    
    
    except Exception as e:
        return str(e), 500
    
def addIndexes(driver):
    try:
        query = """
        // create index for each label that has a Name
        match (d:LABEL)
        where d.public = true or tolower(toString(d.public)) = "true"
        with d.label as l
        call apoc.cypher.runSchema('CREATE FULLTEXT INDEX ' + l + ' IF NOT EXISTS FOR (n:' + l + ') ON EACH [n.names]',{}) yield value return count(*);
        """
        with driver.session() as session:
            session.run(query)
            driver.close()
        return "Completed"
    except Exception as e:
        return str(e), 500