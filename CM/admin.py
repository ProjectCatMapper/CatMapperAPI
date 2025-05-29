''' admin.py '''

from .utils import *
from .log import createLog
from .USES import processUSES

# This is a module for admin functions in CatMapper

import re
from neo4j import GraphDatabase


def replaceProperty(cmid, property, old, new, database, datasetID = None, Key = None):
    """
    Replace a specified property value in relationships for a given CMID.

    Parameters:
    - cmid: The CMID for which the property replacement should occur.
    - datasetID: The ID of the dataset to be used.
    - Key: The key to identify the relationship.
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
        driver = getDriver(database)
        if datasetID is None and Key is None:
            query = f"""
            unwind $cmid as cmid
            match (:CATEGORY {{CMID: cmid}})<-[r:USES]-(:DATASET) 
            where not r.{property} is null
            with r, [i in r.{property} | case when i = $old then $new else i end] as prop
            set r.{property} = prop
            """
        else:
            if len(cmid) > 0:
                raise Exception("cmid must be a single value, not a list")
            query = f"""
            match (:CATEGORY {{CMID: $cmid}})<-[r:USES {{Key: $key}}]-(:DATASET {{CMID: $datasetID}}) 
            where not r.{property} is null
            with r, [i in r.{property} | case when i = $old then $new else i end] as prop
            set r.{property} = prop
            """
        getQuery(query, driver, params={
            "cmid": cmid,
            "datasetID": datasetID,
            "key": Key,
            "old": old,
            "new": new
        })
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
        return distinct elementId(r) as relID, 
        a.CMName + '-' + type(r) + '-' + coalesce(r.Key,'') + '->' + b.CMName as relationship 
        order by relationship
        """
        with driver.session() as session:
            result = session.run(query, cmid=cmid)
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
            results = session.run(query, cmid=cmid, dataset=dataset)
            driver.close()

        return f"Completed adding names to {cmid}"

    except Exception as e:
        return str(e), 500

# function to merge nodes


def mergeNodes(keepcmid,deletecmid,user,database):
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

        if keepcmid == deletecmid:
            raise Exception(f"keepcmid and deletecmid cannot be the same")
        
        driver = getDriver(database)

        results = [f"Started Combining {deletecmid} into {keepcmid}"]

        validKeep = isValidCMID(keepcmid, driver)

        results = results + ["checking if keepcmid is valid", validKeep]

        if len(validKeep) > 0:
            if validKeep[0].get("exists") != True:
                raise Exception(f"{keepcmid} is invalid")
        else:
            raise Exception(f"{keepcmid} is invalid")

        deleteKeep = isValidCMID(deletecmid, driver)

        results = results + ["checking if deletecmid is valid", deleteKeep]

        if len(deleteKeep) > 0:
            if deleteKeep[0].get("exists") != True:
                raise Exception(f"{deletecmid} is invalid")
        else:
            raise Exception(f"{deletecmid} is invalid")

        results = results + [addCMNametoName(keepcmid, driver)]
        results = results + [addCMNametoName(deletecmid, driver)]

        # get EC relID
        query = """
        unwind $cmid as cmid match (c {CMID: cmid})<-[r:USES]-(d:DATASET {CMID: "SD11"}) return elementId(r) as relID
            """

        relID = getQuery(query, driver, params = {"cmid": keepcmid}, type = "list")

        results = results + ["relID to keep"]
        results = results + relID

        # replace the CMID in the USES relationships
        contextProps = getQuery(
            "match (m:PROPERTY) where m.relationship is not null return m.property as property", driver, type = "list")
        contextProps.append("parentContext") 

        cmids = getQuery(
            "match (:CATEGORY {CMID: $deletecmid})-[rel]->(c:CATEGORY) return c.CMID as cmid", driver, params={
                "deletecmid": deletecmid
            },
            type="list"
        )

        for property in contextProps:
            results = results + [f"updating {property} with new CMID"]
            replaceProperty(cmid=cmids, property=property,
                            old=deletecmid, new=keepcmid, database=database)

        # combine the nodes

        query = """
        match (a) where a.CMID = $keepcmid
        match (b) where b.CMID = $deletecmid 
        WITH collect(a) + collect(b) AS nodes
        CALL apoc.refactor.mergeNodes(nodes,{properties: {
        CMID:'discard',
        CMName:'discard',
        `.*`: 'combine'} })
        YIELD node
        return node.CMID as CMID
        """

# add in properties for anything that is a parent type node
        merged = getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid, type = "list")
        if not keepcmid in merged:
            raise Exception(f"Failed to merge {deletecmid} into {keepcmid}")

        # create deleted node and relationship
        query = """
        unwind $keepcmid as keepcmid 
        unwind $deletecmid as deletecmid 
        match (new {CMID: keepcmid}) 
        create (del:DELETED {CMID: deletecmid}) 
        with new, del 
        create (del)-[:IS]->(new)
        return elementId(del) as delID
        """

        delID = getQuery(query = query, driver = driver, keepcmid=keepcmid, deletecmid=deletecmid, type = "list")

        createLog(id=delID, type="node",
                  log=f"deleted {deletecmid} and merged into {keepcmid}", user=user, driver=driver)

        # combine EC USES ties

        if len(relID) > 0:
            query = """
            unwind $cmid as cmid 
            match (:DATASET {CMID: "SD11"})-[r:USES]->({CMID: cmid}) 
            with collect(r) as rels 
            call apoc.do.when(elementId(head(rels)) in $relID,"call apoc.refactor.mergeRelationships(rels,{properties: {Key: 'discard', language: 'discard',`.*`: 'combine'}}) yield rel 
            return count(*) as first","call apoc.refactor.mergeRelationships(rels,{properties: {Key: 'overwrite', language: 'overwrite',`.*`: 'combine'}}) yield rel 
            return count(*) as second",{rels:rels}) yield value 
            return value
            """

            getQuery(query = query, driver = driver, cmid=keepcmid, relID=relID)

        # need to update USES ties

        id = getQuery(
            "unwind $keepcmid as cmid match (n {CMID: cmid}) return elementId(n) as id", driver = driver, keepcmid=keepcmid, type = "list")
        results = results + ["id is:", id]
        createLog(id=id, type="node",
                  log=f"merged {deletecmid} into {keepcmid}", user=user, driver=driver)

        processUSES(database = database, CMID=keepcmid, user="0")

        results = results + \
            [f"Completed combining {deletecmid} into {keepcmid}"]

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
