''' admin.py '''

from .utils import *
from .log import createLog
from .USES import processUSES
from .USES import addCMNameRel,processDATASETs,waitingUSES
from .upload_optimised import updateProperty,createUSES
from flask import jsonify


# This is a module for admin functions in CatMapper

import re
from neo4j import GraphDatabase

# when a node is deleted or merged, this finds all instances of that CMID in other
# USES tie properties and edits them to reflect the change
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

def moveUSESValidate(relid, database):
    
    driver = getDriver(database)

    query = """
    match (p:CATEGORY)<-[r:USES]-(d:DATASET)
    where elementId(r) = $relid
    match (p)<-[r2:USES]-(d)
    with p,d, collect(r2) as rels, count(*) as n
    where n > 1
    with p,d
    match (p)-[rel]->(c:CATEGORY)<-[r3:USES]-(d)
    where not isEmpty([i in keys(r3) where p.CMID in r3[i]])
    return c.CMID as childCMID, c.CMName as childCMName, r3.Key as childKey, type(rel) as relationship
    """
    result = getQuery(query, driver, params={"relid": relid})

    return result

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
        unwind $cmid as cmid match (c:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET {CMID: "SD11"}) return elementId(r) as relID
            """

        relID = getQuery(query, driver, params = {"cmid": keepcmid}, type = "list")

        results = results + ["relID to keep"]
        results = results + relID

        # replace the CMID in the USES relationships
        contextProps = getQuery(
            "match (m:PROPERTY) where m.relationship is not null return m.CMName as property", driver, type = "list")
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

        # determine if the CMID is dataset or category
        if len(keepcmid) > 1 and keepcmid[1] == "D":
            domain = "DATASET"
        else: 
            domain = "CATEGORY"
        
        # combine the nodes

        query = f"""
        match (a:{domain} {{CMID: $keepcmid}})
        match (b:{domain} {{CMID: $deletecmid}})
        WITH collect(a) + collect(b) AS nodes
        CALL apoc.refactor.mergeNodes(nodes,{{properties: {{
        CMID:'discard',
        CMName:'discard',
        `.*`: 'combine'}} }})
        YIELD node
        return node.CMID as CMID
        """

# add in properties for anything that is a parent type node
        merged = getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid, type = "list")
        if not keepcmid in merged:
            raise Exception(f"Failed to merge {deletecmid} into {keepcmid}")

        # create deleted node and relationship to remaining node
        query = f"""
        unwind $keepcmid as keepcmid 
        unwind $deletecmid as deletecmid 
        match (new:{domain} {{CMID: keepcmid}}) 
        create (del:DELETED {{CMID: deletecmid}}) 
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
        with d.CMName as l
        call apoc.cypher.runSchema('CREATE FULLTEXT INDEX ' + l + ' IF NOT EXISTS FOR (n:' + l + ') ON EACH [n.names]',{}) yield value return count(*);
        """
        with driver.session() as session:
            session.run(query)
            driver.close()
        return "Completed"
    except Exception as e:
        return str(e), 500

def add_edit_delete_Node(database,user,input):
    changeNodeID = input.get('s1_2')
    changeNodeOptions = input.get('s1_7')
    changeNodeValue = input.get('s1_3')
    addOrEditNode = input.get('s1_1')

    driver = getDriver(database)

    if not changeNodeID or not addOrEditNode:
        return
    
    # label = CMgetLabel(CMID=changeNodeID, con=con)[0]
    if changeNodeID[1] == "D":
        label = "DATASET"
    elif changeNodeID[1] == "M":
        label = "CATEGORY"
    elif changeNodeID[1] == "P":
        label = "PROPERTY"
    elif changeNodeID[1] == "L":
        label = "LABEL"

    # Get prior value
    priorValQuery = f"""
        MATCH (a {{CMID: '{changeNodeID}'}})
        RETURN a.{changeNodeOptions} AS val
    """
    #priorVal = CMCypherQuery(con=con, query=priorValQuery)
    priorVal = getQuery(priorValQuery,driver=driver,type='list')

    if addOrEditNode == "delete":
        if label == "DATASET" and changeNodeOptions in ["District", "parent"]:
            if changeNodeOptions == "District":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:DISTRICT_OF]-(c:DISTRICT)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)
            if changeNodeOptions == "parent":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:CONTAINS]-(c:DATASET)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)

        q = f"""
            MATCH (a {{CMID: '{changeNodeID}'}})
            SET a.{changeNodeOptions} = NULL
        """
        #CMCypherQuery(con=con, query=q)
        getQuery(q,driver=driver)

    else:  # edit or add
        if label == "DATASET" and changeNodeOptions in ["District", "parent"]:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeOptions} = split($id, ' || ')
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id":changeNodeValue})

            processDATASETs(database,CMID=changeNodeID,user=user)
        else:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeOptions} = $id
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id":changeNodeValue})

            if changeNodeOptions == "CMName":
                try:
                    #CMaddCMNameRel(CMID=changeNodeID, user=user, con=con)
                    addCMNameRel(database,CMID=changeNodeID)
                except Exception as e:
                    print(f"CMaddCMNameRel failed: {e}")

    new_val = "NULL" if addOrEditNode == "delete" else changeNodeValue
    log_msg = f"updated CMID {changeNodeID} {changeNodeOptions} from {priorVal} to {new_val}"
    #CMlog(id=changeNodeID, type="node", log=log_msg, user=user, con=con)
    return "updated successfully"

def add_edit_delete_USES(database,user,input):
    CMID = input.get('s1_2')
    USES_property = input.get('s1_8')
    new_property_value = input.get('s1_3')
    addOrEditNode = input.get('s1_1')
    indexValue = input.get('s1_7')
    key = input.get('s1_4')[indexValue-1][1]["Key"]
    datasetID = input.get('s1_4')[indexValue-1][2]["CMID"]

    if "||" in new_property_value:
        new_property_value=new_property_value.split("||")

    data = {
            'CMID': CMID,
            'Key': key,
            'datasetID': datasetID,
            USES_property: new_property_value
        }
    
    driver = getDriver(database)
        
    if CMID[1] == "D":
        isDataset = True
    elif CMID[1] == "M":
        isDataset = False

    df = pd.DataFrame([data])

    if addOrEditNode == "edit" or addOrEditNode == "add":
        updateProperty(df,isDataset,database,user,updateType = "overwrite", propertyType="USES")
        processUSES(CMID=CMID,database=database,user=user)
    elif addOrEditNode == "delete":
        q = f"""
                MATCH (a:CATEGORY {{CMID: '{CMID}'}})<-[r:USES {{Key: '{key}'}}]-(d:DATASET {{CMID: '{datasetID}'}})
                REMOVE r.{USES_property} RETURN elementId(r) as relID
            """
        result = getQuery(q,driver=driver)
        processUSES(CMID=CMID,database=database,user=user)

        createLog(id=[row["id"] for row in result], type="relation",
                        log=f"deleted USES property {input.get('s1_8')} with value {input.get('s1_3')}",
                        user=user, driver=driver)

    return "done"

def createLabel(database,user,input):
    driver = getDriver(database)

    q = f"""MATCH (n:LABEL) WHERE n.CMName='{input.get('s1_2')}' RETURN n.CMName as CMName"""

    result = getQuery(q,driver=driver)

    #returns error if label name already exists
    if result != []:
        return "Label name already exists"
    
    q = 'MATCH (n:LABEL) WHERE n.CMID STARTS WITH "CL" WITH n, toInteger(replace(n.CMID, "CL", "")) AS numericID RETURN numericID ORDER BY numericID DESC LIMIT 1'

    result = getQuery(q,driver=driver)

    CMID = "CL" + str(result[0]['numericID']+1)

    if input.get('s1_7') == "NA":
        grouplabel = input.get('s1_2').strip()
    else:
        grouplabel = input.get('s1_7').strip()
    
    if input.get('s1_6') == "":
        color = "#404040"
    else:
        color = input.get('s1_6')
    
    if input.get('s1_3').strip() != "":       
        q = f"""CREATE (n:METADATA:LABEL {{CMID:'{CMID}',CMName:'{input.get('s1_2')}',groupLabel:'{grouplabel}',relationship:'{input.get('s1_3')}',description:'{input.get('s1_4')}',displayName:'{input.get('s1_5')}',color:'{color}',label:'{input.get('s1_5')}',public:"TRUE"}})"""
    else:
        q = f"""CREATE (n:METADATA:LABEL {{CMID:'{CMID}',CMName:'{input.get('s1_2')}',groupLabel:'{grouplabel}',description:'{input.get('s1_4')}',displayName:'{input.get('s1_5')}',color:'{color}',label:'{input.get('s1_5')}',public:"TRUE"}})"""


    result = getQuery(q,driver=driver)

    return "done"

def getLabel(CMID, driver, filter=True):
    # Run Cypher query to get labels
    query = """
        UNWIND $CMID AS cmid 
        MATCH (a) WHERE a.CMID = cmid 
        UNWIND labels(a) AS label 
        RETURN label
    """
    result = getQuery(query=query, params={"CMID": CMID}, driver=driver)
    
    # Sort labels alphabetically
    labels = sorted([row["label"] for row in result])

    if filter:
        # Get groupLabel metadata
        grpLabels = getLabelsMetadata(driver=driver)
        grpLabels = list(set(row["groupLabel"] for row in grpLabels if row["groupLabel"] is not None))

        # Filter out group labels
        resultF = [label for label in labels if label not in grpLabels]
        if resultF:
            labels = resultF

        # Filter out "CATEGORY"
        resultF = [label for label in labels if label != "CATEGORY"]
        if resultF:
            labels = resultF

    return labels

def getID(id_value, property, driver):
    # Ensure id_value is a list of trimmed strings
    if isinstance(id_value, str):
        id_list = [id_value.strip()]
    else:
        id_list = [str(x).strip() for x in id_value]

    # Construct and execute Cypher query
    query = f"""
        UNWIND $id AS id
        MATCH (a)
        WHERE a.{property} = id
        RETURN id(a) AS id
    """
    result = getQuery(query=query, params={"id": id_list}, driver=driver)

    # Return first result if any, else None
    return result[0]["id"] if result else None

def deleteID(id_value, driver, type="node"):
    # Validate and coerce input to integer or list of integers
    if isinstance(id_value, int):
        id_list = [id_value]
    elif isinstance(id_value, list):
        try:
            id_list = [int(i) for i in id_value]
        except (ValueError, TypeError):
            raise ValueError("id should be an integer or list of integers")
    else:
        raise ValueError("id should be an integer or list of integers")

    if type not in ["relationship", "node"]:
        raise ValueError("type should be 'relationship' or 'node'")

    count_deleted = 0
    queries = []

    if type == "relationship":
        for id in id_list:
            q = f"MATCH ()-[r]->() WHERE id(r) = {id} DELETE r RETURN count(*) AS count"
            queries.append(q)
    else:  # type == "node"
        for id in id_list:
            q = f"MATCH (a) WHERE id(a) = {id} DETACH DELETE a RETURN count(*) AS count"
            queries.append(q)

    for query in queries:
        result = getQuery(query, driver=driver)
        if result and "count" in result[0]:
            count_deleted += result[0]["count"]

    return f"Deleted {count_deleted} of type {type}"


def deleteNode(database,user,input):
    driver = getDriver(database)

    try:
        label = getLabel(input.get('s1_2'),driver,filter=True)
        
        # If you delete a dataset node, need to remove it’s CMID from all parent properties 
        # in other dataset nodes and Dataset in USES ties
        if "DATASET" in label:
            ids_query = f"""
                MATCH (:DATASET)-[r:USES]->(:CATEGORY)
                WHERE '{input.get('s1_2')}' IN r.Dataset
                WITH r, [i IN r.Dataset WHERE NOT i = '{input.get('s1_2')}'] AS prop
                SET r.Dataset = prop
                RETURN id(r) AS ids
            """

            ids = getQuery(ids_query,driver=driver)

            if len(ids) > 0:

                cleanup_query = """
                    UNWIND $ids AS id
                    MATCH (:DATASET)-[r:USES]->(:CATEGORY)
                    WHERE id(r) = id AND size(r.Dataset) = 0
                    SET r.Dataset = NULL
                """
                getQuery(cleanup_query,driver=driver,params={"ids": [row['ids'] for row in ids]})
                createLog(id=[row['ids'] for row in ids], type="relation",
                      log=f"removed reference to deleted node {input.get('s1_2')} from Dataset property",
                      user=user, driver=driver)
            
            # removing CMID for deleted node from parent property in dataset nodes
            datasetIDs_query = f"""
                MATCH (d:DATASET)
                WHERE '{input.get('s1_2')}' IN d.parent
                RETURN id(d) AS ids
            """
            datasetIDs = getQuery(datasetIDs_query,driver=driver)

            if len(datasetIDs) > 0:
                for prop in ["parent"]:
                    ids_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id
                        WITH d, [i IN d.{prop} WHERE NOT i = '{input.get('s1_2')}'] AS p
                        SET d.{prop} = p
                        RETURN id(d) AS ids
                    """
                    ids = getQuery(ids_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    nullify_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id AND size(d.{prop}) = 0
                        SET d.{prop} = NULL
                    """
                    getQuery(nullify_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    createLog(id=[row['ids'] for row in ids], type="node",
                          log=f"removed reference to deleted node {input.get('s1_2')} from {prop}",
                          user=user, driver=driver)


        # If you delete a category node, need to remove it’s CMID from all properties (including parentContext) 
        # in all USES ties (district, country, parent, language, culture….) and 
        # from dataset nodes (District)
        else:
            props = getPropertiesMetadata(driver=driver)
            props = list(set([p['property'] for p in props if p['relationship'] is not None] + ["parentContext"]))

            rels_query = f"""
                UNWIND $keys AS key
                MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
                WITH key, d, c, '{input.get('s1_2')}' AS cmid, r
                WHERE r[key] IS NOT NULL AND (
                    toString(cmid) IN r[key] OR 
                    (r.parentContext IS NOT NULL AND ANY(i IN r.parentContext WHERE i CONTAINS '\"parent\":\"' + cmid))
                )
                RETURN id(r) AS id, r[key] AS val, cmid, key
            """
            rels = getQuery(rels_query, driver = driver, params={"keys": props})

            datasetIDs_query = f"""
                MATCH (d:DATASET)
                WHERE '{input.get('s1_2')}' IN d.District
                RETURN id(d) AS ids
            """
            datasetIDs = getQuery(datasetIDs_query,driver=driver)

            # removing CMID for deleted node from District property in dataset nodes
            if len(datasetIDs) > 0:
                for prop in ["District"]:
                    ids_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id
                        WITH d, [i IN d.{prop} WHERE NOT i = '{input.get('s1_2')}'] AS p
                        SET d.{prop} = p
                        RETURN id(d) AS ids
                    """
                    ids = getQuery(ids_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    nullify_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id AND size(d.{prop}) = 0
                        SET d.{prop} = NULL
                    """
                    getQuery(nullify_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    createLog(id=[row['ids'] for row in ids], type="node",
                          log=f"removed reference to deleted node {input.get('s1_2')} from {prop}",
                          user=user, driver=driver)
                    
            # getting all the affected relationships and extracting the safe data and setting it back
            if len(rels) > 0:
                from itertools import groupby
                import re

                sepRels = []
                for row in rels:
                    for val in re.split(r' \|\|', row['val']):
                        val = val.strip()
                        if {input.get('s1_2')} not in val:
                            sepRels.append({"id": row["id"], "key": row["key"], "val": val})

                # if there's saved data, it is set back before removing the purely unsaved data
                if len(sepRels) > 0:
                    grouped = {}
                    for r in sepRels:
                        grouped.setdefault((r['id'], r['key']), []).append(r['val'])

                    for (id_val, key), vals in grouped.items():
                        val_string = '; '.join(vals)
                        set_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE id(r) = {id_val}
                            SET r.{key} = split('{val_string}', '; ')
                        """
                        getQuery(set_query,driver=driver)
                        nullify_empty_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE id(r) = {id_val} AND size(r.{key}) = 0
                            SET r.{key} = NULL
                        """
                        getQuery(nullify_empty_query,driver=driver)
                # removing the purely unsaved data
                else:
                    for row in rels:
                        nullify_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE id(r) = {row["id"]}
                            SET r.{row["key"]} = NULL
                        """
                        getQuery(nullify_query,driver=driver)

                createLog(id=[row["id"] for row in rels], type="relation",
                      log=f"removed reference to deleted node {input.get('s1_2')}",
                      user=user, driver=driver)

        nodeID = getID(input.get('s1_2'), "CMID", driver)
        create_deleted_query = f"""
            MATCH (n) WHERE id(n) = {nodeID}
            CREATE (n2:DELETED)
            SET n2.CMID = n.CMID, n2.CMName = n.CMName, n2.log = n.log
            RETURN id(n2) AS nodeID
        """
        deletedID = getQuery(create_deleted_query,driver=driver)
        createLog(id=[deletedID[0]['nodeID']], type="node",
              log=[f"deleted node {input.get('s1_2')}"],
              user=user, driver=driver)

        deleteID(nodeID,driver,type="node")
        print("deleted node")
        return "done"

    except Exception as e:
        print(f"error deleting node: {str(e)}")

def deleteUSES(database,user,input):
    driver = getDriver(database)
    CMID = input.get('s1_2')
    USES_property = json.loads(input.get('s1_7'))
    id = USES_property[1]["id"]

    q = f"MATCH ()-[r]->() WHERE elementId(r) = '{id}' DELETE r RETURN count(*) AS count"
    result = getQuery(q, driver=driver)

    processUSES(database,CMID)

    print("Action completed")

    return "done"

def CMgetID(id_value, property_name, driver):
    """
    Given a node property value, returns the internal Neo4j node ID.
    Returns None if no node is found.
    
    Parameters:
    - id_value: str or int, the value to match
    - property_name: str, property key to search on
    - con: Neo4j driver session or wrapper with a run(query, params) method
    """
    # Convert id_value to string and trim whitespace
    id_str = str(id_value).strip()

    query = f"""
    UNWIND $id AS id
    MATCH (a)
    WHERE a.{property_name} = id
    RETURN id(a) AS id
    """

    # Run the query with parameters
    result = getQuery(query,driver, params={"id": [id_str]})
    
    # Collect records
    records = list(result)
    if len(records) > 0:
        return records[0]["id"]
    else:
        return None

def USESLogText(relid, driver):
    """
    Creates a custom log text for the uses tie that is to be moved.

    Parameters:
    - relid: str - relationshipID
    - driver: Neo4j session
    
    Returns:
    - pandas DataFrame with columns: logtext
    """
    query = """
    UNWIND $relid AS relid
    MATCH (a)-[r:USES]->(b)
    WHERE elementId(r) = relid
    RETURN coalesce(a.CMName,'NA') + '-' + type(r) + '-' + coalesce(r.Key,'') + '->' + coalesce(b.CMName,'NA') AS logtext
    """
    
    result = getQuery(query,driver, params={"relid": relid})
    logtext = result[0]["logtext"]
    
    return logtext

def check_ambiguous_ties_moveUSESties(driver,CMID_from,rel_id):
    # 1. Get dataset CMID linked to the relID
    query_dataset = """
    UNWIND $relID AS relID
    MATCH (d:DATASET)-[r:USES]->(:CATEGORY)
    WHERE elementId(r) = relID
    RETURN d.CMID AS datasetID
    """
    dataset_df = getQuery(query_dataset,driver,params = {'relID': rel_id})

    if dataset_df:
        dataset = dataset_df[0]['datasetID']
    else:
        return "No Dataset found for this USES tie."
    
    #checks if there are multiple uses ties from the same Dataset d to the from node p
    query_check_for_multiple_uses_ties = """
    UNWIND $fromCMID AS fromCMID
    UNWIND $dataset AS dataset
    MATCH (p:CATEGORY {CMID: fromCMID})<-[r:USES]-(d:DATASET {CMID: dataset})
    Return count(r) as uses_count
    """

    uses_count = getQuery(query_check_for_multiple_uses_ties,driver,params= {'fromCMID': [CMID_from], 'dataset': [dataset]})
    uses_count = uses_count[0]['uses_count']

    # do any contextual children of fromNode A have a USES tie from D that includes A as a property
    query = """
        UNWIND $fromCMID AS fromCMID
        UNWIND $dataset AS dataset
        MATCH (p:PROPERTY)
        WHERE p.relationship IS NOT NULL
        WITH collect(p.CMName) AS prop_CMNames,fromCMID,dataset

        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET {CMID: dataset})
        WHERE any(k IN prop_CMNames WHERE fromCMID IN r[k])
        RETURN c.CMID as CMID, r.Key as Key
        """

    child_USES_check = getQuery(query,driver,params= {'fromCMID': [CMID_from], 'dataset': [dataset]})

    if uses_count > 1 and child_USES_check:
        return jsonify({
            "status" : "True",
            "dataset": dataset,
            "child_USES_check": child_USES_check
        })
    else:
        return jsonify({"status" : "False",
                        "dataset": dataset,
                        "child_USES_check": child_USES_check})

# table data includes user decisions about ambiguous parents
def moveUSESties(database,user,input,dataset,tabledata):
    driver = getDriver(database)
    CMID_from = input.get('s1_2')
    CMID_to = input.get('s1_3')
    USES_property = json.loads(input.get('s1_7'))
    rel_id = USES_property[1]["id"]
    # only need to revise operation if user wants to keep some parent-child ties with the FROM node.
    USES_to_change = [row for row in tabledata if row['optionA'] != 'From']

    try:
        if len(USES_to_change) > 0:

            query_update_parents = """
            UNWIND $changes AS change
            MATCH (c {CMID: change.cmid})<-[r:USES {Key: change.Key}]-(d:DATASET {CMID: $dataset})
            WITH c, d, r, $old AS old, $new AS new

            WITH c, d, r, old, new, [x IN r.parentContext WHERE x IS NOT NULL | 
                CASE 
                    WHEN apoc.convert.fromJsonMap(x).parent = old 
                    THEN apoc.convert.toJson(apoc.map.setKey(apoc.convert.fromJsonMap(x), 'parent', new))
                    ELSE x 
                END
            ] AS updatedParentContext
            SET r.parentContext = updatedParentContext

            WITH c, d, r, old, new

            MATCH (p:PROPERTY) WHERE p.relationship IS NOT NULL
            WITH c, d, r,old,new, collect(p.CMName) AS prop_CMNames

            UNWIND prop_CMNames AS propName
            WITH c, d, r, old, new, propName
            WHERE r[propName] IS NOT NULL
            SET r[propName] = [element IN r[propName] | CASE WHEN element = old THEN new ELSE element END]

            RETURN c.CMID AS CMID, r, d.CMID AS datasetID
            """
            print(CMID_from)
            print(CMID_to)
            result = getQuery(query_update_parents,driver,params = {
                'changes': [{"cmid": row['CMID'], "Key": row['Key']} for row in USES_to_change],
                'old': CMID_from,
                'new': CMID_to,
                'dataset': dataset,
            })

            print("completed moving props")

            processUSES(CMID=[row['CMID']for row in USES_to_change], database=database)

        # Move the relationship itself
        # Fetch relationship details for log
        logtext = USESLogText(rel_id,driver)
        
        log_msg = f"moved relationship {logtext} from {CMID_from} to {CMID_to}"

        query_move_rel = f"""
        MATCH ()-[r:USES]->(from)
        WHERE from.CMID = '{CMID_from}' AND elementId(r) = '{rel_id}'
        MATCH (to)
        WHERE to.CMID = '{CMID_to}'
        CALL apoc.refactor.to(r, to) YIELD input, output
        RETURN elementId(output) AS relID
        """
        rel_id_df = getQuery(query_move_rel,driver)
        new_rel_id = rel_id_df[0]['relID'] if rel_id_df else None

        #Logging
        from_node_id = CMgetID(CMID_from, "CMID", driver)
        to_node_id = CMgetID(CMID_to, "CMID", driver)

        #CMlog(id=from_node_id, type_="node", log=log_msg, user=user, con=con)
        #CMlog(id=to_node_id, type_="node", log=log_msg, user=user, con=con)

        try:
            if new_rel_id is not None:
                createLog(id=new_rel_id, type="relation", log=log_msg, user=user, driver=driver)
        except Exception as e:
            print(f"Warning while logging relation: {e}")

    except Exception as e:
        return str(e)

    # Final updates and notifications
    print("move completed: updating USES ties")
    processUSES(CMID=[CMID_from, CMID_to], database=database)
    print("Completed USES ties update")

    return "done"




