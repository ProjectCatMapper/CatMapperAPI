''' admin.py '''

############################
# Code for the following admin functions
# 1.  add, edit, delete USES tie properties
# 2.  add, edit, delete NODE properties
# 3.  Merge nodes
# 4.  Move uses ties
# 5.  delete Node
# 6.  delete USES relation
# 7.  create label
############################

from .utils import *
from .log import createLog
from .USES import processUSES
from .USES import addCMNameRel,processDATASETs,waitingUSES
from .upload import updateProperty,createUSES
from flask import jsonify
from itertools import groupby
from collections import Counter

# This is a module for admin functions in CatMapper

import re
from neo4j import GraphDatabase

############################
#section for general use helper functions

#Function used by deleteNode to get elementId for either a single or list of CMIDs
#deletenode only needs to operate on a string
def getID(id_value, property, driver):
    """
    Given a node property CMID, returns the internal Neo4j element ID.
    Returns None if no node is found.
    
    Parameters:
    - id_value: str or int, the value to match
    - property: str, property CMID
    - driver: Neo4j driver session or wrapper with a run(query, params) method
    """
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
        RETURN elementId(a) AS id
    """
    result = getQuery(query=query, params={"id": id_list}, driver=driver)

    # Return first result if any, else None
    return result[0]["id"] if result else None

def getGroupLabels(CMID,driver):

    query = """
    UNWIND $cmid AS cmid
    MATCH (n)
    WHERE n.CMID = cmid
    RETURN labels(n)
    """
    result = getQuery(query=query, params={"cmid": [CMID]}, driver=driver,type="list")

    result = result[0]
    if "CATEGORY" in result:
        result.remove("CATEGORY")

    if not result:
        raise Exception(f"{CMID} has improper labels, unable to verify grouplabels")
        
    result = result[0]

    query = """
    UNWIND $label AS label
    MATCH (m:LABEL)
    WHERE m.CMName = label
    RETURN m.groupLabel AS groupLabel
    """
    result = getQuery(query=query, params={"label": result}, driver=driver)
    result = result[0]['groupLabel']

    return result

def validatePropertyCMID(value,proptoChange,validgroupLabel,driver):
    if "||" in value:
            value = value.split("||")
    else:
        value = [value]
    for val in value:
        val = val.strip()

        validprop = isValidCMID(val, driver)

        if len(validprop) == 0:
            raise Exception(f"{val} is invalid")

        grouplabel = getGroupLabels(val,driver)
        #permits GENERICS as parents of other domains
        if not (proptoChange == "parent" and grouplabel == "GENERIC"):
            if validgroupLabel != grouplabel:
                raise Exception(f"All {proptoChange} CMIDS should be a {validgroupLabel}")

def validate_parent_context_list(driver,parent_context_list):
    """
    parent_context_list: list of JSON strings
    """

    VALID_EVENT_TYPES = {
    "SPLIT",
    "HIERARCHY",
    "SPLITMERGE",
    "MERGED",
    "FOLLOWS",
    }

    CURRENT_YEAR = datetime.now().year

    errors = []

    for idx, raw in enumerate(parent_context_list):
        # here we load the string as a JSON element
        try:
            # use object_pairs_hook to get all key occurrences
            def check_duplicates(pairs):
                keys = [k for k, _ in pairs]
                counts = Counter(keys)
                duplicates = [k for k, v in counts.items() if v > 1]
                if duplicates:
                    raise ValueError(f"Duplicate keys found: {duplicates}")
                return dict(pairs)
            
            pc = json.loads(raw, object_pairs_hook=check_duplicates)
        except ValueError as ve:
            errors.append((idx, str(ve)))
            continue
        except Exception as e:
            errors.append((idx, f"Invalid JSON:{e}"))
            continue

        # 1️⃣ Must be a dict
        if not isinstance(pc, dict):
            errors.append((idx, "parentContext entry is not an object"))
            continue

        # 2️⃣ Must contain ONLY allowed keys
        allowed_keys = {"parent", "eventType", "eventDate"}
        extra_keys = set(pc.keys()) - allowed_keys
        missing_keys = {"parent"} - set(pc.keys())

        if extra_keys:
            errors.append((idx, f"Extra keys found: {extra_keys}"))
            continue

        if missing_keys:
            errors.append((idx, f"Missing required keys: {missing_keys}"))
            continue

        parent = pc.get("parent")
        event_type = pc.get("eventType")
        event_date = pc.get("eventDate")

        # 3️⃣ parent validation
        if not isinstance(parent, str):
            errors.append((idx, "Invalid parent value - parent is not string"))
            continue

        q = """
            unwind $CMID as cmid
            RETURN EXISTS { MATCH (p { CMID: cmid })} AS cmidExists
            """
        result = getQuery(q,driver=driver,params = {"CMID": parent})

        if not result[0]['cmidExists']:
            errors.append((idx, "Parent CMID not in database"))
            continue

        # 4️⃣ eventType validation
        if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
            errors.append((idx, f"Invalid eventType: {event_type}"))
            continue

        # 5️⃣ eventDate validation
        if event_date not in (None, ""):
            if event_type in (None, ""):
                errors.append((idx, "eventType is required when eventDate exists."))
                continue

            if not isinstance(event_date, int):
                errors.append((idx, "eventDate must be an integer year"))
                continue

            if event_date > CURRENT_YEAR:
                errors.append((idx, f"eventDate out of range: {event_date}"))
                continue

    return errors
        
############################
#section for add, edit, delete USES ties
#Function for editing USES tie properties.
def add_edit_delete_USES(database,user,input):
    CMID = input.get('s1_2')
    USES_property = input.get('s1_8')
    new_property_value = input.get('s1_3').strip()
    addOrEditNode = input.get('s1_1')
    indexValue = input.get('s1_7')
    key = input.get('s1_4')[indexValue-1][1]["Key"]
    datasetID = input.get('s1_4')[indexValue-1][2]["CMID"]

    driver = getDriver(database)

    metaTypes = getPropertiesMetadata(driver)
    metaType = [item['metaType'] for item in metaTypes if item["type"] == "relationship" and item["property"] == USES_property]
    
    # When adding or editing properties, checks to make sure CMIDs are valid and the labels are correct.
    if addOrEditNode != "delete":
        if USES_property == "parent":
            groupLabel = getGroupLabels(CMID,driver)
            if "||" in new_property_value:
                for i in new_property_value.split("||"):
                    validatePropertyCMID(i,USES_property,groupLabel,driver)
            else:
                validatePropertyCMID(new_property_value,USES_property,groupLabel,driver)
        else:
            query = """
                    UNWIND $prop as prop
                    MATCH (n:PROPERTY)
                    WHERE n.CMName = prop
                    RETURN n.groupLabel as groupLabel
                    """
            groupLabel = getQuery(query=query, params={"prop": USES_property}, driver=driver)
            groupLabel = groupLabel[0]['groupLabel']
            if groupLabel:
                validatePropertyCMID(new_property_value,USES_property,groupLabel,driver)
        
    if "list" in metaType:        
        if "||" in new_property_value:
            new_property_value=new_property_value.split("||")
        else:
            new_property_value=[new_property_value]
    
    if addOrEditNode == "edit" or addOrEditNode == "add":
        if USES_property == "Key":
            data = {
                'CMID': CMID,
                'Key': key,
                'datasetID': datasetID,
                "NewKey": new_property_value
            }
            USES_property = ["NewKey"]

            query = """UNWIND $rows AS row
                OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.NewKey}]->(b:CATEGORY {CMID: row.CMID})
                RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.NewKey AS Key, COUNT(r) AS rel_count"""
        
            with driver.session() as session:
                results = session.run(query, rows=data)
                keyExists = [
                    (r["CMID"], r["datasetID"], r["Key"])
                    for r in results.data()
                    if r["rel_count"] >= 1
                ]

                if keyExists:
                    raise ValueError(
                        f"Error:CMID, Key and datasetID triplet already exists for {keyExists}"
                    )
        
        else:
            if USES_property == "parentContext":
                result = validate_parent_context_list(driver,new_property_value)
                if result:
                    raise ValueError(
                        f"Error: {result}"
                    )

            data = {
                    'CMID': CMID,
                    'Key': key,
                    'datasetID': datasetID,
                    USES_property: new_property_value
                }
            USES_property = [USES_property]
        
        if CMID[1] == "D":
            isDataset = True
        elif CMID[1] == "M":
            isDataset = False
              
        df = pd.DataFrame([data])
        # Arguments coming from admin have already been parsed into lists
        updateProperty(df,USES_property,isDataset,database,user,updateType = "overwrite", propertyType="USES")
        processUSES(CMID=CMID,database=database,user=user)
    elif addOrEditNode == "delete":
        q = f"""
                MATCH (a:CATEGORY {{CMID: $CMID}})<-[r:USES {{Key: $key}}]-(d:DATASET {{CMID: $datasetID}})
                REMOVE r[$USES_property] RETURN elementId(r) as relID
            """
        params = {
            "CMID": CMID,
            "key": key,
            "datasetID": datasetID,
            "USES_property": USES_property
        }
        result = getQuery(q,driver=driver,params = params)
        processUSES(CMID=CMID,database=database,user=user)

        createLog(id=[row["relID"] for row in result], type="relation",
                        log=f"deleted USES property {input.get('s1_8')}",
                        user=user, driver=driver)

    return "done"

############################
#section for add, edit, delete node ties
#Function that allows editing Node properties.
def add_edit_delete_Node(database,user,input):
    changeNodeID = input.get('s1_2')
    changeNodeProperty = input.get('s1_7')
    changeNodeValue = input.get('s1_3').strip()
    addOrEditNode = input.get('s1_1')

    driver = getDriver(database)

    if changeNodeProperty == "parent":
        validatePropertyCMID(changeNodeValue,changeNodeProperty,"DATASET",driver)
                
    if changeNodeProperty == "District":
        validatePropertyCMID(changeNodeValue,changeNodeProperty,"DISTRICT",driver)

    if not changeNodeID or not addOrEditNode:
        return "CMID is empty or choice of add/edit/delete is empty"
    
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
        RETURN a.{changeNodeProperty} AS val
    """
    #priorVal = CMCypherQuery(con=con, query=priorValQuery)
    priorVal = getQuery(priorValQuery,driver=driver,type='list')

    if addOrEditNode == "delete":
        if label == "DATASET" and changeNodeProperty in ["District", "parent"]:
            if changeNodeProperty == "District":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:DISTRICT_OF]-(c:DISTRICT)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)
            if changeNodeProperty == "parent":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:CONTAINS]-(c:DATASET)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)

        q = f"""
            MATCH (a {{CMID: '{changeNodeID}'}})
            SET a.{changeNodeProperty} = NULL
        """
        #CMCypherQuery(con=con, query=q)
        getQuery(q,driver=driver)

    else:  # edit or add
        if label == "DATASET" and changeNodeProperty in ["District", "parent"]:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeProperty} = split($id, ' || ')
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id":changeNodeValue})

            processDATASETs(database,CMID=changeNodeID,user=user)
        else:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeProperty} = $id
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id":changeNodeValue})

            if changeNodeProperty == "CMName":
                try:
                    #CMaddCMNameRel(CMID=changeNodeID, user=user, con=con)
                    addCMNameRel(database,CMID=changeNodeID)
                    normalize_single_query = """
                            MATCH (n)
                            WHERE n.CMID = $nodeID
                            AND (n:CATEGORY OR n:DATASET)
                            AND n.names IS NOT NULL
                            WITH n, [name IN n.names | toLower(apoc.text.clean(name))] AS cleaned
                            WITH n, apoc.coll.flatten([x IN cleaned | split(x, ' ')]) AS toks
                            SET n.normNames = apoc.coll.toSet([t IN toks WHERE t <> ''])
                            RETURN count(n) AS normalizedCount
                            """
                
                    getQuery(normalize_single_query,driver,params={"nodeID": changeNodeID})
                except Exception as e:
                    print(f"CMaddCMNameRel failed: {e}")

    new_val = "NULL" if addOrEditNode == "delete" else changeNodeValue
    log_msg = f"updated CMID {changeNodeID} {changeNodeProperty} from {priorVal} to {new_val}"
    #CMlog(id=changeNodeID, type="node", log=log_msg, user=user, con=con)
    return "updated successfully"

############################
#section for merging nodes
# when a node is merged, this finds all instances of that CMID in other
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

#Main function for merging nodes
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

        if len(validKeep) == 0:
            raise Exception(f"{keepcmid} is invalid")

        deleteKeep = isValidCMID(deletecmid, driver)

        results = results + ["checking if deletecmid is valid", deleteKeep]

        if len(deleteKeep) == 0:
            raise Exception(f"{deletecmid} is invalid")
        
        keep_label = getGroupLabels(keepcmid,driver)
        delete_label = getGroupLabels(deletecmid,driver)

        if keep_label != delete_label:
            raise Exception(f"The CMIDs are not of the same group label.")
        
        results = results + [addCMNameRel(database, keepcmid)]
        results = results + [addCMNameRel(database, deletecmid)]

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
        
        if domain == "CATEGORY":
            query = f"""
            MATCH (c)-[e1:EQUIVALENT]->(k {{CMID: $keepcmid}})
            MATCH (c)-[e2:EQUIVALENT]->(d {{CMID: $deletecmid}})
            WHERE e1.stack     = e2.stack
            AND e1.datasetID = e2.datasetID
            AND e1.key       = e2.key

            WITH e2

            DELETE e2
            """

            getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid)

            query = f"""
            MATCH (c)<-[e1:EQUIVALENT]-(k {{CMID: $keepcmid}})
            MATCH (c)<-[e2:EQUIVALENT]-(d {{CMID: $deletecmid}})
            WHERE e1.stack     = e2.stack
            AND e1.datasetID = e2.datasetID
            AND e1.key       = e2.key

            WITH  e2

            DELETE e2
            """

            getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid)
        
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

        merged = getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid, type = "list")
        if not keepcmid in merged:
            raise Exception(f"Failed to merge {deletecmid} into {keepcmid}")

        # create deleted node and "IS" relationship to remaining node
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
            # this query does a merge with errors such as turning lists into strings
            query = """
            unwind $cmid as cmid 
            match (:DATASET {CMID: "SD11"})-[r:USES]->({CMID: cmid}) 
            with collect(r) as rels

            WITH CASE
                    WHEN elementId(head(rels)) IN $relID THEN rels
                    ELSE reverse(rels)
                END AS relIDfirst

            MATCH (p:PROPERTY) 
            WHERE p.type = "relationship" 
                AND p.metaType = "list" 
                AND p.CMName <> "language"
            WITH relIDfirst, collect(p.CMName) AS listProps

            MATCH (p:PROPERTY) 
            WHERE p.type = "relationship" 
                AND p.metaType = "string" 
            WITH relIDfirst, listProps, collect(p.CMName) AS stringProps

            WITH relIDfirst,
                [prop IN listProps | [prop,'combine']] +
                    [prop IN stringProps | [prop,'retain']] +
                    [['language','retain']] AS allProps,listProps+['language'] AS listPropsPluslanguage
            
            WITH relIDfirst, apoc.map.fromPairs(allProps) AS props,listPropsPluslanguage

            call apoc.refactor.mergeRelationships(relIDfirst,{properties: props,singleElementAsArray: listPropsPluslanguage}) yield rel 
            
            RETURN count(*) AS mergedCount
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
        
        normalize_single_query = """
        MATCH (n)
        WHERE n.CMID = $nodeID
        AND (n:CATEGORY OR n:DATASET)
        AND n.names IS NOT NULL
        WITH n, [name IN n.names | toLower(apoc.text.clean(name))] AS cleaned
        WITH n, apoc.coll.flatten([x IN cleaned | split(x, ' ')]) AS toks
        SET n.normNames = apoc.coll.toSet([t IN toks WHERE t <> ''])
        RETURN count(n) AS normalizedCount
        """
    
        getQuery(normalize_single_query,driver,params={"nodeID": keepcmid})

        return results

    except Exception as e:
        return str(e), 500
    
############################
#section for moving USES ties

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

#When moving USES tie U from dataset D from category node A to B, if U defines contextual children from A to C and there are multiple uses ties from D to A, 
#this creates ambiguity in whether C should be a child of A or B.
#This function detects that issue and leads to the user being prompted to make decisions about this ambiguity.
def check_ambiguous_ties_moveUSESties(driver,CMID_from,CMID_to,rel_id):
    
    try:
        #checks to see if CMID is valid
        validCMID_to = isValidCMID(CMID_to, driver)

        if CMID_from == CMID_to:
            raise Exception(f"Both CMIDs cannot be the same.")

        if len(validCMID_to) == 0:
            raise Exception(f"{CMID_to} is invalid")
        
        # gets labels of uses tie using relID, then gets groupLabel of the label and returns it.
        query = """
                MATCH ()-[r:USES]->()
                WHERE elementId(r) = $relID
                WITH r.label AS label
                MATCH (m:LABEL {CMName: label})
                RETURN m.groupLabel as groupLabel
                """

        uses_label = getQuery(query,driver,params = {'relID': rel_id})

        if uses_label:
            uses_label = uses_label[0]['groupLabel']
        else:
            return "No label found for this USES tie."
        
        #checks to see if uses tie labels is consistent with label of destination node
        to_label = getGroupLabels(CMID_to,driver)
        #from_label = getGroupLabels(uses_label,driver)

        if to_label != uses_label:
            raise Exception(f"The CMIDs are not of the same group label.")
        
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
    except Exception as e:
        return {"error": str(e)},500

# table data includes user decisions about ambiguous parents
#Function that moves uses tie from one category node to another category node
def moveUSESties(database,user,input,dataset,tabledata):
    driver = getDriver(database)
    CMID_from = input.get('s1_2').strip()
    CMID_to = input.get('s1_3').strip()
    USES_property = json.loads(input.get('s1_7'))
    rel_id = USES_property[1]["id"]
    # only need to revise operation if user wants to keep some parent-child ties with the FROM node.
    USES_to_change = [row for row in tabledata if row['optionA'] != 'From']
    
    if len(USES_to_change) > 0:

        try:
            query = """
                MATCH (d1)-[r:USES]->(c1)
                WHERE c1.CMID = $CMID_from AND elementId(r) = $rel_id

                WITH c1, r.Key as Key, d1.CMID as datasetID

                OPTIONAL MATCH (s:STACK)-[:MERGING]->(d1)
                WHERE d1.CMID = datasetID

                WITH c1, Key, datasetID, s.CMID as stackID

                OPTIONAL MATCH (c1)-[e:EQUIVALENT]->(c3)
                WHERE e.stack = stackID AND e.dataset = datasetID AND e.Key = Key

                WITH c1, e, c3, $CMID_to AS c2CMID
                WHERE e IS NOT NULL

                MATCH (c2:CATEGORY)
                WHERE c2.CMID = c2CMID
                CREATE (c2)-[newEq:EQUIVALENT {stack: e.stack,dataset: e.dataset,Key: e.Key}]->(c3)
                DELETE e
                RETURN 'Moved equivalence', c1.CMID AS oldCategory, c2.CMID AS newCategory, c3.CMID AS target
                """
                    
            result = getQuery(query,driver,params = {"CMID_from": CMID_from, "rel_id": rel_id, "CMID_to": CMID_to})
            
        except Exception as e:
            return "Error occurred while moving equivalence ties."
        
        try:
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
            
            result = getQuery(query_update_parents,driver,params = {
                'changes': [{"cmid": row['CMID'], "Key": row['Key']} for row in USES_to_change],
                'old': CMID_from,
                'new': CMID_to,
                'dataset': dataset,
            })

            print("completed moving props")

            processUSES(CMID=[row['CMID']for row in USES_to_change], database=database)
        except Exception as e:
            return "Error occurred while moving USES ties."
    
    # Move the relationship itself
    # Fetch relationship details for log
    try:
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
        from_node_id = getID(CMID_from, "CMID", driver)
        to_node_id = getID(CMID_to, "CMID", driver)

        #CMlog(id=from_node_id, type_="node", log=log_msg, user=user, con=con)
        #CMlog(id=to_node_id, type_="node", log=log_msg, user=user, con=con)

    
        if new_rel_id is not None:
            createLog(id=new_rel_id, type="relation", log=log_msg, user=user, driver=driver)
    except Exception as e:
        return f"Warning while logging relation: {e}"
            
    # Final updates and notifications
    print("move completed: updating USES ties")
    processUSES(CMID=[CMID_from, CMID_to], database=database)

    normalize_single_query = """
        MATCH (n)
        WHERE n.CMID = $nodeID
        AND (n:CATEGORY OR n:DATASET)
        AND n.names IS NOT NULL
        WITH n, [name IN n.names | toLower(apoc.text.clean(name))] AS cleaned
        WITH n, apoc.coll.flatten([x IN cleaned | split(x, ' ')]) AS toks
        SET n.normNames = apoc.coll.toSet([t IN toks WHERE t <> ''])
        RETURN count(n) AS normalizedCount
        """
    
    getQuery(normalize_single_query,driver,params={"nodeID": CMID_to})

    print("Completed USES ties update")

    return "done"

############################
#section for deleting a Node

# Function used by deleteNode to get the label of the node to differentiate b/w DATASET or not
# only returns CATGEORY or DATASET
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

# Helper function which does the delete operation for delete Node
def deleteID(id_value, driver, type="node"):
    # Validate and coerce input to integer or list of integers
    if isinstance(id_value, str):
        id_list = [id_value]
    elif isinstance(id_value, list):
        try:
            id_list = [str(i) for i in id_value]
        except (ValueError, TypeError):
            raise ValueError("id should be an string or list of strings")
    else:
        raise ValueError("id should be an string or list of strings")

    if type not in ["relationship", "node"]:
        raise ValueError("type should be 'relationship' or 'node'")

    count_deleted = 0
    queries = []

    if type == "relationship":
        for id in id_list:
            q = f"MATCH ()-[r]->() WHERE id(r) = '{id}' DELETE r RETURN count(*) AS count"
            queries.append(q)
    else:  # type == "node"
        for id in id_list:
            q = f"MATCH (a) WHERE elementId(a) = '{id}' DETACH DELETE a RETURN count(*) AS count"
            queries.append(q)

    for query in queries:
        result = getQuery(query, driver=driver)
        if result and "count" in result[0]:
            count_deleted += result[0]["count"]

    return f"Deleted {count_deleted} of type {type}"

# Function that deletes node, cleans up properties with that CMID, and re-assigns the node the DELETED label
def deleteNode(database,user,input):
    driver = getDriver(database)

    try:
        label = getLabel(input.get('s1_2'),driver,filter=True)
        
        # If you delete a dataset node, need to remove it’s CMID from all parent properties 
        # in other dataset nodes and Dataset in USES ties
        if "DATASET" in label:

            query = """
                    unwind $cmid as cmid
                    MATCH ()-[r:EQUIVALENT]->()
                    WHERE r.datasetID = $cmid
                    RETURN DISTINCT r.stackID AS stackID
                    """

            result = getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

            if result:
                raise ValueError(f"this dataset can't be deleted, because it is used by an existing stack for a merge(stackID={result}).")

            # if a merging template is being deleted, need to remove references to that merging template from all equivalence ties.            
            if "STACK" in label:
                query = """
                    unwind $cmid as cmid
                    MATCH ()-[r:EQUIVALENT]->()
                    WHERE r.stack = $cmid
                    DELETE r
                    """

                getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

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

            query = """
                    MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
                    WHERE c.CMID = $cmid

                    WITH c.CMID as cmid, r.Key as Key, d.CMID as datasetID

                    MATCH ()-[e:EQUIVALENT]->()
                    WHERE e.key = Key
                    AND e.datasetID = datasetID
                    
                    RETURN DISTINCT
                    e.stackID as stackID
                    """

            result = getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

            if result:
                raise ValueError(f"This category can't be deleted, because it is used by an existing stack for a merge(stackID={result}).")

            print("1")

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
                WHERE $cmid IN d.District
                RETURN id(d) AS ids
            """
            datasetIDs = getQuery(datasetIDs_query,driver=driver,params = {"cmid": input.get("s1_2")})

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

                sepRels = []
                for row in rels:
                    vals = row['val']
                    if isinstance(vals, list):
                        vals = vals  # already a list
                    else:
                        vals = re.split(r' \|\|', vals)  # split string

                    for val in vals:
                        val = val.strip()
                        if input.get('s1_2') not in val:
                            sepRels.append({"id": row["id"], "key": row["key"], "val": val})
                    # for val in re.split(r' \|\|', row['val']):
                    # #for val in row['val']:
                    #     val = val.strip()
                    #     if input.get('s1_2') not in val:
                    #         sepRels.append({"id": row["id"], "key": row["key"], "val": val})
                                
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
            MATCH (n) WHERE elementId(n) = '{nodeID}'
            CREATE (n2:DELETED)
            SET n2.CMID = n.CMID, n2.CMName = n.CMName, n2.log = n.log
            RETURN elementId(n2) AS nodeID
        """
        deletedID = getQuery(create_deleted_query,driver=driver)
        print(deletedID)
        print("Stgae 3")
        createLog(id=[deletedID[0]['nodeID']], type="node",
              log=[f"deleted node {input.get('s1_2')}"],
              user=user, driver=driver)

        deleteID(nodeID,driver,type="node")
        print("deleted node")
        return "done"

    except Exception as e:
        print(f"error deleting node: {str(e)}")

############################
#section for deleting a USES tie
def deleteUSES(database,user,input):
    driver = getDriver(database)
    CMID = input.get('s1_2')
    USES_property = json.loads(input.get('s1_7'))
    id = USES_property[1]["id"]

    q = f"""MATCH (a)-[r:USES]->(d:DATASET)
            WHERE elementId(r) = '{id}'

            WITH d.CMID as datasetID,a.CMID as CMID, r.Key as Key
            OPTIONAL MATCH (a)-[e:EQUIVALENT]->(target)
            WHERE e.key = Key AND e.datasetID = datasetID
            WITH CMID, Key, datasetID, e.stackID as stackID

            WHERE e IS NOT NULL

            RETURN 
            CMID,
            datasetID,
            Key,
            stackID
            """
    
    result = getQuery(q, driver=driver)

    if result:
        raise ValueError(f"There is a stack that uses this USES tie for a merging template. It is not possible to delete this USES tie using admin functions.(stackID = {result})")

    q = f"MATCH ()-[r]->() WHERE elementId(r) = '{id}' DELETE r RETURN count(*) AS count"
    result = getQuery(q, driver=driver)

    processUSES(database,CMID)

    return "done"

############################
#section for creating a new label
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
        displayOrder = 4
    else:
        grouplabel = input.get('s1_7').strip()
        displayOrder = 100
    
    if input.get('s1_6') == "":
        color = "#404040"
    else:
        color = input.get('s1_6')
    
    if input.get('s1_3').strip() != "":       
        q = f"""CREATE (n:METADATA:LABEL {{CMID:'{CMID}',CMName:'{input.get('s1_2')}',groupLabel:'{grouplabel}',relationship:'{input.get('s1_3')}',description:'{input.get('s1_4')}',displayName:'{input.get('s1_5')}',color:'{color}',label:'{input.get('s1_5')}',displayOrder:'{displayOrder}',public:"TRUE"}})"""
    else:
        q = f"""CREATE (n:METADATA:LABEL {{CMID:'{CMID}',CMName:'{input.get('s1_2')}',groupLabel:'{grouplabel}',description:'{input.get('s1_4')}',displayName:'{input.get('s1_5')}',color:'{color}',label:'{input.get('s1_5')}',displayOrder:'{displayOrder}',public:"TRUE"}})"""


    result = getQuery(q,driver=driver)

    # create index after creation

    q =    """UNWIND $label_name as label_name
            match (d:METADATA:LABEL)
            where (d.public = true or tolower(d.public) = "true") AND d.CMName= label_name
            with d.label as l
            call apoc.cypher.runSchema('CREATE FULLTEXT INDEX ' + l + ' IF NOT EXISTS FOR (n:' + l + ') ON EACH [n.names]',{}) yield value return count(*)"""

    result = getQuery(q,driver=driver,params={"label_name":input.get('s1_5')})

    return "done"


def mergeUSESties(database, CMID, Key, datasetID):
    """
    Merge all `USES` relationships between a CATEGORY node and a DATASET node 
    in a Neo4j database while respecting constraints on which properties can 
    or cannot be combined.

    Parameters
    ----------
    database : str
        Name of the Neo4j database to connect to.
    CMID : str
        The unique identifier of the CATEGORY node.
    Key : str
        The key value identifying the specific USES relationships to merge.
    datasetID : str
        The unique identifier of the DATASET node.

    Returns
    -------
    str
        Success message confirming that the USES relationships were merged.

    Raises
    ------
    Exception
        If multiple distinct values are found for a non-combinable property, 
        or if no relationships are found to merge.
    """

    # Obtain a Neo4j driver connection for the specified database.
    driver = getDriver(database)

    # Query for properties that cannot be combined when merging relationships.
    props_query = """
    MATCH (p:PROPERTY)
    WHERE p.type = "relationship" AND p.metaType = "string"
    RETURN p.CMName AS property
    """
    non_combinable_props = getQuery(props_query, driver=driver)

    # Query for all existing USES relationships matching the given identifiers,
    # and extract property-value pairs for the non-combinable properties.
    existing_query = """
    MATCH (:CATEGORY {CMID: $cmid})<-[r:USES {Key: $key}]-(:DATASET {CMID: $datasetID})
    UNWIND keys(r) AS prop
    WITH prop, r
    WHERE prop IN $props AND r[prop] IS NOT NULL
    RETURN DISTINCT prop AS property, r[prop] AS value
    """
    existing_props = getQuery(
        existing_query,
        driver=driver,
        params={
            "cmid": CMID,
            "key": Key,
            "datasetID": datasetID,
            "props": [row['property'] for row in non_combinable_props]
        }
    )

    # Check for any non-combinable properties that have conflicting values.
    # If found, merging is aborted to prevent data loss or inconsistency.
    for row in existing_props:
        if isinstance(row['value'], list) and len(set(row['value'])) > 1:
            raise Exception(
                f"Cannot merge USES ties for CMID {CMID} with Key {Key} in Dataset {datasetID} "
                f"due to multiple distinct values for property {row['property']}"
            )

    # Construct a property merge map defining which properties to combine or discard.
    # Non-combinable properties are discarded; all others are combined.
    properties_map = {row['property']: 'discard' for row in non_combinable_props}
    properties_map[".*"] = 'combine'  # Default: combine all unspecified properties.

    # Execute the merge operation using APOC's refactor.mergeRelationships procedure.
    merge_query = """
    MATCH (:CATEGORY {CMID: $cmid})<-[r:USES {Key: $key}]-(:DATASET {CMID: $datasetID})
    WITH collect(r) AS rels
    CALL apoc.refactor.mergeRelationships(rels, {properties: $propsMap}) YIELD rel
    RETURN count(rel) AS mergedCount
    """
    result = getQuery(
        merge_query,
        driver=driver,
        params={
            "cmid": CMID,
            "key": Key,
            "datasetID": datasetID,
            "propsMap": properties_map
        }
    )

    # Raise an error if no relationships were merged (indicating a mismatch or missing ties).
    if result[0]['mergedCount'] == 0:
        raise Exception(
            f"No USES ties found to merge for CMID {CMID} with Key {Key} in Dataset {datasetID}"
        )

    # Return success confirmation.
    return f"Merged USES ties successfully for CMID {CMID} with Key {Key} in Dataset {datasetID}"





############################
#section for potentially deprecating functions
