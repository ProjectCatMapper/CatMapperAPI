''' USES.py '''

from .utils import *
from .metadata import getPropertiesMetadata
from datetime import datetime
from .log import createLog

def mergeUSES(database, CMID, Key, datasetID, properties = None):
    """
    Merge USES relationships in the database.

    Parameters:
    - database: The database connection object.
    - CMID: The CMID to filter the relationships.
    - Key: The key to filter the relationships.
    - datasetID: The dataset ID to filter the relationships.
    - properties: Optional; a list of properties to combine the relationships.

    Returns:
    - result: The result of the merge operation or an error message.
    """
    driver = getDriver(database)

    if isinstance(CMID, list) and len(CMID) > 1:
        raise ValueError("CMID must be a single value, not a list.")
    if isinstance(Key, list) and len(Key) > 1:
        raise ValueError("Key must be a single value, not a list.")
    if isinstance(datasetID, list) and len(datasetID) > 1:
        raise ValueError("datasetID must be a single value, not a list.")
    if isinstance(properties, list) and len(properties) > 1:
        raise ValueError("properties must be a single value, not a list.")
    
    CMID = unlist(CMID)
    Key = unlist(Key)
    datasetID = unlist(datasetID)
    properties = unlist(properties)
    
    if properties is None:
        properties = "{properties: {`.*`: 'combine'}}"
        
    query = f"""
    match (:DATASET {{CMID: $datasetID}})-[r:USES {{Key: $Key}}]->(:CATEGORY {{CMID: $CMID}})
    return count(r) as count
    """
    count = getQuery(query, driver=driver, params={"CMID": CMID, "Key": Key, "datasetID": datasetID}, type = 'list')

    query = f"""
    match (:DATASET {{CMID: $datasetID}})-[r:USES {{Key: $Key}}]->(:CATEGORY {{CMID: $CMID}})
    with collect(r) as rels
    call apoc.refactor.mergeRelationships(rels,{properties}) yield rel
    return count(*) as count
    """
    result = getQuery(query, driver=driver, params={"CMID": CMID, "Key": Key, "datasetID": datasetID}, type = 'list')

    return "Merged " + str(count[0]) + " relationship(s) into " + str(result[0]) + " relationship(s) for CMID " + f"'{CMID}' with Key '{Key}' and datasetID (CMID) '{datasetID}' using properties combined as {properties}" + "."
        
# merges duplicate contextual ties - not USES ties
def mergeDupRelations(database, CMID=None):
    try:
        driver = getDriver(database)
        if isinstance(CMID, str):
            CMID = [CMID]
        if isinstance(CMID, list) and len(CMID) == 0:
            CMID = None

        query = """
        MATCH ()-[r]->()
        WHERE type(r) <> 'USES'
        WITH r, startNode(r) AS a, endNode(r) AS b
        WHERE $cmid IS NULL OR a.CMID IN $cmid OR b.CMID IN $cmid
        WITH a, b, type(r) AS relType, collect(r) AS rels
        WHERE size(rels) > 1
        CALL apoc.refactor.mergeRelationships(rels, {properties: {`.*`: 'combine'}}) YIELD rel
        RETURN count(*) AS count
        """
        result = getQuery(query, driver=driver, params={"cmid": CMID})

        return result
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

# ensures reference Keys in contextual ties are correct.
def fixUsesRels(database, property, relationship, CMID=None):
    try:
        driver = getDriver(database)
        if property in ["country", "district"]:
            qProp = "collect(distinct coalesce(r.country, [])) + collect(distinct coalesce(r.district, []))"
        else:
            qProp = f"collect(distinct coalesce(r.{property}, []))"

        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "where a.CMID = cmid "
        else:
            qFiltera = ""
            qFilterb = ""

        query1 = f"""
        {qFiltera}
match (a:CATEGORY)<-[r:USES]-(:DATASET)
{qFilterb}
with a, [x in apoc.coll.flatten({qProp}, true) where x is not null and not x = ""] as rawCMIDs
with a, apoc.coll.toSet(rawCMIDs) as cmids
match (n:CATEGORY) where n.CMID in cmids
merge (a)<-[:{relationship}]-(n)
return count(*) as count
"""

        query2 = f"""
        {qFiltera}
      match (n)-[rel:{relationship}]->(a:CATEGORY)<-[r:USES]-(:DATASET)
      {qFilterb}
      with a, apoc.coll.toSet([x in apoc.coll.flatten({qProp}, true) where x is not null and not x = ""]) as current, 
      apoc.coll.toSet(apoc.coll.flatten(collect(distinct n.CMID),true)) as exists, collect(distinct rel) as rels, collect(n) as nodes
      with a, rels, nodes, [i in exists where not i in current] as extra
      unwind nodes as n unwind rels as rel with n,  rel, extra where n.CMID in extra and elementId(startNode(rel)) = elementId(n)
      delete rel
"""

        query3 = f"""
        {qFiltera}
     match (d:DATASET)-[rU:USES]->(a:CATEGORY)<-[rs:{relationship}]-(b:CATEGORY)

    {qFilterb}
     unwind rU.{property} as prop
     with d,a,b,rU,rs,prop
     where b.CMID = prop
     with distinct b, rs,a, collect(distinct d.shortName + " Key: " + rU.Key) as refKey
     set rs.referenceKey = refKey
"""
        data = getQuery(query=query1, driver=driver,
                        params={'cmid': CMID}, type="list")
        getQuery(query=query2, driver=driver, params={'cmid': CMID})
        getQuery(query=query3, driver=driver, params={'cmid': CMID})

        return {str(property): data}
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

# if no CMID is set, it updates labels for all
def updateLabels(database, CMID=None):
    try:
        driver = getDriver(database)
        logs = []
        if CMID is not None:
            msg = "setting CATEGORY label on CMID"
            logs.append(msg)
            count = getQuery(
                "unwind $CMID as cmid match (c {CMID: cmid})<-[:USES]-(:DATASET) where not 'CATEGORY' in labels(c) set c:CATEGORY return count(distinct c)", driver, params={"CMID": CMID}, type = "list")
            msg = f"Set CATEGORY label for {count[0]} nodes"
            logs.append(msg)
        else:
            msg = "setting CATEGORY label for all"
            logs.append(msg)
            count = getQuery(
                "match (c)<-[:USES]-(:DATASET) where not 'CATEGORY' in labels(c) set c:CATEGORY return count(distinct c)", driver, type = "list")
            msg = f"Set CATEGORY label for {count[0]} nodes"
            logs.append(msg)
            
        if CMID is not None:
            unwind = "unwind $cmid as cmid"
            match = "{CMID: cmid}"
        else:
            unwind = ""
            match = ""

        query = f"""
        {unwind}
        match (c:CATEGORY {match})<-[r:USES]-(:DATASET)
        where r.label is not null
        WITH c, apoc.coll.toSet(apoc.coll.flatten(collect(distinct [r.label,"CATEGORY"]), true)) AS labels
        CALL apoc.create.setLabels(c, labels) YIELD node
        RETURN count(distinct c)
        """
        if CMID is not None:
            label_results = getQuery(query=query, driver=driver,
                                params={"cmid": CMID}, type='list')
        else:
            label_results = getQuery(query=query, driver=driver, type='list')
            
        msg = f"Set labels for {label_results[0]} nodes"
        logs.append(msg)

        # adds grouplabels to nodes
        labels = getQuery(
            'match (l:LABEL) where l.public = "TRUE" and not l.label in ["CATEGORY","ALL NODES","ANY DOMAIN"] return distinct l.label as label, l.groupLabel as group', driver, type  = "df")

        for i in range(len(labels)):
            label = labels.loc[i, 'label']
            group = labels.loc[i, 'group']
            query = f"""
                {unwind}
                match (d:DATASET)-[r:USES]->(c:`{label}` {match}) 
                set c:`{group}`
                return count(distinct c)
            """
            if CMID is not None:
                group_results = getQuery(query=query, driver=driver,params={"cmid": CMID}, type='list')
            else:
                group_results = getQuery(query=query, driver=driver, type='list')
            msg = f"Set group label {group} for {group_results[0]} nodes with label {label}"
            logs.append(msg)
        return logs
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


def updateContains(database, CMID=None):
    try:
        driver = getDriver(database)
        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "where a.CMID = cmid"
        else:
            qFiltera = ""
            qFilterb = ""

        query1 = qFiltera + """
        match (:DATASET)-[rU:USES]->(a:CATEGORY)
        """ + qFilterb + """
        unwind rU.parentContext as propsL
        // Filter out non-string types if necessary
        with a, propsL where apoc.meta.cypher.type(propsL) = "STRING"

        // Parse JSON strings into lists or maps
        with a, 
        case 
            when propsL contains '[' then apoc.convert.fromJsonList(propsL) 
            when propsL contains '{' then apoc.convert.fromJsonMap(propsL) 
            else NULL 
        end as prop
        unwind prop as p

        // Match categories using parent CMID and prepare for collecting events
        match (a)<-[rC:CONTAINS]-(c:CATEGORY {CMID: p.parent})
        WITH rC, p,
                CASE
                    WHEN p.eventType IS NULL THEN []
                    WHEN apoc.meta.cypher.type(p.eventType) = "STRING" THEN [p.eventType]
                    ELSE p.eventType
                END AS eventType,
                CASE
                    WHEN p.eventDate IS NULL THEN []
                    WHEN apoc.meta.cypher.type(p.eventDate) = "STRING" THEN [p.eventDate]
                    ELSE p.eventDate
                END AS eventDate

        WITH rC, eventType, eventDate, range(0, size(eventType)-1) AS idxs
        UNWIND idxs AS i
        WITH rC, eventType[i] AS et,
            CASE WHEN size(eventDate) <= i THEN NULL ELSE eventDate[i] END AS ed
        WHERE et IS NOT NULL

        WITH rC,collect([et,ed]) AS eventPairs
        WITH rC,
            reduce(acc = [], pair IN eventPairs |
                CASE WHEN NOT pair IN acc THEN acc + [pair] ELSE acc END
            ) AS uniquePairs
        WITH rC,
                [p in uniquePairs | p[0]] AS eventType,
                [p in uniquePairs | p[1]] AS eventDate
        WITH rC,eventType,[x IN eventDate | coalesce(x, "NULL")] AS eventDate
        
        SET rC.eventType = eventType,
            rC.eventDate = eventDate
        
        RETURN count(rC) AS updatedRelationships
        """

        # apoc.coll.toSet(apoc.coll.flatten(collect(p.eventType))) as eventType, 
        # apoc.coll.toSet(apoc.coll.flatten(collect(p.eventDate))) as eventDate

        # // Update rC properties with events data using APOC
        # call {
        # with rC, eventDate
        # call apoc.do.when(
        #     size(eventDate) > 0,
        #     "set rC.eventDate = $eventDate",
        #     "set rC.eventDate = NULL",
        #     {rC: rC, eventDate: eventDate}
        # ) yield value
        # }
        # with rC, eventType
        # call {
        # with rC, eventType
        # call apoc.do.when(
        #     size(eventType) > 0,
        #     "set rC.eventType = $eventType",
        #     "set rC.eventType = NULL",
        #     {rC: rC, eventType: eventType}
        # ) yield value
        # }
        # return count(*)

        # When a contains tie going to node "a" has a non-empty eventDate list or eventType list, but there are no USES ties to "a" with a parentContext, then remove eventDate and eventType lists.
        query2 = qFiltera + """
        match (d:DATASET)-[rU:USES]->(a:CATEGORY)<-[rC:CONTAINS]-(p:CATEGORY) 
        """ + qFilterb + """
        with a,p, apoc.coll.flatten(collect(coalesce(rU.parentContext, [])),true) as pProps, apoc.coll.flatten(collect(coalesce(rC.eventDate, [])),true) as eds, apoc.coll.flatten(collect(coalesce(rC.eventType, [])),true) as ets
        with a,p, [i in pProps where i is not null and not i = ""] as pProps, [i in eds where i is not null and not i = ""] as eds, [i in ets where i is not null and not i = ""] as ets
        where isEmpty(pProps) and (not isEmpty(eds) or not isEmpty(ets))
        match (a)<-[r:CONTAINS]-(p) set r.eventDate = NULL, r.eventType = NULL
        return count(*) as count
        """

        result1 = getQuery(query1, driver, params={'cmid': CMID})
        result2 = getQuery(query2, driver, params={'cmid': CMID})

        return {"query1": result1, "query2": result2}
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

# if no CMID is set, it updates names for all
def updateAltNames(database, CMID=None, domain = "CATEGORY"):
    try:
        driver = getDriver(database)
        
        if isinstance(CMID, str):
            CMID = [CMID]
            
        if isinstance(CMID, list) and len(CMID) == 0:
            CMID = None
            
        if CMID is not None and isinstance(CMID, list):
            if CMID[0][1] == 'D':
                domain = "DATASET"
            elif CMID[0][1] == 'M':
                domain = "CATEGORY"
            else:
                raise ValueError("Invalid CMID format. Must start with 'AD/SD' or 'AM/SM'.")

        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "where n.CMID = cmid"
        else:
            qFiltera = ""
            qFilterb = ""

        if domain == "DATASET":
            query =  f"""
                    {qFiltera}
                    MATCH (n:DATASET {qFilterb})
                    WITH n,
                    [x IN [n.CMName, n.shortName, n.DatasetCitation] WHERE x IS NOT NULL] AS newNames
                    SET n.names = apoc.coll.toSet(
                    (CASE WHEN n.names IS NOT NULL THEN n.names ELSE [] END) + newNames
                    )
                    RETURN n.names
                    """
        elif domain == "CATEGORY":
            query = qFiltera + """
                    match (n:CATEGORY)<-[r:USES]-(:DATASET)
                    """ + qFilterb + """
                    with n, apoc.coll.toSet(apoc.coll.flatten(collect(distinct r.Name),true)) as names
                    set n.names = names
                    return count(n)
                    """
        else:
            raise ValueError("Invalid domain. Must be 'DATASET' or 'CATEGORY'.")
        getQuery(query, driver, params={'cmid': CMID}, type = "list")

        normalize_query = """
                            UNWIND $CMID as cmid
                            MATCH (n:CATEGORY|DATASET)
                            WHERE n.CMID = cmid
                            AND n.names IS NOT NULL
                            WITH n, [name IN n.names | toLower(apoc.text.clean(name))] AS cleaned
                            WITH n, apoc.coll.flatten([x IN cleaned | split(x, ' ')]) AS toks
                            SET n.normNames = apoc.coll.toSet([t IN toks WHERE t <> ''])
                            RETURN count(n) AS normalizedCount
                            """
                
        getQuery(normalize_query,driver,params={"CMID": CMID})

        return f"Completed updating alternate names for {CMID}" if CMID else f"Completed updating alternate names for all {domain} nodes."
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


def processUSES(database, CMID=None, user="0", detailed = True):
    try:
        driver = getDriver(database)
        mergeDupRelationsResults = None
        propertiesResults = []
        updateContainsResults = None
        updateLabelsResults = None
        updateAltNamesResults = None

        # Normalize CMID values so all downstream functions consistently receive
        # either None or a list.
        cmid_list = CMID
        if CMID is not None and not isinstance(CMID, list):
            cmid_list = [CMID]

        def unwrap_step(result, step_name):
            if isinstance(result, tuple) and len(result) == 2 and result[1] == 500:
                raise RuntimeError(f"{step_name} failed: {result[0]}")
            return result

        # Update alternative names
        print("updating alternate names")
        updateAltNamesResults = unwrap_step(
            updateAltNames(CMID=cmid_list, database=database),
            "updateAltNames"
        )

        # Update labels
        print("updating labels")
        updateLabelsResults = unwrap_step(
            updateLabels(CMID=cmid_list, database=database),
            "updateLabels"
        )

        # Fix duplicate relationships
        if cmid_list is not None:
            print("running merge duplicate relations")
            mergeDupRelationsResults = unwrap_step(
                mergeDupRelations(CMID=cmid_list, database=database),
                "mergeDupRelations"
            )
        else:
            mergeDupRelationsResults = unwrap_step(
                mergeDupRelations(CMID=None, database=database),
                "mergeDupRelations"
            )

        # Update structural properties and referenceKeys
        properties = getPropertiesMetadata(driver=driver)
        properties = [item for item in properties if item.get(
            'relationship') is not None]
        for property, relationship in zip([item['property'] for item in properties if 'property' in item], [item['relationship'] for item in properties if 'relationship' in item]):
            print(f"{property} {relationship} {cmid_list}")

            r = unwrap_step(
                fixUsesRels(CMID=cmid_list, property=property, relationship=relationship, database=database),
                f"fixUsesRels({property},{relationship})"
            )
            propertiesResults.append(r)

        # Update contains relationships
        print("updating contains")
        updateContainsResults = unwrap_step(
            updateContains(CMID=cmid_list, database=database),
            "updateContains"
        )

        #for singular CMID coming from admin functions
        if cmid_list:
            q = """Match (c:CATEGORY)<-[r:USES]-(d:DATASET)
                   where c.CMID IN $CMID_list and r.status is not null and r.status = 'update'
                   set r.status = NULL"""
            getQuery(q, driver=driver, params={"CMID_list": cmid_list})

        if detailed:
            return {
                "CMID": cmid_list,
                "mergeDupRelations": mergeDupRelationsResults,
                "properties": propertiesResults,
                "updateLabels": updateLabelsResults,
                "updateContains": updateContainsResults,
                "updateAltNames": updateAltNamesResults
            }
        else:
            return (
                f"Completed processing USES for {cmid_list}"
                if cmid_list and cmid_list != [None]
                else "Completed processing USES for all CATEGORY nodes."
            )
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

# gets all CMIDs based on r.status = 'update' and sends to processUSES in batches
#If no CMIDs are found , it does nothing
def waitingUSES(database, BATCH_SIZE=1000):
    try:
        driver = getDriver(database)
        CMID = getQuery(
            "Match (c:CATEGORY)<-[r:USES]-(d:DATASET) where r.status is not null and r.status = 'update' return c.CMID as CMID", driver, type='list')
        CMID = list(set(CMID))
        if CMID:
            for i in range(0, len(CMID), BATCH_SIZE):
                # Slice the CMID list to get the current batch
                batch = CMID[i:i + BATCH_SIZE]

                # Perform the update operation for the current batch
                processUSES(database=database, CMID=batch)

                # Optional: Print progress (useful for debugging or monitoring)
                print(
                    f"Processed batch {i // BATCH_SIZE + 1} with {len(batch)} CMIDs.")
            # this query maybe redundant, the status is set in processUSES
            getQuery("Match (c:CATEGORY)<-[r:USES]-(d:DATASET) where r.status is not null and r.status = 'update' set r.status = NULL", driver)
            result = f"Successfully updated {len(CMID)} CMIDs in batches of {BATCH_SIZE}."
        else:
            result = "Nothing to update"
        return result
    except Exception as e:
        try:
            return str(e), 500
        except:
            return "Error", 500


def addCMNameRel(database, CMID=None):
    """
    Add CMName to relationship if new node.

    Parameters:
    - driver: CatMapper connection object
    - CMID: Optional; the CMID to match (default is None)
    - user: The user identifier (default is "0")

    Returns:
    - None

    Example:
    - addCMNameRel(con=con)
    """
    try:
        if database is None:
            raise ValueError("Database is not specified")
        elif database.lower() == "sociomap":
            database = "SocioMap"
        elif database.lower() == "archamap":
            database = "ArchaMap"
        else:
            raise ValueError(
                f"database must be either 'SocioMap' or 'ArchaMap', but value was '{database}'")

        driver = getDriver(database)

        if CMID is None:
            q = ""
        else:
            q = "c.CMID in apoc.coll.flatten($cmids,true) and"

        datasetID = "SD11" if database == "SocioMap" else "AD941"

        query_1 = f'''
            MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET {{CMID: "{datasetID}"}})
            WHERE {q} NOT c.CMName IN c.names
            WITH DISTINCT c, r
            SET r.Name = r.Name + [c.CMName]
            return elementId(r) as relID
        '''

        result = getQuery(query_1, driver, params={"cmids": [CMID]}, type = "list")

        if len(result) > 0:
            createLog(
            id=result,
            type="relation",
            log=[
                "Added CMName to relationship"
            ],
            user="0",
            driver=driver,
        )


        query_2 = f'''
            MATCH (c:CATEGORY)
            WHERE {q} NOT c.CMName IN c.names AND NOT (c)<-[:USES]-(:DATASET {{CMID: "{datasetID}"}})
            WITH DISTINCT c
            MATCH (d:DATASET) WHERE d.CMID = "{datasetID}"
            CREATE (c)<-[r:USES]-(d)
            SET r.Key = "Key == " + c.CMID,
                r.label = ['CATEGORY'],
                r.Name = [c.CMName]
            return elementId(r) as relID
        '''

        result = getQuery(query_2, driver, params={"cmids": [CMID]}, type = "list")

        if len(result) > 0:
            createLog(
            id=result,
            type="relation",
            log=[
                "Added CMName to relationship"
            ],
            user="0",
            driver=driver,
        )
        
        updateAltNames(database, CMID=CMID)

    except Exception as e:
        try:
            return str(e), 500
        except:
            return "Error", 500

#when dataset properties, parent or district are changed, this function creates or fixes contextual ties.
def processDATASETs(database, CMID=None, user="0"):
    try:

        driver = getDriver(database)

        # update District
        print("updating District")
        if CMID:
            q1 = 'unwind $CMID as cmid'
            q2 = '{CMID: cmid}'
        else:
            q1 = ""
            q2 = ""
        query = f"""
        {q1}
        match (d:DATASET {q2}) 
optional match (d)<-[:DISTRICT_OF]-(c:DISTRICT) 
unwind d.District as dist
with d, dist, collect(c.CMID) as districts 
with d, dist, districts, [i in collect(dist) where not i in districts] as missing,
[i in districts where not i = dist] as extra
with d, missing, extra
call apoc.do.when(size(missing) > 0,'with missing, d match (c:DISTRICT) where c.CMID in missing CREATE (d)<-[:DISTRICT_OF]-(c)','return NULL', {{d:d, missing:missing}}) yield value as v1
with d, missing, extra, v1
call apoc.do.when(size(extra) > 0,'match (d)<-[r:DISTRICT_OF]-(c) where c.CMID in extra delete r','RETURN NULL',{{d:d,extra:extra}}) yield value as v2
return count(*)
"""
        getQuery(query, driver=driver, params={"CMID": CMID})

        # update parent
        print("updating parent")
        query = f"""
        {q1}
        match (d:DATASET {q2}) 
optional match (d)<-[:CONTAINS]-(p:DATASET) 
unwind d.parent as par
with d, par, collect(p.CMID) as parents 
with d, par, parents, [i in collect(par) where not i in parents] as missing,
[i in parents where not i = par] as extra
with d, missing, extra
call apoc.do.when(size(missing) > 0,'with missing, d match (p:DATASET) where p.CMID in missing CREATE (d)<-[:CONTAINS]-(p)','return NULL', {{d:d, missing:missing}}) yield value as v1
with d, missing, extra, v1
call apoc.do.when(size(extra) > 0,'match (d)<-[r:CONTAINS]-(p) where p.CMID in extra delete r','RETURN NULL',{{d:d,extra:extra}}) yield value as v2
return count(*)
"""
        getQuery(query, driver=driver, params={"CMID": CMID})

        updateAltNames(database, CMID=CMID)
        
        return "Completed processing DATASET nodes"

    except Exception as e:
        try:
            return str(e), 500
        except:
            return "Error", 500
