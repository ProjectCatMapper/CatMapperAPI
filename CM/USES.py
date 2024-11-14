''' USES.py '''

from .utils import *
from datetime import datetime

def mergeDupRelations(driver, CMID = None):
    try:

        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "a.CMID = cmid and"
        else: 
            qFiltera = ""
            qFilterb = ""

        query = qFiltera + """
match (a)-[r]-(b) 
where 
""" + qFilterb + """
not type(r) = 'USES' 
with a, b, collect(r) as rels 
call apoc.refactor.mergeRelationships(rels,{properties: {`.*`: 'combine'}}) yield rel 
return count(*) as count
"""
        result = getQuery(query, driver = driver, params = {"cmid": CMID})

        return result
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

def fixUsesRels(driver, property, relationship, CMID = None):
    try:
        if property in ["country","district"]:
            qProp = "collect(distinct r.country) + collect(distinct r.district)"
        else:
            qProp = f"collect(distinct r.{property})"


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
with a, apoc.coll.toSet(apoc.coll.flatten({qProp},true)) as cmids 
match (n:CATEGORY) where n.CMID in cmids
merge (a)<-[:{relationship}]-(n)
return count(*) as count
"""

        query2 = f"""
        {qFiltera}
      match (n)-[rel:{relationship}]->(a:CATEGORY)<-[r:USES]-(:DATASET)
      {qFilterb}
      with a, apoc.coll.toSet(apoc.coll.flatten({qProp},true)) as current, 
      apoc.coll.toSet(apoc.coll.flatten(collect(distinct n.CMID),true)) as exists, collect(distinct rel) as rels, collect(n) as nodes
      with a, rels, nodes, [i in exists where not i in current] as extra
      unwind nodes as n unwind rels as rel with n,  rel, extra where n.CMID in extra and id(startNode(rel)) = id(n)
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
        data = getQuery(query = query1, driver = driver, params = {'cmid': CMID}, type = "list")
        getQuery(query = query2, driver = driver, params = {'cmid': CMID})
        getQuery(query = query3, driver = driver, params = {'cmid': CMID})

        return {str(property):data}
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

def updateLabels(driver, CMID = None):
    try:

        if CMID is not None:
            getQuery("unwind $CMID as cmid match (a {CMID: cmid})<-[:USES]-(:DATASET) set a:CATEGORY",driver, params = {"CMID":CMID})
        else:
            getQuery("match (a)<-[:USES]-(:DATASET) set a:CATEGORY",driver)

        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "a.CMID = cmid and"
            qFilterC = "with l, labelGroupMapping, cmid"
        else: 
            qFiltera = ""
            qFilterb = ""
            qFilterC = "with l, labelGroupMapping"

        query = f"""
MATCH (l:LABEL)
WITH l.label AS label, l.groupLabel AS groupLabel
WITH collect({{label: label, groupLabel: groupLabel}}) AS labelGroupMapping
match (l:METADATA:LABEL)
with apoc.coll.toSet(collect(distinct l.label) + 'CATEGORY') as l, labelGroupMapping
{qFiltera}
{qFilterC}
match (a:CATEGORY)<-[r:USES]-(:DATASET)
where 
{qFilterb}
r.label is not null
WITH a, r, l, labelGroupMapping
WITH a, l, apoc.coll.toSet(apoc.coll.flatten(collect(distinct r.label + "CATEGORY"), true)) AS labels, labelGroupMapping
WITH a, [i in labels WHERE i in l] + [d IN labelGroupMapping WHERE d.label IN labels | d.groupLabel] AS labels
CALL apoc.create.addLabels(a, labels) YIELD node
RETURN count(*)
"""

        result = getQuery(query = query, driver = driver, params = {"cmid":CMID})

        return result
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

def updateContains(driver, CMID = None):
    try:

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
        with a, rC, c, 
        apoc.coll.toSet(apoc.coll.flatten(collect(p.eventType))) as eventType, 
        apoc.coll.toSet(apoc.coll.flatten(collect(p.eventDate))) as eventDate

        // Update rC properties with events data using APOC
        call {
        with rC, eventDate
        call apoc.do.when(
            size(eventDate) > 0,
            "set rC.eventDate = $eventDate",
            "set rC.eventDate = NULL",
            {rC: rC, eventDate: eventDate}
        ) yield value
        }
        with rC, eventType
        call {
        with rC, eventType
        call apoc.do.when(
            size(eventType) > 0,
            "set rC.eventType = $eventType",
            "set rC.eventType = NULL",
            {rC: rC, eventType: eventType}
        ) yield value
        }
        return count(*)

        """
        
        query2 = qFiltera + """
        match (d:DATASET)-[rU:USES]->(a:CATEGORY)<-[rC:CONTAINS]-(p:CATEGORY) 
        """ + qFilterb + """
        with a,p, apoc.coll.flatten(collect(rU.parentContext),true) as pProps, apoc.coll.flatten(collect(rC.eventDate),true)  as eds, apoc.coll.flatten(collect(rC.eventType),true)  as ets
        with a,p, [i in pProps where not i = ""] as pProps, [i in eds where not i = ""] as eds, [i in ets where not i = ""] as ets
        where isEmpty(pProps) and (not isEmpty(eds) or not isEmpty(ets))
        match (a)<-[r:CONTAINS]-(p) set r.eventDate = NULL, r.eventType = NULL
        return count(*) as count
        """

        result1 = getQuery(query1,driver, params = {'cmid':CMID})
        result2 = getQuery(query2,driver, params = {'cmid':CMID})

        return {"query1":result1,"query2":result2}
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

def updateAltNames(driver, CMID = None):
    try:
        
        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "where a.CMID = cmid"
        else: 
            qFiltera = ""
            qFilterb = ""

        query = qFiltera + """
match (a:CATEGORY)<-[r:USES]-(:DATASET)
""" + qFilterb + """
with a, apoc.coll.toSet(apoc.coll.flatten(collect(distinct r.Name),true)) as names
set a.names = names
return count(a)
"""
        getQuery(query,driver, params = {'cmid':CMID})

        return "Completed"
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

def updateUses(driver, CMID=None, user="0"): # update name to processUSES
    try:

        # Update alternative names
        print("updating alternate names")
        updateAltNamesResults = updateAltNames(CMID=CMID, driver = driver)

        # Update labels
        print("updating labels")
        updateLabelsResults = "Not ran"
        updateLabelsResults = updateLabels(CMID=CMID, driver = driver)

        # Fix duplicate relationships
        mergeDupRelationsResults = "Not ran"
        # if CMID is not None:
        #     mergeDupRelationsResults = mergeDupRelations(CMID=CMID, driver = driver)

        # Update structural properties and referenceKeys
        properties = getPropertiesMetadata(driver = driver)
        properties = [item for item in properties if item.get('relationship') is not None] 
        propertiesResults = []
        for property, relationship in zip([item['property'] for item in properties if 'property' in item], [item['relationship'] for item in properties if 'relationship' in item]):
            print(f"{property} {relationship} {CMID}")
            
            r = fixUsesRels(CMID=CMID, property=property, relationship=relationship, driver = driver)
            propertiesResults.append(r)

        # Update contains relationships
        print("updating contains")
        updateContainsResults = updateContains(CMID=CMID, driver = driver)
        
        return {"CMID":CMID,"mergeDupRelations":mergeDupRelationsResults,"properties":propertiesResults,"updateLabels":updateLabelsResults,"updateContains":updateContainsResults,"updateAltNames":updateAltNamesResults}
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
    

def waitingUSES(database, BATCH_SIZE = 1000):
    try:
        driver = getDriver(database)
        CMID = getQuery("Match (c)<-[r:USES]-(d:DATASET) where r.status is not null and r.status = 'update' return c.CMID as CMID", driver, type = 'list')
        if CMID:
            for i in range(0, len(CMID), BATCH_SIZE):
                # Slice the CMID list to get the current batch
                batch = CMID[i:i + BATCH_SIZE]
                
                # Perform the update operation for the current batch
                updateUses(driver=driver, CMID=batch)
                
                # Optional: Print progress (useful for debugging or monitoring)
                print(f"Processed batch {i // BATCH_SIZE + 1} with {len(batch)} CMIDs.")
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
            raise ValueError(f"database must be either 'SocioMap' or 'ArchaMap', but value was '{database}'")

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
            SET r.Name = r.Name + [c.CMName],
                r.log = r.log + ["{datetime.now()} auto: added CMName to Names as CMName not in USES ties"]
        '''

        getQuery(query_1, driver, params={"cmids": [CMID]})

        query_2 = f'''
            MATCH (c:CATEGORY)
            WHERE {q} NOT c.CMName IN c.names AND NOT (c)<-[:USES]-(:DATASET {{CMID: "{datasetID}"}})
            WITH DISTINCT c
            MATCH (d:DATASET) WHERE d.CMID = "{datasetID}"
            CREATE (c)<-[r:USES]-(d)
            SET r.Key = "Key: " + c.CMID,
                r.log = ["{datetime.now()} auto: added USES tie as CMName not in USES ties"],
                r.Name = [c.CMName]
        '''

        getQuery(query=query_2, driver = driver, params={"cmids": [CMID]})

    except Exception as e:
        try:
            return str(e), 500
        except:
            return "Error", 500

def processDATASETs(database, CMID = None, user = "0"):
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
        getQuery(query, driver = driver, params = {"CMID":CMID})

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
        getQuery(query, driver = driver, params = {"CMID":CMID})


    except Exception as e:
        try:
            return str(e), 500
        except:
            return "Error", 500