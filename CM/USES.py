''' USES.py '''

from .utils import *

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
            qProp2 = ""
        else:
            qProp = f"collect(distinct r.{property})"
            if CMID is None:
                qProp2 = f"not r.{property} is null"  
            else:
                qProp2 = f"and not r.{property} is null"

        if CMID is not None:
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "a.CMID = cmid "
        else: 
            qFiltera = ""
            qFilterb = ""

        if qProp2 == "" and qFiltera == "":
            qWhere = ""
        else:
            qWhere = "where"

        query1 = f"""
        {qFiltera}
match (a:CATEGORY)<-[r:USES]-(:DATASET)
{qWhere}
{qFilterb}
{qProp2}
with a, apoc.coll.toSet(apoc.coll.flatten({qProp},true)) as cmids 
match (n:CATEGORY) where n.CMID in cmids
merge (a)<-[:{relationship}]-(n)
return count(*) as count
"""

        if qFiltera == "":
            qWhere = ""
        else:
            qWhere = "where"

        query2 = f"""
        {qFiltera}
      match (n)-[rel:{relationship}]->(a:CATEGORY)<-[r:USES]-(:DATASET)
      {qWhere} 
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
    {qWhere}
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
            qFiltera = "unwind $cmid as cmid"
            qFilterb = "a.CMID = cmid and"
        else: 
            qFiltera = ""
            qFilterb = ""

        query = qFiltera + """
    match (a)<-[r:USES]-(:DATASET)
where 
""" + qFilterb + """
r.label is not null
with a,r
match (l:METADATA:LABEL)
with a, collect(distinct l.label) as l, apoc.coll.flatten(collect(distinct r.label),true) as labels
with a, [i in labels where i in l] as labels
with a, labels + ["CATEGORY"] as labels
call apoc.create.setLabels(a,labels) yield node
return count(*)
"""

        result = getQuery(query = query, driver = driver, params = {"cmid":CMID})

        labels = getLabelsMetadata(driver = driver)

        for label,groupLabel in zip([item['label'] for item in labels if 'label' in item],[item['groupLabel'] for item in labels if 'groupLabel' in item]):
            query = f"match (a:{label}) set a:{groupLabel}"
            getQuery(driver = driver, query = query)

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

def updateUses(driver, CMID=None, user="0"):
    try:
        # Fix duplicate relationships
        mergeDupRelationsResults = "Not ran"
        if CMID is not None:
            mergeDupRelationsResults = mergeDupRelations(CMID=CMID, driver = driver)

        # Update structural properties and referenceKeys
        properties = getPropertiesMetadata(driver = driver)
        properties = [item for item in properties if item.get('relationship') is not None] 
        propertiesResults = []
        for property, relationship in zip([item['property'] for item in properties if 'property' in item], [item['relationship'] for item in properties if 'relationship' in item]):
            # print(f"{property} {relationship} {CMID}")
            
            r = fixUsesRels(CMID=CMID, property=property, relationship=relationship, driver = driver)
            propertiesResults.append(r)

        # Update labels
        updateLabelsResults = updateLabels(CMID=CMID, driver = driver)

        # Update contains relationships
        updateContainsResults = updateContains(CMID=CMID, driver = driver)

        # Update alternative names
        updateAltNamesResults = updateAltNames(CMID=CMID, driver = driver)
        
        return {"CMID":CMID,"mergeDupRelations":mergeDupRelationsResults,"properties":propertiesResults,"updateLabels":updateLabelsResults,"updateContains":updateContainsResults,"updateAltNames":updateAltNamesResults}
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500