''' routines.py '''

# This is a module for automatic routines in CatMapper

from neo4j import GraphDatabase
from CM.utils import *
from CM.email import *
import pandas as pd
import json

def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False

def validateJSON(database, property = 'parentContext', path ="/mnt/storage/app/tmp/invalid_json.xlsx" ):
    try:

        driver = getDriver(database)

        query = f"match (c:CATEGORY)<-[r:USES]-(d:DATASET) where not r.{property} is null return d.CMID as datasetID, c.CMID as CMID, r.Key as Key, r.{property} as prop"

        results = getQuery(query, driver)

        results = pd.DataFrame(results)

        results = pd.DataFrame.explode(results, "prop")

        results["is_valid_json"] = results["prop"].apply(is_valid_json)

        invalid = results[results["is_valid_json"] == False]

        invalid.to_excel(path)

        invalid = invalid.to_dict(orient="records")

        return(invalid)

    except Exception as e:
        return "Unable to validate JSON properties: " + str(e)
    

def addLog(database):
    try:
        driver = getDriver(database)
        query = """
        match ()-[r:USES]-() where r.log is null set r.log = custom.makeLog([],"0","added log property")
        """
        getQuery(query, driver)

        query = """
        match (c) where c.log is null set c.log = [custom.makeLog([],"0","added log property")]
        """
        getQuery(query, driver)

        return "Completed"
    except Exception as e:
        return str(e)
    
def checkDomains(data,database):
    try:
        if data is None:
            data = False
        elif data.lower() == "true":
            data = True
        else:
            data = False
        driver = getDriver(database)
        query = """
        match (n:CATEGORY)
        where size(labels(n)) = 1
        optional match (n)<-[r:USES]-(d:DATASET) 
        return "CATEGORY" as query, n.CMID as CMID, n.CMName as CMName, r.label as label, d.CMID as datasetID
        UNION ALL
        match (n)
        where isEmpty([i in labels(n) where i in ["DATASET","METADATA","USER","CATEGORY","DELETED"]]) 
        optional match (n)<-[r:USES]-(d:DATASET) 
        return "MissingCATEGORY" as query,  n.CMID as CMID, n.CMName as CMName, r.label as label, d.CMID as datasetID
        """
        result = getQuery(query, driver)
        
        if data:
            return result
        else:
            return "Completed"
         
    except Exception as e:
        return str(e)
    

# need to create function based off this
# with "eventType" as prop match (a)-[r]->(b) where not r[prop] is null and r[prop] = [] call apoc.cypher.doIt("with r set r." + prop + " = NULL",{r:r}) yield value return count(*)

def backup2CSV(database, mail = None):
    try:
        driver = getDriver(database)
        query = """
            with 'match (d:DATASET) unwind keys(d) as property return distinct id(d) as nodeID, property, d[property] as value' as query CALL apoc.export.csv.query(query, '/backups/datasetNodes.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
        """

        result = getQuery(query, driver)
        print(result)
         
        query2 = """
            with 'match (n:CATEGORY)<-[r:USES]-(d:DATASET) unwind keys(r) as property return distinct id(r) as relID, n.CMID as CMID, n.CMName as CMName, d.CMName as dataset, d.CMID as datasetID, property, r[property] as value order by CMName' as query CALL apoc.export.csv.query(query, '/backups/USESties.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
            """
        
        result2 = getQuery(query2, driver)
        print(result2)

        if mail:
            sendEmail(mail, subject = "Weekly CSV Backup", recipients = ["rjbischo@catmapper.org"], body = "Weekly CSV backup completed.", sender = os.getenv("mail_default"))

        return "backup2CSV completed"
        
    except Exception as e:
        return str(e)
    


def getBadCMID(database,mail = None):
    try:
         
        driver = getDriver(database)
        properties = getPropertiesMetadata(driver)
        properties = pd.DataFrame(properties)
        properties = properties[properties["relationship"].notna()]

        results = []

        for property in properties["property"]:
            print("Checking CMIDs for property: ", property)

            queryFix = """
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        WHERE not r.{property} is null and not apoc.meta.cypher.type(r.{property}) = "LIST OF STRING" set r.{property} = [r.{property}]
        """

            query = f"""
            MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET) 
            with apoc.coll.toSet(apoc.coll.flatten(collect(distinct r.{property}),true)) as val
            call {{with val match (c:CATEGORY) where c.CMID in val return collect(c.CMID) as val2}} 
            with [i in val where not i in val2] as badlist
            unwind badlist as bad
            with bad
            match (c:CATEGORY)<-[r:USES]-(d:DATASET) where bad in r.{property} 
            return c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset, '{property}' as propertyType, apoc.text.join(r.{property},"; ") as property, bad as badCMID
            """
            result = getQuery(query, driver)
            if len(result) > 0:
                results.append(result)

        if len(results) > 0:
            results = [pd.DataFrame(item) for item in results]
            results = pd.concat(results)

            query2 = """
            MATCH (old:DELETED)-[:IS]->(c) 
            where old.CMID in $cmids 
            return old.CMID as badCMID, c.CMID as newCMID
            """

            deleted = getQuery(query2, driver, {"cmids": list(results["badCMID"].unique())})

            if len(deleted) > 0:

                deleted = pd.DataFrame(deleted)

                results = results.merge(deleted, on = "badCMID", how = "left")

            if mail is not None:
                fp1 = "tmp/badCMIDs.xlsx"
                results.to_excel(fp1, index = False)
                sendEmail(mail, subject = "Bad CMIDs", recipients = ["rjbischo@catmapper.org"], body = "See attached", sender = os.getenv("mail_default"), attachments = [fp1])

            return results.to_dict(orient="records")
        else: 
            return "No bad CMIDs found"

    except Exception as e:
        return "Error: " + str(e)
        

def getMultipleLabels(database):
    try:
        driver = getDriver(database)
        query = """
        match (d:DATASET)-[r:USES]->(c:CATEGORY) where apoc.meta.cypher.type(r.label) = "LIST OF STRING" and size(r.label) > 1 return c.CMID as CMID, c.CMName as CMName, d.CMID as datasetID, r.Key as Key, apoc.text.join(r.label, "; ") as label
        """
        results = getQuery(query, driver)
        return results
    except Exception as e:
        return str(e)
    
def getBadJSON(database,mail = None):
    try:        
        fp1 = "tmp/invalid_json_geoCoords.xlsx"
        results1 = validateJSON(database = database, property = 'geoCoords', path = fp1)
        fp2 = "tmp/invalid_json_parentContext.xlsx"
        results2 = validateJSON(database = database, property = 'parentContext', path = fp2)

        if mail is not None:
            if results1:
                sendEmail(mail, subject = "Invalid geoCoords properties", recipients = ["rjbischo@catmapper.org"], body = "See attached", sender = os.getenv("mail_default"), attachments = [fp1])
                mailSent = "True"

            if results2:
                sendEmail(mail, subject = "Invalid parentContext properties", recipients = ["rjbischo@catmapper.org"], body = "See attached", sender = os.getenv("mail_default"), attachments = [fp2])
                mailSent = "True"

        return {"geoCoords": len(results1), "parentContext": len(results2), "geoCoords": results1, "parentContext": results2, "emailSent": mailSent}    
    except Exception as e:
        result = str(e)
        return result, 500  