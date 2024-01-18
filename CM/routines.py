''' routines.py '''

# This is a module for automatic routines in CatMapper

from neo4j import GraphDatabase

def addLog(driver):
    try:
        query = """
        match ()-[r:USES]-() where r.log is null set r.log = []
        """
        with driver.session() as session:
                results = session.run(query)
                driver.close()
        query = """
        match (c) where c.log is null set c.log = []
        """
        with driver.session() as session:
                results = session.run(query)
                driver.close()
        return "Completed"
    except Exception as e:
        return str(e)
    
def checkDomains(data,driver):
    try:
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
        with driver.session() as session:
                results = session.run(query)           
                result = [dict(record) for record in results]
                if data is False:
                     result = str(len(result)) + " invalid domains"
                driver.close()
        
        return result
         
    except Exception as e:
        return str(e)