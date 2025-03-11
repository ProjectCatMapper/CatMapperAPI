from CM.utils import *
import pandas as pd

def generate_cypher_query(nContains):
    if nContains < 1:
        raise ValueError("nContains must be at least 1")
    elif nContains  > 4:
        raise ValueError("nContains must be at most 4")
    base_query = """
    UNWIND $datasets AS dataset
    MATCH (d:DATASET {CMID: dataset})-[r:USES]->(c:ETHNICITY)
    RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
    c.CMID as LCA_CMID, c.CMName as LCA_CMName,
    apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, 0 as tie
    UNION ALL
    UNWIND $datasets AS dataset
    MATCH (d:DATASET {CMID: dataset})-[r:USES]->(c:ETHNICITY)
    MATCH (c)<-[rc:CONTAINS]-(p:CATEGORY)
    WHERE not rc.generic = true
    RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
    p.CMID as LCA_CMID, p.CMName as LCA_CMName,
    apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, 1 as tie
    """
    
    union_queries = []
    for i in range(2, nContains + 1):
        union_query = f"""
        UNION ALL
        UNWIND $datasets AS dataset
        MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:ETHNICITY)
        MATCH (c)<-[rc:CONTAINS*{i - 1}]-(p:CATEGORY)
        WHERE isEmpty([i in rc WHERE i.generic = true])
        RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
        p.CMID as LCA_CMID, p.CMName as LCA_CMName,
        apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, {i} as tie
        """
        union_queries.append(union_query)
    
    full_query = base_query + "\nUNION ALL".join(union_queries)
    return full_query

# Example usage
nContains = 2
query = generate_cypher_query(nContains)
print(query)
