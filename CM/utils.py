''' utils.py '''

# general utility functions

import re
from datetime import datetime
from neo4j import GraphDatabase
import pandas as pd
import numpy as np

def getQuery(query,driver, params = None):
    try:
        with driver.session() as session:
            result = session.run(query,params)
            result = [dict(record) for record in result]
            driver.close()
        return result
    except Exception as e:
        return str(e), 500

def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l

def isValidCMID(cmid, driver):
    
    query = "unwind $cmid as cmid match (c) where c.CMID = cmid return c.CMID as cmid, true as exists"

    with driver.session() as session:
        result = session.run(query,cmid = cmid)
        result = [dict(record) for record in result]
        driver.close()

    return result

def createLog(id, type, log, user, driver):
    # Remove single and double quotes from the log message
    logQ = re.sub(r"[\'\"]", "", log)

    # Format the log message with current UTC time, user, and the cleaned log message
    logQ = f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: {logQ}"

    # Constructing the query string based on the type
    if type == "node":
        qs = "(l) where id(l) = toInteger(id)"
    elif type == "relation":
        qs = "()-[l]->() where id(l) = toInteger(id)"
    else:
        raise ValueError("error: type must be node or relation")

    # Final query construction with string interpolation
    q = f"unwind $ids as id match {qs} with l, apoc.coll.flatten(['{logQ}',coalesce(l.log,[])],true) as log set l.log = log"

    with driver.session() as session:
        session.run(q, user = user, ids = id)
        driver.close()

    return "Completed"

def getPropertiesMetadata(driver):
    try:

        query = """
match (n:METADATA:PROPERTY) 
return n.property as property, n.type as type, 
n.relationship as relationship, n.description as description, 
n.display as display, n.group as group, 
n.metaType as metaType, n.search as search, 
n.translation as translation
"""
        data = getQuery(query = query, driver = driver)
        return data
    
    except Exception as e:
        return str(e), 500
    
def getLabelsMetadata(driver):
    try:

        query = """
match (n:METADATA:LABEL) 
return n.label as label, n.groupLabel as groupLabel, 
n.relationship as relationship, n.public as public, 
n.default as default, n.description as description, 
n.displayName as displayName, n.remove as remove, n.color as color
"""
        data = getQuery(query = query, driver = driver)
        return data
    
    except Exception as e:
        return str(e), 500
    
def addMatchResults(results):
    try:
        # Select and distinct the necessary columns
        df = results[['term', 'CMID', 'matchingDistance']].drop_duplicates(['term', 'CMID'])

        # Group by 'term' and count occurrences
        df['n'] = df.groupby('term')['term'].transform('count')

        # Determine the match type
        conditions = [
            df['CMID'].isna(),
            (df['n'] > 1) & df['CMID'].notna(),
            df['matchingDistance'] > 0,
            True
        ]
        choices = [
            'none',
            'one-to-many',
            'fuzzy match',
            'exact match'
        ]
        df['matchType'] = np.select(conditions, choices, default=np.nan)

        # Group by 'CMID' and count occurrences
        df['n'] = df.groupby('CMID')['CMID'].transform('count')

        # Adjust match type for many-to-one scenarios
        df.loc[(df['matchType'] == 'one-to-many') & (df['matchType'] != 'none') & (df['n'] > 1), 'matchType'] = 'many-to-one'

        # Drop the 'n' and 'matchingDistance' columns
        df = df.drop(columns=['n', 'matchingDistance'])

        # Join the original results with the new matchType information
        results = pd.merge(results, df, on=['CMID', 'term'], how='left')

    except Exception as e:
        print(f"Error returning match statistics: {e}")
        return e

    return results

def validateDatabase(database):
    try:
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        return driver
    except Exception as e:
        print(f"Error validating database: {e}")
        return e