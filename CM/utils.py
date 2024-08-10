''' utils.py '''

# general utility functions

import re
from datetime import datetime
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from dotenv import load_dotenv, find_dotenv
import os
from flask import abort
import itertools

load_dotenv(find_dotenv())

def getQuery(query,driver, params = None, type = "dict"):
    try:
        with driver.session() as session:
            result = session.run(query,params)
            if type == "dict":
                result = [dict(record) for record in result]
            elif type == "list":
                result = list(itertools.chain.from_iterable(record.values() for record in result))
            else: 
                raise Exception("invalid type")
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

def getDriver(database):
    try:
        if str.lower(database) == "sociomap":
            driver = GraphDatabase.driver(os.getenv("uriSM"), auth=(os.getenv("user"), os.getenv("pwdSM")))
        elif str.lower(database) == "archamap":
            driver = GraphDatabase.driver(os.getenv("uriAM"), auth=(os.getenv("user"), os.getenv("pwdAM")))
        elif str.lower(database) == "gisdb":
            driver = GraphDatabase.driver(os.getenv("uriG"), auth=(os.getenv("user"), os.getenv("pwdG")))
        elif str.lower(database) == "userdb":
            driver = GraphDatabase.driver(os.getenv("uriU"), auth=(os.getenv("user"), os.getenv("pwdU")))
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        
        return driver

    except Exception as e:
        print(f"Error validating database: {e}")
        abort(500, description = f"An unexpected error occurred: {str(e)}")
    
def validateCols(df,required):
    missing = [col for col in df.columns if col not in required]

    if len(missing) > 0:
        return f"Missing the following required column(s): {missing}\n"
    else:
        return True
    
def verifyUser(user,pwd):
    try:
        driver = getDriver("userdb")
        with driver.session() as session:
            query = "match (u:USER {userid: toString($user),password: $pwd}) return 'verified' as verified"
            result = session.run(query, user = user, pwd = pwd)
            result = [dict(record) for record in result]
            driver.close()
        return result[0].get("verified")
    except Exception as e:
        return f"Error verifying user: {e}", 500
    
def cleanCMID(cmid):
    # Define the regex pattern for valid prefixes
    valid_prefix_pattern = re.compile(r'^(AD|SD|AM|SM)')

    # Function to check if a string has a valid prefix
    def has_valid_prefix(s):
        return bool(valid_prefix_pattern.match(s))

    if isinstance(cmid, list):
        # If cmid is a list, filter out items that don't have a valid prefix
        cleaned_cmid = [item for item in cmid if has_valid_prefix(item)]
        # If the cleaned list is empty, return None
        return cleaned_cmid if cleaned_cmid else None
    elif isinstance(cmid, str):
        # If cmid is a string, check if it has a valid prefix
        return cmid if has_valid_prefix(cmid) else None
    else:
        # If cmid is neither a list nor a string, return None
        return None

def getAvailableID(new_id="CMID", label="CATEGORY", n=1, database = "SocioMap"):

    driver = getDriver(database)

    # Ensure label is either "DATASET", "USER", or "CATEGORY"
    if label not in ["DATASET", "USER"]:
        label = "CATEGORY"

    # Define the Cypher query to find the next available ID
    query = f'''
    MATCH (a) 
    WHERE a.{new_id} IS NOT NULL 
    WITH toInteger(apoc.text.replace(toString(a.{new_id}), "[^0-9]", "")) AS new_id 
    WHERE NOT apoc.meta.cypher.isType(new_id, "NULL") 
    WITH new_id 
    ORDER BY new_id DESC 
    LIMIT 1 
    RETURN new_id + 1 as new_id
    '''

    newID = getQuery(query, driver, type = "list")
    newID = newID[0]

    # If no ID is found, start from 1
    if newID is None:
        newID = 1

    # Generate the range of new IDs
    newID = list(range(newID, newID + n))

    # Add prefixes based on the database and label
    if new_id == "CMID":
        prefix = ''
        if database == "SocioMap":
            prefix = "S"
        elif database == "ArchaMap":
            prefix = "A"
        elif database == "gisdb":
            prefix = "gis"
        
        if database == "gisdb":
            newID = [f"{prefix}{x}" for x in newID]
        else:
            if label == "DATASET":
                newID = [f"{prefix}D{x}" for x in newID]
            else:
                newID = [f"{prefix}M{x}" for x in newID]

    return newID
