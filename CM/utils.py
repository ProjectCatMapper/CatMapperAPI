''' utils.py '''

# general utility functions

import re
from datetime import datetime
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import os
from flask import abort
import itertools
from collections.abc import Iterable
import json


def getQuery(query, driver, params=None, type="dict", **kwargs):
    try:
        params = params or {}
        params.update(kwargs)
        with driver.session() as session:
            result = session.run(query, params)
            if type == "dict":
                result = [dict(record) for record in result]
            elif type == "list":
                result = list(itertools.chain.from_iterable(
                    record.values() for record in result))
            elif type == "df":
                result = pd.DataFrame([dict(record) for record in result])
            else:
                raise Exception("invalid type")
            driver.close()
        return result
    except Exception as e:
        raise RuntimeError(f"An error occurred: {e}")


def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l

# it returns true if CMID exists else returns empty list
def isValidCMID(cmid, driver):

    query = "unwind $cmid as cmid match (c:CATEGORY:DATASET) where c.CMID = cmid return c.CMID as cmid, true as exists"

    with driver.session() as session:
        result = session.run(query, cmid=cmid)
        result = [dict(record) for record in result]
        driver.close()
    
    return result


def getPropertiesMetadata(driver):
    try:

        query = """
match (n:PROPERTY) 
return n.CMName as property, n.type as type, 
n.relationship as relationship, n.description as description, 
n.display as display, n.group as group,
n.metaType as metaType, n.search as search,
n.translation as translation
"""
        data = getQuery(query=query, driver=driver)
        return data

    except Exception as e:
        return str(e), 500


def getLabelsMetadata(driver):
    try:

        query = """
match (n:LABEL)
return n.CMName as label, n.groupLabel as groupLabel, 
n.relationship as relationship, n.public as public, 
n.default as default, n.description as description, 
n.displayName as displayName, n.remove as remove, n.color as color  
"""
        data = getQuery(query=query, driver=driver)
        return data

    except Exception as e:
        return str(e), 500


def getDriver(database):
    try:
        from configparser import ConfigParser
        config = ConfigParser()
        config.read('config.ini')
        user = config['DB']['user']
        pwd = config['DB']['pwd']
        database = str.lower(database)
        if not database in config['DB']:
            raise ValueError(
                f"Database must be 'SocioMap', 'ArchaMap', 'gisdb', or 'userdb', but database is {database}")
        uri = config['DB'][database]
        configOpt='DB'
        if not testConnection():
            configOpt = 'OFFLINE'
        if not testConnection(configOpt=configOpt, database=database):
            raise Exception(
                f"Database connection failed for {database}. Please check your configuration.")
        driver = GraphDatabase.driver(config[configOpt][uri], auth=(
                user, pwd))

        return driver

    except Exception as e:
        print(f"Error validating database: {e}")
        abort(500, description=f"An unexpected error occurred: {str(e)}")


def validateCols(df, required):
    missing = [col for col in df.columns if col not in required]

    if len(missing) > 0:
        return f"Missing the following required column(s): {missing}\n"
    else:
        return True


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


def getAvailableID(new_id="CMID", label="CATEGORY", n=1, database="SocioMap"):
    print(database)

    if database.lower() == 'sociomap':
        database = 'SocioMap'
    elif database.lower() == 'archamap':
        database = 'ArchaMap'
    elif database.lower() == 'gisdb':
        database = 'gisdb'
    elif database.lower() == 'userdb':
        database = 'userdb'
    else:
        raise ValueError(
            f"Database must be 'SocioMap', 'ArchaMap', 'gisdb', or 'userdb', but database is {database}")

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

    newID = getQuery(query, driver, type="list")
    newID = newID[0]

    # If no ID is found, start from 1
    if newID is None:
        newID = 1

    # Generate the range of new IDs
    newID = list(range(newID, newID + n))

    # Add prefixes based on the database and label
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


def list2character(col):
    # If col is a list, join the items into a single string
    if isinstance(col, list):
        return ','.join(map(str, col))
    # If col is a string, return it as is
    elif isinstance(col, str):
        return col
    # Otherwise, convert it to a string
    else:
        return str(col)


def flattenList(input_data):
    if isinstance(input_data, str):
        return [input_data]
    elif isinstance(input_data, dict):
        input_data = input_data.keys()

    if isinstance(input_data, Iterable):
        flat_list = []
        for item in input_data:
            flat_list.extend(flattenList(item))
        return flat_list
    else:
        return [input_data]


def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False


def flatten_json(json_obj, parent_key='', sep='_'):
    flat_dict = {}
    for key, value in json_obj.items():
        new_key = key if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        else:
            flat_dict[new_key] = value
    return flat_dict


def custom_sort(elem):
    if elem == 'CONTAINS':
        return 0
    elif elem == 'DISTRICT_OF':
        return 1
    elif elem == 'USES':
        return 2
    else:
        return 3

def testConnection(configOpt="DB",database="SocioMap"):
    """
    Test the connection to the specified database.
    """
    try:
        from configparser import ConfigParser
        config = ConfigParser()
        config.read('config.ini')
        user = config['DB']['user']
        pwd = config['DB']['pwd']
        database = str.lower(database)
        uri = config['DB'][database]  # Default to SocioMap URI
        driver = GraphDatabase.driver(config[configOpt][uri], auth=(
                user, pwd))
        with driver.session() as session:
            result = session.run("RETURN 1")
            success = result.single()[0] == 1
        driver.close()
        return success
    except Exception as e:
        return False
    
def getNodeProperties(database, domain, CMID):
    """
    Get the properties of a node in the specified database.
    Args:
        database (str): The name of the database.
        domain (str): The domain of the node (e.g., "CATEGORY", "DATASET").
        CMID (list): A list of CMIDs to query.
    """
    driver = getDriver(database)
    if domain == "CATEGORY":
        query = """
        unwind $CMID as cmid
        match (c:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET)
        with distinct apoc.coll.toSet(apoc.coll.flatten(collect(keys(c) + keys(r)))) as properties
        with [i in properties where not i in ["log","logID","CMName","CMID","names","label"]] as properties
        unwind properties as property
        return property
        """
    elif domain == "DATASET":
        query = """
        unwind $CMID as cmid
        match (d:DATASET {CMID: cmid})
        with distinct apoc.coll.toSet(collect(keys(d))) as properties
        with [i in properties where not i in ["log","logID","CMName","CMID","names"]] as properties
        unwind properties as property
        return property
        """
    else: 
        raise ValueError("Invalid domain specified")

    properties = getQuery(query, driver, CMID = CMID, type = "list")
    
    return properties


# Function to serialize a Neo4j Node object into a serializable dictionary
def serialize_node(node):
    return {
        "id": node.element_id,
        "labels": list(node.labels),
        "properties": dict(node)
    }

# Function to serialize Neo4j Relationship object into a serializable dictionary


def serialize_relationship(relationship):
    return {
        "type": relationship.type,
        "start_node_id": relationship.start_node.element_id,
        "end_node_id": relationship.end_node.element_id,
        "properties": dict(relationship.items())
    }
