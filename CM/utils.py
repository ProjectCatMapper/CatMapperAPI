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
import json


def getQuery(query, driver, params=None, type="dict", **kwargs):
def getQuery(query, driver, params=None, type="dict", **kwargs):
    try:
        params = params or {}
        params.update(kwargs)
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

        raise RuntimeError(f"An error occurred: {e}")


def unlist(l):
    if isinstance(l, list):
        l = l[0]
    return l



def isValidCMID(cmid, driver):


    query = "unwind $cmid as cmid match (c) where c.CMID = cmid return c.CMID as cmid, true as exists"

    with driver.session() as session:
        result = session.run(query, cmid=cmid)
        result = session.run(query, cmid=cmid)
        result = [dict(record) for record in result]
        driver.close()

    return result


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
        data = getQuery(query=query, driver=driver)
        data = getQuery(query=query, driver=driver)
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
n.displayName as displayName, n.remove as remove, n.color as color  
"""
        data = getQuery(query=query, driver=driver)
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

        from configparser import ConfigParser
        config = ConfigParser()
        config.read('config.ini')
        user = config['DB']['user']
        pwd = config['DB']['pwd']

        if str.lower(database) == "sociomap":
            driver = GraphDatabase.driver(config['DB']['uriSM'], auth=(
                user, pwd))
            driver = GraphDatabase.driver(config['DB']['uriSM'], auth=(
                user, pwd))
        elif str.lower(database) == "archamap":
            driver = GraphDatabase.driver(config['DB']['uriAM'], auth=(
                user, pwd))
            driver = GraphDatabase.driver(config['DB']['uriAM'], auth=(
                user, pwd))
        elif str.lower(database) == "gisdb":
            driver = GraphDatabase.driver(config['DB']['uriG'], auth=(
                user, pwd))
            driver = GraphDatabase.driver(config['DB']['uriG'], auth=(
                user, pwd))
        elif str.lower(database) == "userdb":
            driver = GraphDatabase.driver(config['DB']['uriU'], auth=(
                user, pwd))
            driver = GraphDatabase.driver(config['DB']['uriU'], auth=(
                user, pwd))
        else:
            raise Exception(
                f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")

            raise Exception(
                f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")

        return driver

    except Exception as e:
        print(f"Error validating database: {e}")
        abort(500, description=f"An unexpected error occurred: {str(e)}")


def validateCols(df, required):
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

def getAvailableID(new_id="CMID", label="CATEGORY", n=1, database="SocioMap"):
    print(database)

    if database.lower() == 'sociomap':
        database = 'SocioMap'
    elif database.lower() == 'archamap':
    elif database.lower() == 'archamap':
        database = 'ArchaMap'
    elif database.lower() == 'gisdb':
        database = 'gisdb'
    elif database.lower() == 'userdb':
        database = 'userdb'
    else:
        raise ValueError(
            f"Database must be 'SocioMap', 'ArchaMap', 'gisdb', or 'userdb', but database is {database}")
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

import ast

def pivot_property_value_columns(filepath):
    """
    Loads a CSV, expands list-like strings in the 'value' column,
    filters out unwanted 'property' values, pivots the table wider,
    and overwrites the original file.

    Parameters:
        filepath (str): Path to the input CSV file.
    """

    # Check if file exists
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # Load CSV
    df = pd.read_csv(filepath)

    # Check required columns
    required_cols = {'property', 'value'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Function to safely parse list-like strings
    def parse_list(val):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except:
            return [val]

    # Filter out unwanted properties
    ignore_props = {'log', 'logID', 'geoPolygon', 'names'}
    df_filtered = df[~df['property'].isin(ignore_props)].copy()

    # Convert stringified lists to actual lists
    df_filtered['value'] = df_filtered['value'].apply(parse_list)

    # Fill NaNs to prevent explode/join issues
    df_filtered.fillna('', inplace=True)

    # Convert list columns to semicolon-separated strings
    for col in df_filtered.columns:
        if df_filtered[col].apply(lambda x: isinstance(x, list)).any():
            df_filtered[col] = df_filtered[col].apply(
                lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x
            )

    # Identify index columns (all except 'property' and 'value')
    index_cols = [col for col in df_filtered.columns if col not in ['property', 'value']]

    # Pivot to wide format
    df_wide = df_filtered.pivot_table(
        index=index_cols,
        columns='property',
        values='value',
        aggfunc=lambda x: ', '.join(x.astype(str))  # join duplicates
    ).reset_index()

    df_wide.columns.name = None  # Remove column name from pivot
    df_wide.fillna('', inplace=True)  # Replace NaNs with empty strings

    # Save the resulting DataFrame
    df_wide.to_csv(filepath, index=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pivot CSV based on 'property' and 'value' columns.")
    parser.add_argument("filepath", help="Path to the CSV file to process.")
    args = parser.parse_args()

    pivot_property_value_columns(args.filepath)
    print(f"Processed and saved: {args.filepath}")


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

import ast

def pivot_property_value_columns(filepath):
    """
    Loads a CSV, expands list-like strings in the 'value' column,
    filters out unwanted 'property' values, pivots the table wider,
    and overwrites the original file.

    Parameters:
        filepath (str): Path to the input CSV file.
    """

    # Check if file exists
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # Load CSV
    df = pd.read_csv(filepath)

    # Check required columns
    required_cols = {'property', 'value'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Function to safely parse list-like strings
    def parse_list(val):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except:
            return [val]

    # Filter out unwanted properties
    ignore_props = {'log', 'logID', 'geoPolygon', 'names'}
    df_filtered = df[~df['property'].isin(ignore_props)].copy()

    # Convert stringified lists to actual lists
    df_filtered['value'] = df_filtered['value'].apply(parse_list)

    # Fill NaNs to prevent explode/join issues
    df_filtered.fillna('', inplace=True)

    # Convert list columns to semicolon-separated strings
    for col in df_filtered.columns:
        if df_filtered[col].apply(lambda x: isinstance(x, list)).any():
            df_filtered[col] = df_filtered[col].apply(
                lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x
            )

    # Identify index columns (all except 'property' and 'value')
    index_cols = [col for col in df_filtered.columns if col not in ['property', 'value']]

    # Pivot to wide format
    df_wide = df_filtered.pivot_table(
        index=index_cols,
        columns='property',
        values='value',
        aggfunc=lambda x: ', '.join(x.astype(str))  # join duplicates
    ).reset_index()

    df_wide.columns.name = None  # Remove column name from pivot
    df_wide.fillna('', inplace=True)  # Replace NaNs with empty strings

    # Save the resulting DataFrame
    df_wide.to_csv(filepath, index=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pivot CSV based on 'property' and 'value' columns.")
    parser.add_argument("filepath", help="Path to the CSV file to process.")
    args = parser.parse_args()

    pivot_property_value_columns(args.filepath)
    print(f"Processed and saved: {args.filepath}")