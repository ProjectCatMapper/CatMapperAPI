""" download datasets updated weekly from AWS """

import boto3
import os
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/app/aws_credentials"
from datetime import datetime
import re
from CM import getDriver, getQuery
import pandas as pd

def get_backup_csv_urls(database, bucket="sociomap-backups", region="us-west-1", mostRecent=True):
    """
    Returns a list of full S3 URLs to backup CSV files for the given database,
    along with their sizes in megabytes.

    Parameters:
        database (str): "ArchaMap" or "SocioMap"
        bucket (str): S3 bucket name (default: "sociomap-backups")
        region (str): AWS region (default: "us-west-1")
        mostRecent (bool): If True, only return the most recent CSV files.

    Returns:
        list of tuples: (url, size_in_MB)
    """
    prefix_map = {
        "ArchaMap": "archamap-backups/download",
        "SocioMap": "sociomap1-backups/download"
    }

    if database not in prefix_map:
        raise ValueError(
            f"Unknown database: {database}. Must be 'ArchaMap' or 'SocioMap'.")

    prefix = prefix_map[database]

    s3 = boto3.client("s3")
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    file_info = []

    date_pattern = re.compile(r'_(\d{4}-\d{2}-\d{2})\.csv$')

    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size_bytes = obj["Size"]
            if key.endswith(".csv"):
                match = date_pattern.search(key)
                if match:
                    try:
                        file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                        size_mb = round(size_bytes / (1024 * 1024), 2)
                        file_info.append((file_date, key, size_mb))
                    except ValueError:
                        continue  # skip invalid dates

    if mostRecent and file_info:
        most_recent_date = max(date for date, _, _ in file_info)
        file_info = [(date, key, size) for date, key, size in file_info if date == most_recent_date]

    # Build list of (URL, size_MB) tuples
    results = [
        (f"https://{bucket}.s3.{region}.amazonaws.com/{key}", size)
        for _, key, size in file_info
    ]

    return results

def getAdvancedDownload(database, CMID, properties):
    """
    Returns a dataset with the given CMIDs and properties.

    Parameters:
        database (str): "ArchaMap" or "SocioMap"
        CMID (str): The CatMapperID of the content to download
        properties (dict): Additional properties to include with the download

    Returns:
        list of tuples: (url, size_in_MB)
    """
    driver = getDriver(database)
    
    if isinstance(CMID, str):
        CMID = [CMID]
    if isinstance(properties, str):
        properties = [properties]
        
    if not isinstance(CMID, list) or not isinstance(properties, list):
        raise ValueError("CMID and properties must be lists.")
    
    # determine if the second letter starts with a 'D' or 'M' to determine if it is a dataset or category
    if CMID[0][1] == 'D':
        domain = "DATASET"
    elif CMID[0][1] == 'M':
        domain = "CATEGORY"
    else:
        raise ValueError("Invalid CMID format. Must start with 'AD/SD' or 'AM/SM'.")
        
    # determine if properties are node or relationship properties
    if domain == "DATASET":
        prop_query = """
    match (p:PROPERTY) where p.CMName in $properties and not p.type = "relationship" return p.CMName as property, p.type as type
    """
    elif domain == "CATEGORY":
        prop_query = """
    match (p:PROPERTY) where p.CMName in $properties and not p.nodeType = "DATASET" return p.CMName as property, p.type as type
    """
    else:
        raise ValueError("Invalid domain. Must be 'DATASET' or 'CATEGORY'.")
    
    prop_metadata = getQuery(query=prop_query, driver=driver, properties=properties)
    
    # format return query as c.{property} if property is a node property or r.{property} if relationship property
    node_properties = [p['property'] for p in prop_metadata if p['type'] == 'node']
    relationship_properties = [p['property'] for p in prop_metadata if p['type'] == 'relationship']

    node_query = [f"c.{prop} as {prop}" for prop in node_properties]
    relationship_query = [f"r.{prop} as {prop}" for prop in relationship_properties]
    if not node_query and not relationship_query:
        raise ValueError("No valid properties found for the given CMIDs.")
    prop_query = ", ".join(node_query + relationship_query)   
    
    if domain == "DATASET":
        query = f"""
        unwind $CMID as cmid
        match (c:DATASET) where c.CMID = cmid
        return c.CMID as CMID, c.CMName as CMName, {prop_query} 
        """
    elif domain == "CATEGORY":
        query = f"""
        unwind $CMID as cmid
        match (c:CATEGORY)<-[r:USES]-(:DATASET) where c.CMID = cmid
        return c.CMID as CMID, c.CMName as CMName, {prop_query} 
        """
        
    result = getQuery(query=query, driver=driver, CMID=CMID, type = "df")
    
    # convert list columns to strings separated by "; "
    for col in result.columns:
        if result[col].apply(lambda x: isinstance(x, list)).any():
            result[col] = result[col].apply(lambda x: "; ".join(x) if isinstance(x, list) else x)
    
    # convert all to strings
    result = result.astype(str)
    return result.to_dict(orient='records')
