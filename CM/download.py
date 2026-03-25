""" download datasets updated weekly from AWS """

import boto3
import os
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/app/aws_credentials"
from datetime import datetime
import re
from configparser import ConfigParser
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from CM import getDriver, getQuery
import pandas as pd

BACKUP_SOURCE_MAP = {
    "ArchaMap": {
        "s3_prefix": "archamap-backups/download",
        "local_dir": "/db/archamap1/backups/download",
    },
    "SocioMap": {
        "s3_prefix": "sociomap1-backups/download",
        "local_dir": "/db/sociomap1/backups/download",
    },
}


def _aws_client_kwargs_from_config():
    """Read optional AWS credentials from config.ini."""
    parser = ConfigParser()
    parser.read(["config.ini", "/app/config.ini"])
    if not parser.has_section("AWS"):
        return {}

    access_key = parser.get("AWS", "AccessKeyId", fallback="").strip()
    secret_key = parser.get("AWS", "SecretAccessKey", fallback="").strip()
    session_token = parser.get("AWS", "SessionToken", fallback="").strip()

    if not access_key or not secret_key:
        return {}

    kwargs = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    }
    if session_token:
        kwargs["aws_session_token"] = session_token
    return kwargs


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
    if database not in BACKUP_SOURCE_MAP:
        raise ValueError(
            f"Unknown database: {database}. Must be 'ArchaMap' or 'SocioMap'.")

    source = BACKUP_SOURCE_MAP[database]
    prefix = source["s3_prefix"]
    local_dir = source["local_dir"]
    aws_client_kwargs = _aws_client_kwargs_from_config()

    def list_s3_pages(unsigned=False):
        client_kwargs = {"region_name": region}
        if unsigned:
            client_kwargs["config"] = Config(signature_version=UNSIGNED)
        else:
            client_kwargs.update(aws_client_kwargs)
        s3 = boto3.client("s3", **client_kwargs)
        paginator = s3.get_paginator('list_objects_v2')
        return paginator.paginate(Bucket=bucket, Prefix=prefix)

    date_pattern = re.compile(r'_(\d{4}-\d{2}-\d{2})\.csv$')

    def collect_file_info_from_s3(unsigned=False):
        file_info = []
        page_iterator = list_s3_pages(unsigned=unsigned)
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
        return file_info

    def collect_file_info_from_local():
        file_info = []
        if not os.path.isdir(local_dir):
            return file_info

        for filename in os.listdir(local_dir):
            if not filename.endswith(".csv"):
                continue
            match = date_pattern.search(filename)
            if not match:
                continue
            try:
                file_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
                size_bytes = os.path.getsize(os.path.join(local_dir, filename))
                size_mb = round(size_bytes / (1024 * 1024), 2)
                file_info.append((file_date, f"{prefix}/{filename}", size_mb))
            except (ValueError, OSError):
                continue
        return file_info

    try:
        file_info = collect_file_info_from_s3(unsigned=False)
    except (NoCredentialsError, ClientError):
        try:
            # Backup files may be public; attempt read-only unsigned listing.
            file_info = collect_file_info_from_s3(unsigned=True)
        except ClientError:
            # Last resort: use local backup directories mounted into the API container.
            file_info = collect_file_info_from_local()

    if mostRecent and file_info:
        most_recent_date = max(date for date, _, _ in file_info)
        file_info = [(date, key, size) for date, key, size in file_info if date == most_recent_date]

    # Build list of (URL, size_MB) tuples
    results = [
        (f"https://{bucket}.s3.{region}.amazonaws.com/{key}", size)
        for _, key, size in file_info
    ]

    return results

def getAdvancedDownload(database,domain, properties,CMIDs):
    """
    Returns a dataset with the given CMIDs and properties.

    Parameters:
        database (str): "ArchaMap" or "SocioMap"
        CMID (str): The CatMapperID of the content to download
        properties (dict): Additional properties to include with the download

    Returns:
        list of dict: Each dict contains the CMID, CMName, and requested properties
    """
    driver = getDriver(database)
    
    # if isinstance(CMID, str):
    #     CMID = [CMID]
    if isinstance(properties, str):
        properties = [properties]
        
    if not isinstance(properties, list):
        raise ValueError("Properties must be lists.")
        
    # determine if properties are node or relationship properties

    prop_query1 = """
    match (p:PROPERTY) where p.CMName in $properties and not p.type = "relationship" return p.CMName as property, p.type as type
    """
    prop_query2 = """
    match (p:PROPERTY) where p.CMName in $properties and (p.nodeType contains "CATEGORY" or p.nodeType is null) return p.CMName as property, p.type as type
    """
    
    prop_metadata1 = getQuery(query=prop_query1, driver=driver, properties=properties)
    prop_metadata2 = getQuery(query=prop_query2, driver=driver, properties=properties)
    
    # format return query as c.{property} if property is a node property or r.{property} if relationship property
    node_properties1 = [p['property'] for p in prop_metadata1 if p['type'] == 'node']
    node_properties2 = [p['property'] for p in prop_metadata2 if p['type'] == 'node']
    relationship_properties = [p['property'] for p in prop_metadata2 if p['type'] == 'relationship']

    node_query1 = [f"c.{prop} as {prop}" for prop in node_properties1]
    node_query2 = [f"c.{prop} as {prop}" for prop in node_properties2]
    relationship_query = [f"r.{prop} as {prop}" for prop in relationship_properties]
    if not node_query1 and not node_query2 and not relationship_query:
        raise ValueError("No valid properties found for the given CMIDs.")
    
    prop_query = ", ".join(node_query2 + relationship_query)  
    if node_query1: 
        node_query1 = "," + ", ".join(node_query1)
    else: 
        node_query1 = ""
    
    query1 = f"""
        unwind $CMID as cmid
        match (c:DATASET) where c.CMID = cmid
        return c.CMID as CMID, c.CMName as CMName {node_query1} 
        """
    query2 = f"""
        unwind $CMID as cmid
        match (c:CATEGORY)<-[r:USES]-(d:DATASET) where c.CMID = cmid
        return c.CMID as CMID, c.CMName as CMName, labels(c) as domains, apoc.text.join(collect(distinct d.CMID),"; ") as datasets, {prop_query} 
        """
    
    result1 = getQuery(query=query1, driver=driver, CMID=CMIDs, type = "df")
    result2 = getQuery(query=query2, driver=driver, CMID=CMIDs, type = "df")

    result = pd.concat([result1, result2])

    def agg_semicolon(series):
        # flatten any lists
        values = []
        for v in series.dropna():
            if isinstance(v, list):
                values.extend(v)
            else:
                values.append(v)
        # keep unique values, preserve order
        seen = []
        for v in values:
            if v not in seen:
                seen.append(v)
        return "; ".join(map(str, seen)) if seen else ""
    
# group and aggregate all columns
    group_cols = ["CMID"]
    result = result.groupby(group_cols, as_index=False).agg(agg_semicolon)
    # convert all to strings
    result = result.astype(str)
    return result.to_dict(orient='records')
