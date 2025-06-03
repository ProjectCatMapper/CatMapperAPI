""" download datasets updated weekly from AWS """

import boto3
import os
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/app/aws_credentials"


def get_backup_csv_urls(database, bucket="sociomap-backups", region="us-west-1"):
    """
    Returns a list of full S3 URLs to backup CSV files for the given database.

    Parameters:
        database (str): "ArchaMap" or "SocioMap"
        bucket (str): S3 bucket name (default: "sociomap-backups")
        region (str): AWS region (default: "us-west-1")

    Returns:
        list: List of full S3 URLs to CSV files
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

    urls = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".csv"):
                url = f"https://{bucket}.s3.{region}.amazonaws.com/{key}"
                urls.append(url)

    return urls
