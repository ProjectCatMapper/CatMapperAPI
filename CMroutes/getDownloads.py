from CM import get_backup_csv_urls, getAdvancedDownload
from flask import request


def get_backup_csv_urls_route(database):
    try:
        mostRecent = request.args.get('mostRecent', 'true').lower() == 'true'
        if mostRecent not in [True, False]:
            mostRecent = True  # Default to True if not specified correctly
        urls = get_backup_csv_urls(database, mostRecent=mostRecent)
        return {"urls": urls}
    except Exception as e:
        return {"error": str(e)}, 500

def get_advanced_download_route(database):
    try:
        CMIDs = request.json.get('CMIDs', [])
        domain = request.json.get('domain', "")
        properties = request.json.get('properties', [])
        # return {"CMIDs":CMIDs,"properties":properties}
        if not properties:
            return {"error": "Properties must be provided"}, 400
        data = getAdvancedDownload(database,domain, properties,CMIDs)
        return {"data": data}
    except Exception as e:
        return {"error": str(e)}, 500
