from CM import get_backup_csv_urls, getAdvancedDownload
from flask import request, Blueprint, abort, send_from_directory
import os

download_bp = Blueprint('download', __name__)

@download_bp.route('/CSVURLs/<database>', methods=['GET'])
def get_backup_csv_urls_route(database):
    try:
        mostRecent = request.args.get('mostRecent', 'true').lower() == 'true'
        if mostRecent not in [True, False]:
            mostRecent = True  # Default to True if not specified correctly
        urls = get_backup_csv_urls(database, mostRecent=mostRecent)
        return {"urls": urls}
    except Exception as e:
        return {"error": str(e)}, 500

@download_bp.route('/download/advanced/<database>', methods=['POST'])
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

@download_bp.route("/download/test", methods=["GET"])
def test_download():

    filename = "test.txt"
    TMP_DIR = "/app/tmp"
    file_path = os.path.join(TMP_DIR, filename)

    if os.path.exists(file_path):
        return send_from_directory(TMP_DIR, filename, as_attachment=True)
    else:
        abort(404, description="test.txt not found")
        
@download_bp.route('/download/zip/<hash_id>', methods=['GET'])
def download_zip(hash_id):

    import subprocess

    TMP_DIR = "/app/tmp"

    subprocess.run(["chmod", "-R", "777", TMP_DIR], check=True)

    filename = f"merged_output_{hash_id}.zip"
    file_path = os.path.join(TMP_DIR, filename)

    if os.path.exists(file_path):
        return send_from_directory(TMP_DIR, filename, as_attachment=True)
    else:
        abort(404, description=f"{file_path} not found")
