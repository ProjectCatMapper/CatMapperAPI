from CM import get_backup_csv_urls


def get_backup_csv_urls_route(database):
    try:
        urls = get_backup_csv_urls(database)
        return {"urls": urls}
    except Exception as e:
        return {"error": str(e)}, 500
