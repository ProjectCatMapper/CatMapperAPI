''' upload.py '''

from .utils import *

def advancedValidate(data):
    try:
        data = json.loads(data)
        database = CM.unlist(data.get('database'))

        driver = validateDatabase(database)

        return 'advancedValidate'
    except Exception as e:
        return str(e), 500

def advancedUpload():
    try:
        return 'advancedUpload'
    except Exception as e:
        return str(e), 500