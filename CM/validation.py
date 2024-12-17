# tests

from .utils import *
import pandas as pd
import json

def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False

def validateJSON(database, property = 'parentContext', path ="/mnt/storage/app/tmp/invalid_json.xlsx" ):
    try:

        driver = getDriver(database)

        query = f"match (c:CATEGORY)<-[r:USES]-(d:DATASET) where not r.{property} is null return d.CMID as datasetID, c.CMID as CMID, r.Key as Key, r.{property} as prop"

        results = getQuery(query, driver)

        results = pd.DataFrame(results)

        results = pd.DataFrame.explode(results, "prop")

        results["is_valid_json"] = results["prop"].apply(is_valid_json)

        invalid = results[results["is_valid_json"] == False]

        invalid.to_excel(path)

        invalid = invalid.to_dict(orient="records")

        return(invalid)

    except Exception as e:
        return "Unable to validate JSON properties: " + str(e)
    
