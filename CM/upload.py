''' upload.py '''

from .utils import *
import json
import pandas as pd
from flask import jsonify

data = [{"CMID":"test-1","datasetID":"SD11","Key":"test-1","geoCoords":"yep"}]
df = pd.DataFrame(data)

def createNodes(df,driver):
    try:
        required = ["CMID","CMName","label"]

        return "unfinished"
    
    except Exception as e:
        return str(e), 500

def createUSES(df,driver):
    try:
        required = ["CMID","CMName","label"]

        return "unfinished"

    except Exception as e:
        return str(e), 500

def modifyNodes(df,driver):
    try:
        required = ["CMID","CMName","label"]

        return "unfinished"
    
    except Exception as e:
        return str(e), 500

def modifyUSES(df,driver):
    try:
        required = ["CMID","datasetID","Key"]
        check = validateCols(df,required)
        if check is not True:
            return check
        
        properties = getPropertiesMetadata(driver)
        properties = pd.DataFrame(properties)

        exclude_columns = ["CMID", "datasetID", "Key"]
        vars = [col for col in df.columns if col not in exclude_columns]

        vars = pd.DataFrame(vars,columns = ["property"])
        vars = pd.merge(vars,properties)
        vars = vars.to_dict(orient='records')

        keys = []
        for row in vars:
            var = row['property']
            type = row['type']
            if type == "string":
                keys.append(f"r.{var} = row.{var}")
            elif type == "integer":
                keys.append(f"r.{var} = toInt(row.{var})")
            elif type == "list":
                keys.append(f"r.{var} = split(row.{var},' || ')")
            else:
                keys.append(f"r.{var} = row.{var}")





        return "unfinished"

    except Exception as e:
        return str(e), 500   

    
def advancedValidate(df,uploadType,domain,driver):
    try:
        if domain == "DATASET":
            if uploadType == "usenodes":
                required = ["CMName",
                         "label",
                         "shortName",
                         "DatasetCitation"]
            else:
                raise Exception("Invalid uploadType for DATASET")
        else:
            if uploadType == "newnodes":
                required = ["CMName", "Name","Key", "label", "datasetID"]
            elif uploadType == "newuses":
                required = ["CMName", "Name","Key", "label"]
            elif uploadType == "add":
                required = ['CMID', "Key", "label", "datasetID"]
            elif uploadType == "replace":
                required = ["CMName", "Name","Key", "label"]

        return validateCols(df,required)
    except Exception as e:
        return str(e), 500

def advancedUpload(data):
    try:
        database = unlist(data.get('database'))
        uploadType = unlist(data.get('uploadType'))
        df = data.get('df')
        df = pd.DataFrame(df)
        if 'label' in df.columns:
            domain = df['label']
            domain = domain.unique()
            if len(domain) > 1:
                if 'DATASET' in domain:
                    raise Exception("Cannot upload multiple domains with a DATASET domain")
                else:
                    domain = domain[0]    
        else:
            domain = None

        driver = getDriver(database)
        check = advancedValidate(df,uploadType,domain,driver)
        if check is not True:
            yield check
        yield "\n"
        yield "starting advanced upload\n"
        yield f"uploading to {database}\n"
        yield "finished advanced upload\n"
        result = json.dumps(data)
        yield result
    except Exception as e:
        yield str(e), 500