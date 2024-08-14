''' upload.py '''

from .utils import *
import json
import pandas as pd
from flask import jsonify
import numpy as np

data = [{"CMID":"test-1","datasetID":"SD11","Key":"test-1","geoCoords":"yep","yearStart":2011}]
df = pd.DataFrame(data)

def createNodes(df,database,user):
    try:

        driver = getDriver(database)

        labels = getQuery("MATCH (l:LABEL) return l.label as label", driver, type = "list")

        if "label" in df.columns:
            if "DATASET" in df["label"]:
                isDataset = True
            else:
                isDataset = False
        else: 
            raise Exception("Error: label column is required.")

        if "CATEGORY" in df["label"].values:
            raise Exception("Error: label must be more specific than CATEGORY")

        if not all(label in labels for label in df["label"].unique()):
            raise Exception("Error: label is not valid.")

        if isDataset:
            required = ["CMName","label","DatasetCitation","shortName"]
        else:
            required = ["CMName","label"]
            df['label'] = df['label'].apply(lambda x: f"CATEGORY:{x}")

        if not all(column in df.columns for column in required):
            raise Exception("Error: missing required columns.")

        if not 'uniqueID' in df.columns:
            getQuery("MATCH (c) where not c.uniqueID is null set c.uniqueID = NULL", driver)
            distinct_nodes = df.drop_duplicates(subset='CMName')
            if len(distinct_nodes) != len(df):
                raise Exception("Error: there must be a unique name for each new node.")
            else:
                df['uniqueID'] = df.index

        newID = getAvailableID(new_id = "CMID", n = len(df), database = database)

        print(newID)

        df["CMID"] = newID

        df = df.astype(str)

        vars = [col for col in df.columns if 'label' not in col and 'uniqueID' not in col]

        properties = getQuery("MATCH (p:PROPERTY) return p.property as property", driver, type = "list")

        missing_vars = [var for var in vars if var not in properties]

        if missing_vars:
            raise Exception(f"Error: The following vars are not in properties: {', '.join(missing_vars)}")

        set_clause = ', '.join([f"a.{var} = row.{var}" for var in vars])

        return_clause = ', '.join([f"a.{var} as {var}" for var in vars])

        q = f"""
        unwind $rows as rows
        unwind rows as row
        call apoc.cypher.doIt('
        MERGE (a:' + row.label + ' {{uniqueID: row.uniqueID}})
        ON CREATE SET 
        {set_clause},
        a.log = toString(datetime()) + " user {user}: created node"
        return a',
        {{row: row}}) yield value 
        with value.a as a 
        return distinct id(a) as nodeID,
        {return_clause}
        """

        rows = df.to_dict(orient='records')

        results = getQuery(query = q, driver = driver, params = {"rows": rows})

        results_df = pd.DataFrame(results)

        for var in vars:
            if not np.all(np.isin(df[var].values, results_df[var].values)):
                raise Exception(f"Error: values for {var} were not uploaded correctly. Please check upload")
            
        return results
    
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

def overwriteProperty(df,driver):
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

        keys = ", ".join(keys)





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

def CMoverwriteProperty(links, properties, user, con):

    if isinstance(properties, str):
        properties = [properties, properties]

    vars = links.columns.difference(['from', 'to', 'Key'])
    
    keys = ', '.join([f"r.{var} = apoc.coll.toSet(split(row.{var}, '; '))" for var in vars])

    q = f"""
    MATCH (a {{{properties[0]}: row.from}})-[r:USES {{Key: row.Key}}]->(b {{{properties[1]}: row.to}}) 
    SET {keys} 
    RETURN id(b) as nodeID, b.CMID as CMID
    """
    
    result = CMimportFromS3(con, q, links)
    
    return {'q': result, 'links': links}

def CMoverwritePropertyAPI(links, properties, con, user):

    if isinstance(properties, str):
        properties = [properties, properties]

    vars = links.columns.difference(['from', 'to', 'Key'])
    
    keys = ', '.join([f"r.{var} = apoc.coll.toSet(split(row.{var}, '; '))" for var in vars])

    q = f"""
    UNWIND $rows AS row
    MATCH (a {{{properties[0]}: row.from}})-[r:USES {{Key: row.Key}}]->(b {{{properties[1]}: row.to}})
    SET {keys} 
    RETURN id(b) as nodeID, b.CMID as CMID
    """
    
    result = CMcypherQueryAPI(database=con.database, query=q, user="1", pwd=os.getenv('apipwd'), params={'rows': links.to_dict(orient='records')})
    
    return {'q': result, 'links': links}

def CMupdateProperty(links, properties, con, user):
    requiredCols = ['from', 'to', 'Key']

    for required in requiredCols:
        if required not in links.columns:
            raise ValueError(f"missing required column {required}")

    vars = links.columns.difference(requiredCols)
    
    keys = ', '.join([f"custom.getNonNullProp(r.{var}, row.{var}) AS {var}" for var in vars])
    keys2 = ', '.join([f"r.{var} = {var}[0].prop" for var in vars])

    if isinstance(properties, str):
        properties = [properties, properties]

    q = f"""
    MATCH (a {{{properties[0]}: row.from}})-[r:USES {{Key: row.Key}}]->(b {{{properties[1]}: row.to}}) 
    WITH r, b, {keys} 
    SET {keys2} 
    RETURN id(b) as nodeID, b.CMID as CMID
    """
    
    result = CMimportFromS3(con, q, links)
    
    return {'q': result, 'links': links}

def CMimportFromS3(con, query, links, CQLOnly=False):
    with con.session() as session:
        if CQLOnly:
            return query
        result = session.run(query, rows=links.to_dict(orient='records'))
        return pd.DataFrame([record.data() for record in result])

def CMcypherQueryAPI(database, query, user, pwd, params):
    # This function should implement the logic to interact with the Neo4j API
    pass  # Replace this with actual implementation


