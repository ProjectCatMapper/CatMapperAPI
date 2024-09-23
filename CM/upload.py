''' upload.py '''

from .utils import *
from .USES import *
import json
import pandas as pd
from flask import jsonify
import numpy as np
import time
import re

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

def createUSES(links,database,user, create = "MERGE"):
    try:
        start_time = time.time()
        if 'from' not in links.columns or 'to' not in links.columns:
            raise ValueError("Must have 'from' and 'to' columns")

        if 'Key' not in links.columns:
            raise ValueError("Must have 'Key' column")

        # Split 'from' and 'to' on "; " and trim whitespace
        links['from'] = links['from'].apply(lambda x: x.split('; ') if isinstance(x, str) else []).apply(lambda x: [item.strip() for item in x]).apply(lambda x: '; '.join(x))
        links['to'] = links['to'].apply(lambda x: x.split('; ') if isinstance(x, str) else []).apply(lambda x: [item.strip() for item in x]).apply(lambda x: '; '.join(x))


        # Database connection assumed via driver
        driver = getDriver(database)

        if 'label' not in links.columns:
            raise ValueError("Must have 'label' column")

        if create.lower() not in ['merge', 'create']:
            raise ValueError("create must be either 'merge' or 'create'")

        # Remove duplicates
        links = links.drop_duplicates()

        # Fetch properties from the database
        db_properties = getQuery("MATCH (p:PROPERTY) RETURN p.property AS property", driver)
        db_properties_list = [item['property'] for item in db_properties]
        existing_columns = list(set(db_properties_list) & set(links.columns))
        links[existing_columns] = links[existing_columns].applymap(lambda x: re.sub(r'\s+', '', str(x)) if pd.notnull(x) else x)

        links['log'] = links.apply(lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: created relationship", axis=1)

        # Convert all values to strings and replace NaN with empty strings
        links = links.fillna("").astype(str)

        # Select the appropriate columns based on the relationship type
        vars = links.columns.difference(['from', 'to', 'Key'])

        query = """
match (n:METADATA:PROPERTY) 
return n.property as property, n.type as type, 
n.relationship as relationship, n.description as description, 
n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
"""

        metaTypes = getQuery(query, driver)
        metaTypeDict = {item['property']: item['metaType'] for item in metaTypes}

        keys = []
        for var in vars:
            metaType = metaTypeDict.get(var)  # Get the metaType for the given property
            
            if metaType == "string" or metaType == "JsonMap":
                # If it's a string or JsonMap, use trim on the property
                keys.append(f"r.{var} = trim(row.{var})")
            else:
                # If it's a list or another type, use Cypher-style list comprehension
                keys.append(f"r.{var} = apoc.coll.toSet([i IN apoc.coll.flatten(split(row.{var}, ';')) WHERE trim(i) <> ''])")
                
        # Combine the keys into a single string for the Cypher query
        keys_string = ", ".join(keys)

        onCreate = "" if create.lower() == "create" else "ON CREATE "

        # Create Cypher query for adding relationships
        q = f"""
        UNWIND $rows AS row
        MATCH (a) WHERE row.from = a.CMID
        MATCH (b) WHERE row.to = b.CMID
        {create} (a)-[r:USES {{Key: row['Key']}}]->(b)
        {onCreate}SET r.status = 'update', {keys_string}
        RETURN id(b) AS nodeID, b.CMID AS CMID
        """

        # Get the number of relationships before adding
        nRels = getQuery("MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list")

        # Execute the query and return results
        print("Uploading to database")
        print(q)
        links_dict = links.to_dict(orient='records')
        result = getQuery(q, driver, params={'rows':links_dict})

        # Update alternate names
        CMIDs = [item['CMID'] for item in result]
        updateAltNames(driver,CMIDs)

        # Get the number of relationships after adding
        nRels2 = getQuery("MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list")
        new_rels = nRels2[0] - nRels[0]
        print(f"Number of new relationships in database: {new_rels}")

        end_time = time.time()
        print(f"Elapsed time: {end_time - start_time} seconds")

        return {"q": result, "links": links_dict}

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

def updateProperty(links, database, user, updateType):
    try:
        if not updateType in ['overwrite','update']:
            raise Exception("type must be update or overwrite.")

        driver = getDriver(database)

        requiredCols = ["from", "to", "Key"]

        for required in requiredCols:
            if required not in links.columns:
                raise ValueError(f"Missing required column {required}")
            
        vars = links.drop(columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

        if updateType == "update":
            links['log'] = links.apply(lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: updated properties {', '.join([str(var) for var in vars])}", axis=1)
        else:
            links['log'] = links.apply(lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: overwrote properties {', '.join([str(var) for var in vars])}", axis=1)

        vars = links.drop(columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

        query = """
match (n:METADATA:PROPERTY) 
return n.property as property, n.type as type, 
n.relationship as relationship, n.description as description, 
n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
"""

        metaTypes = getQuery(query, driver)
        metaTypeDict = {item['property']: item['metaType'] for item in metaTypes}

        keys = []
        for var in vars:
            metaType = metaTypeDict.get(var)  # Get the metaType for the given property
            if updateType == "overwrite" and var != 'log':
                keys.append(f"r.{var} = custom.combinedProperties('',row.{var},'{metaType}')[0].prop")
            else:
                keys.append(f"r.{var} = custom.combinedProperties(r.{var},row.{var},'{metaType}')[0].prop")

        keys = ", ".join(keys)

        q = f"""
        UNWIND $rows AS row
        MATCH (a {{CMID: row.from}})-[r:USES {{Key: row.Key}}]->(b {{CMID: row.to}}) 
        WITH row, r, b
        SET {keys} 
        RETURN id(b) as nodeID, b.CMID as CMID
        """

        links_dict = links.to_dict(orient = "records")

        print(q)
        
        result = getQuery(query = q, driver = driver, params = {"rows": links_dict})
        
        return {'result': result, 'links': links_dict}
    except Exception as e:
        return f"Error: {str(e)}"