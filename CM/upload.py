''' upload.py '''

from .utils import *
from .USES import *
import json
import pandas as pd
from flask import jsonify
import numpy as np
import time
import re
import warnings

data = [{"CMID":"test-1","datasetID":"SD11","Key":"test-1","geoCoords":"yep","yearStart":2011}]
df = pd.DataFrame(data)

def createNodes(df,database,user):
    try:

        driver = getDriver(database)

        labels = getQuery("MATCH (l:LABEL) return l.label as label", driver, type = "list")

        df = df.copy()

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
        
        links = links.copy()

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
        MATCH (a:DATASET) WHERE row.from = a.CMID
        MATCH (b:CATEGORY) WHERE row.to = b.CMID
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
        print(f"Elapsed time: {int(end_time - start_time)} seconds")

        return {"q": result, "links": links_dict}

    except Exception as e:
        if isinstance(e, tuple):
            error_message = ', '.join(map(str, e))
        else:
            error_message = str(e)
        return error_message, 500

def combine_properties(df, group_by_cols):
    
    def combine_column(column):
        if isinstance(column, list):
            return "; ".join(sorted(set([str(x).strip() for x in column if pd.notna(x)])))
        return column
    
    grouped_df = df.groupby(group_by_cols, as_index=False).agg(lambda x: x.tolist())
    
    for col in grouped_df.columns:
        if col not in group_by_cols:
            grouped_df[col] = grouped_df[col].apply(combine_column)
    
    return grouped_df

def combine_names_and_altNames(df, name_col, alt_name_col):
    print(df.head())
    print(name_col)
    print(alt_name_col)
    df['combinedNames'] = df.apply(
        lambda row: "; ".join(
            list(filter(None, [row[name_col]] + row[alt_name_col] if pd.notnull(row[alt_name_col]) else []))
        ), axis=1
    )
    return df


def handle_geo_coordinates(df, geo_col):
  
    def extract_coordinates(geo):
        if isinstance(geo, list) and len(geo) >= 2:
            return geo[1], geo[0]
        elif isinstance(geo, str):
            parts = geo.split(',')
            if len(parts) == 2:
                return float(parts[0].strip()), float(parts[1].strip())
        return None, None
    
    df['latitude'], df['longitude'] = zip(*df[geo_col].apply(extract_coordinates))
    
    return df

def input_Nodes_Uses(dataset,
                     database,
                 CMName=None,
                 Name=None,
                 CMID=None,
                 altNames=None,
                 Key=None,
                 formatKey=False,
                 datasetID=None,
                 label=None,
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=None, 
                 linkContext=None,
                 user=None,
                 overwriteProperty=False,
                 updateProperty=False,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=100,
                 ):
    
    print("starting database upload")

    with open(f"{user}uploadProgress.txt", 'w') as f:
        f.write("Starting database upload")

    if nodeContext is None:
        nodeContext = []

    if linkContext is None:
        linkContext = []
    driver = getDriver(database)

    if formatKey is True:
        raise Exception("Error: formatKey must be False")
    
    if geocode is True:
        raise Exception("Error: geocode must be False")
    
    
    if 'eventType' in dataset.columns and 'eventDate' not in dataset.columns:
        dataset['eventDate'] = np.nan

    print("checking whether upload is for DATASET nodes")
    
    if label is None:
        isDataset = False
    else:
        isDataset = label == "DATASET" or dataset['label'].iloc[0] == "DATASET"
    
    if isDataset:
        print("upload is for DATASET nodes")
    else:
        print("upload is for CATEGORY nodes")

    dataset = dataset.dropna(axis=1, how='all')

    print("checking column names")
    
    columns_to_select = [CMName, Name, CMID, altNames, Key, datasetID, label, uniqueID, 
                        "shortName", "DatasetCitation"] + nodeContext + linkContext
    dataset = dataset[[col for col in columns_to_select if col in dataset.columns]]

    if isDataset:
        column_names = [CMName, label, uniqueID] + nodeContext
    else:
        if overwriteProperty or updateProperty:
            column_names = [Name, altNames, CMID, Key, datasetID, uniqueID] + nodeContext + linkContext
        else:
            column_names = [CMName, Name, altNames, label, Key, datasetID, uniqueID] + nodeContext + linkContext

    # Remove None values
    column_names = [col for col in column_names if col is not None]

    errors = [f"{col} must be in dataset" for col in column_names if col not in dataset.columns]

    if len(errors) > 0:
        with open(f"log/{user}uploadProgress.txt", 'a') as f:
            f.write("\n".join(errors))
        raise ValueError("\n".join(errors))

    properties = getPropertiesMetadata(driver)
    properties = pd.DataFrame(properties)

    if not "label" in dataset.columns:
        raise ValueError("Must include label")

    if uniqueID is None or uniqueID not in dataset.columns:
        print("Creating import ID")
        getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver)
        uniqueID = 'importID'
        uniqueProperty = 'importID'
        dataset['importID'] = dataset.index + 1

    sq = range(0, len(dataset), batchSize)

    try:
        dataset_match = pd.DataFrame()
        for s in sq:
            print(f"Beginning upload of rows {s} to {s + batchSize}")
            sub_dataset = dataset.iloc[s:s + batchSize]
            max_row = len(sub_dataset) - 1 + s
            with open(f"log/{user}uploadProgress.txt", 'a') as f:
                f.write(f"uploading {s} to {max_row} of {len(dataset)}")

            if not isDataset:
                print("Combining paired properties")
                paired = properties.merge(pd.DataFrame({'property': sub_dataset.columns}), on='property')
                paired = paired[paired['group'].notna()].groupby('group')
                
                for group, pair in paired:
                    if pair['property'].isin(sub_dataset.columns).any():
                        sub_dataset[group] = sub_dataset[pair['property']].apply(lambda x: x.str.strip()).agg('; '.join, axis=1)
                        linkContext.append(group)

            if 'CMID' in sub_dataset.columns:
                if "datasetID" in sub_dataset.columns and "Key" in sub_dataset.columns:
                    if 'CMID' in sub_dataset.columns:
                        sub_dataset = combine_properties(sub_dataset, ['CMID', 'datasetID', 'Key'])
                    else:
                        sub_dataset = combine_properties(sub_dataset, ['datasetID', 'Key'])

            if addDistrict:
                print("Adding district")
                matches = getQuery(params={'rows': sub_dataset[['datasetID']]}, q='DISTRICT QUERY', database=database, user='1')
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(matches, on="datasetID", how="left")
                    linkContext.append('country')

            if addRecordYear:
                print("Adding record year")
                matches = getQuery(params={'rows': sub_dataset[['datasetID']]}, q='RECORD_YEAR QUERY', driver = driver)
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(matches, on="datasetID", how="left")
                    linkContext.append('recordStart')

            sub_dataset = sub_dataset.fillna('')

            node_columns = [CMName, uniqueID, 'label'] + nodeContext
            node_columns = [col for col in node_columns if col in sub_dataset.columns]  


            if isDataset:
                nodes = sub_dataset[[CMName, "shortName", "DatasetCitation", uniqueID, 'label'] + nodeContext].drop_duplicates()
            else:
                if Name:
                    nodes = sub_dataset[sub_dataset['CMID'] == ''][node_columns].drop_duplicates()
                else:
                    nodes = pd.DataFrame()

            if not nodes.empty:
                print("Adding nodes")
                match = createNodes(nodes,driver, uniqueID=uniqueID, uniqueProperty=uniqueProperty, user=user, checkUnique=False)
                dataset_match = pd.concat([dataset_match, match], ignore_index=True)

            link_columns = ['datasetID', CMName, 'CMID', Name, altNames, Key, uniqueID, label] + linkContext
            link_columns = [col for col in link_columns if col in sub_dataset.columns]

            if not isDataset:
                print("Adding USES relationships")
                links = sub_dataset[link_columns].drop_duplicates().copy()

                links.rename(columns={'datasetID': 'from', 'CMID': 'to'}, inplace=True)
                
                if Name and altNames is not None:
                    links = combine_names_and_altNames(links, Name, altNames)
                
                if linkContext is not None and 'geoCoords' in linkContext:
                    links = handle_geo_coordinates(links, properties)

                link_cols = ['from', 'to', 'Key'] + linkContext
                link_cols = [col for col in link_cols if col in links.columns]
                if overwriteProperty:
                    print("Overwriting property")
                    result = updateProperty(links[link_cols], database = database, user = user, updateType = "overwrite")
                elif updateProperty:
                    print("Updating property")
                    result = updateProperty(links[link_cols], database = database, user = user, updateType = "update")
                else:
                    print("Adding new USES relationships")
                    link_cols = link_cols + [label]
                    result = createUSES(links[link_cols],database = database, user = user, create = "CREATE")
                print("Completed updating USES relationships")

            print("Processing returned CMIDs")
            try:
                # print(result)
                cmid_values = [link['to'] for link in result['links']]
                updateAltNames(driver, CMID = cmid_values)
                print("updated alternate names")
            except KeyError as e:
                print(f"Error updating alternate names: {e}")
                continue

            if uniqueID == 'importID':
                getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver = driver)

    except Exception as e:
        try:
            if isinstance(e, tuple):
                error_message = ', '.join(map(str, e))
            else:
                error_message = str(e)
            warnings.warn(error_message)
            with open(f"log/{user}uploadProgress.txt", 'a') as f:
                f.write(f"Error: {error_message}\n")

            # Return None
        except Exception as internal_error:
            warnings.warn(f"Failed to process the exception: {internal_error}")
            with open(f"log/{user}uploadProgress.txt", 'a') as f:
                f.write(f"Failed to process the exception: {internal_error}\n")
        return None

    with open(f"log/{user}uploadProgress.txt", 'a') as f:
        f.write("Completed dataset upload")

    return dataset_match

    
# def advancedUpload(data):
#     try:
#         database = unlist(data.get('database'))
#         uploadType = unlist(data.get('uploadType'))
#         df = data.get('df')
#         df = pd.DataFrame(df)
#         if 'label' in df.columns:
#             domain = df['label']
#             domain = domain.unique()
#             if len(domain) > 1:
#                 if 'DATASET' in domain:
#                     raise Exception("Cannot upload multiple domains with a DATASET domain")
#                 else:
#                     domain = domain[0]    
#         else:
#             domain = None

#         driver = getDriver(database)
#         # check = advancedValidate(df,uploadType,domain,driver)
#         if check is not True:
#             yield check
#         yield "\n"
#         yield "starting advanced upload\n"
#         yield f"uploading to {database}\n"
#         yield "finished advanced upload\n"
#         result = json.dumps(data)
#         yield result
#     except Exception as e:
#         yield str(e), 500

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
        MATCH (a:DATASET {{CMID: row.from}})-[r:USES {{Key: row.Key}}]->(b:CATEGORY {{CMID: row.to}}) 
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