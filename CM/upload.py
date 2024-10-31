''' upload.py '''

from .utils import *
from .USES import *
from .keys import *
import json
import pandas as pd
from flask import jsonify
import numpy as np
import time
import re
import warnings
import math
import warnings
warnings.simplefilter('error', UserWarning)

data = [{"CMID":"test-1","datasetID":"SD11","Key":"test-1","geoCoords":"yep","yearStart":2011}]
df = pd.DataFrame(data)

def updateLog(f,txt, write = 'a'):
    print(txt)
    with open(f, write) as file:
        file.write(txt + "\n")

def createNodes(df,database,user):
    try:

        driver = getDriver(database)

        labels = getQuery("MATCH (l:LABEL) return l.label as label", driver, type = "list")

        df = df.copy()

        if "label" in df.columns:
            if "DATASET" in df["label"].values:
                isDataset = True
            else:
                isDataset = False
        else: 
            raise Exception("Error: label column is required.")

        if "CATEGORY" in df["label"].values:
            raise Exception("Error: label must be more specific than CATEGORY")

        if not all(label in labels for label in df["label"].unique()):
            raise Exception("Error: label is not valid.")

        idlabel = 'CATEGORY'
        if isDataset:
            required = ["CMName","label","DatasetCitation","shortName"]
            idlabel = 'DATASET'
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

        updateLog(f"log/{user}uploadProgress.txt", "getting new ID", write = 'a')
        newID = getAvailableID(new_id = "CMID", label=idlabel, n = len(df), database = database)


        df["CMID"] = newID

        df = df.astype(str)

        vars = [col for col in df.columns if 'label' not in col and 'uniqueID' not in col]

        properties = getQuery("MATCH (p:PROPERTY) return p.property as property", driver, type = "list")

        missing_vars = [var for var in vars if var not in properties]

        if "importID" in missing_vars:
            missing_vars.remove("importID")

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
                    
        return results_df
    
    except Exception as e:
        updateLog(f"log/{user}uploadProgress.txt", str(e), write = 'a')
        raise

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
        links[existing_columns] = links[existing_columns].applymap(lambda x: re.sub(r'[\t\n\r\f\v]', '', x).strip() if isinstance(x, str) else x)

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
            
            keys.append(f"r.{var} = custom.combinedProperties('',row.{var},'{metaType}')[0].prop")
                
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
        updateLog(f"log/{user}uploadProgress.txt", "Uploading new USES ties", write = 'a')
        links_dict = links.to_dict(orient='records')
        result = getQuery(q, driver, params={'rows':links_dict})

        # Update alternate names
        CMIDs = [item['CMID'] for item in result]
        updateAltNames(driver,CMIDs)

        # Get the number of relationships after adding
        nRels2 = getQuery("MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list")
        new_rels = nRels2[0] - nRels[0]
        updateLog(f"log/{user}uploadProgress.txt", f"Number of new relationships in database: {new_rels}", write = 'a')

        end_time = time.time()
        updateLog(f"log/{user}uploadProgress.txt", f"Elapsed time: {int(end_time - start_time)} seconds", write = 'a')

        return {"q": result, "links": links_dict}

    except Exception as e:
        if isinstance(e, tuple):
            error_message = ', '.join(map(str, e))
        else:
            error_message = str(e)
            updateLog(f"log/{user}uploadProgress.txt", error_message, write = 'a')
        raise

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
    df['Name'] = df.apply(
        lambda row: "; ".join(
            filter(pd.notna, [row[name_col]] + ([row[alt_name_col]] if pd.notna(row[alt_name_col]) else []))
        ), axis=1
    )
    return df

def to_geojson_point(coordinates):

    if len(coordinates) == 1:
        coordinates = coordinates[0]
        latitude = coordinates[0]
        longitude = coordinates[1]
        if math.isnan(latitude) or math.isnan(longitude):
            return None  # Or some other fallback value
        
        # round
        latitude = round(latitude, 4)
        longitude = round(longitude, 4)
        
        # Create the GeoJSON dictionary
        geojson_dict = {
            "type": "Point",
            "coordinates": [longitude,latitude]
        }
    elif len(coordinates) > 1:
        lats = []
        longs = []
        for coord in coordinates:
            latitude = coord[0]
            longitude = coord[1]
            if math.isnan(latitude) or math.isnan(longitude):
                return None 
            
            # round
            latitude = round(latitude, 4)
            longitude = round(longitude, 4)
            lats.append(latitude)
            longs.append(longitude)
        
        geojson_dict = {
            "type": "MultiPoint",
            "coordinates": [[longs[i], lats[i]] for i in range(len(lats))]
        }
    else:
        return ""
        
    # Convert the dictionary to a GeoJSON string
    return json.dumps(geojson_dict)

def convert_coordinates(geo):
    # Return "NA" if geo is None or the string "NA"
    if geo in (None, ''):
        return ''
    
    try:
        # Check if geo is a string and contains multiple coordinate entries
        if isinstance(geo, str):
            # Split the string by semicolons to handle multiple JSON objects
            geo_entries = geo.split(';')
            coordinates = []
            
            for entry in geo_entries:
                entry = entry.strip()  # Remove any leading/trailing spaces
                try:
                    # Parse each JSON string to a dictionary
                    geo_dict = json.loads(entry)
                    
                    lat = geo_dict.get('latitude')
                    lon = geo_dict.get('longitude')
                    
                    if lat is not None and lon is not None:
                        try:
                            coordinates.append((float(lat), float(lon)))
                        except ValueError:
                            continue  # Skip invalid coordinate pairs
                except json.JSONDecodeError:
                    continue  # Skip invalid JSON entries
            
            if coordinates:
                coordinates = to_geojson_point(coordinates)
                return coordinates
        
        # If geo is not a string or no valid coordinates found, return "NA"
        return ''
    
    except Exception:
        # Catch any unexpected errors and return "NA"
        return ''

def create_grouped_columns(row, grouped_columns):
    grouped_data = {}
    
    # Iterate over each unique group (e.g., 'parentContext', 'geoCoords')
    for group in grouped_columns['group'].unique():
        # Find the columns that belong to this group
        group_cols = grouped_columns[grouped_columns['group'] == group]['property']
        
        # Collect the non-null values from these columns into a dictionary
        group_data = {col: row[col] for col in group_cols if pd.notna(row[col])}
        
        # Store this as a separate column for each group
        if group_data:  # Only add if there are valid entries
            grouped_data[group] = json.dumps(group_data)  # Store as a JSON string
    
    return grouped_data

def process_parent_context_element(element):
    try:
        # If element is a list, process the list and return a semicolon-separated string
        if isinstance(element, list):
            return '; '.join(list2character(item) for item in element)
        # If element is a string, return the string itself
        elif isinstance(element, str):
            return element
        # If element is None or any other type, return None
        else:
            return None
        
    except ValueError:
        return None
    
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
                 overwriteProperties=False,
                 updateProperties=False,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=1000,
                 ):
    
    updateLog(f"log/{user}uploadProgress.txt", "Starting database upload", write = 'w')
    dataset = pd.DataFrame(dataset)

    if nodeContext is None:
        nodeContext = []

    if linkContext is None:
        linkContext = []

    if database.lower() == "sociomap":
        database = "SocioMap"
    elif database.lower() == "archamap":
        database = "ArchaMap"
    else:
        raise ValueError(f"database must be either 'SocioMap' or 'ArchaMap', but value was '{database}'")

    if "label" in linkContext:
        linkContext.remove("label")

    if 'eventDate' in dataset.columns:
        dataset['eventDate'] = pd.to_numeric(dataset['eventDate'], errors='coerce').astype('Int64')  # Use 'Int64' to support NaNs
    dataset = dataset.replace({np.nan: None, pd.NA: None})
    dataset = dataset.astype(str)
    dataset = dataset.replace({None,""})
    dataset = dataset.replace({"nan": "", "<NA>": "","None":""})
    
    driver = getDriver(database)

    if formatKey is True:
        dataset = createKey(dataset, Key).copy()
        Key = 'Key'
    
    if geocode is True:
        raise Exception("Error: geocode must be False")
    
    if 'eventType' in dataset.columns and 'eventDate' not in dataset.columns:
        dataset['eventDate'] = np.nan

    updateLog(f"log/{user}uploadProgress.txt", "checking whether upload is for DATASET nodes", write = 'a')
    
    if label is None:
        isDataset = False
    else:
        isDataset = label == "DATASET" or dataset['label'].iloc[0] == "DATASET"
    
    if isDataset:
        updateLog(f"log/{user}uploadProgress.txt", "upload is for DATASET nodes", write = 'a')
    else:
        updateLog(f"log/{user}uploadProgress.txt", "upload is for CATEGORY nodes", write = 'a')

    dataset = dataset.dropna(axis=1, how='all')

    updateLog(f"log/{user}uploadProgress.txt", "checking column names", write = 'a')

    columns_to_select = list(set([
    CMName, Name, CMID, altNames, Key, datasetID, label, uniqueID, "shortName", "DatasetCitation"] + nodeContext + linkContext))

    dataset = dataset[[col for col in columns_to_select if col in dataset.columns]]

    if isDataset:
        column_names = [CMName, label, uniqueID] + nodeContext
    else:
        if overwriteProperties or updateProperties:
            column_names = [Name, CMID, Key, datasetID, uniqueID] + nodeContext + linkContext
        else:
            column_names = [CMName, Name, label, Key, datasetID, uniqueID] + nodeContext + linkContext
    
    # Remove None values
    column_names = [col for col in column_names if col is not None]

    errors = [f"{col} must be in dataset" for col in column_names if col not in dataset.columns]

    if len(errors) > 0:
        updateLog(f"log/{user}uploadProgress.txt", "\n".join(errors), write = 'a')
        raise ValueError("\n".join(errors))

    # Check for NA or empty strings in the required columns

    # if isDataset:
    #     required_cols = [CMName, "shortName", "DatasetCitation",label]
    # else:
    #     if updateProperties or overwriteProperties:
    #         required_cols = [datasetID, CMID, Key]
    #     else:
    #         required_cols = [datasetID, Name, Key, label]
    #         if CMID is None:
    #             required_cols.append(CMName)
    # if dataset[required_cols].isnull().any().any() or (dataset[required_cols] == '').any().any():
    #     raise ValueError("The dataset contains NA values or empty strings in the required columns.")

    properties = getPropertiesMetadata(driver)
    properties = pd.DataFrame(properties)

    if uniqueID is None or uniqueID not in dataset.columns:
        updateLog(f"log/{user}uploadProgress.txt", "Creating import ID", write = 'a')
        getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver)
        uniqueID = 'importID'
        uniqueProperty = 'importID'
        dataset['importID'] = dataset.index + 1

    if not isDataset:
        updateLog(f"log/{user}uploadProgress.txt", "Combining paired properties", write = 'a')
        paired = properties.merge(pd.DataFrame({'property': dataset.columns}), on='property')
        grouped_columns = paired[paired['group'].notna()][['property', 'group']]
        grouped_dict = dataset.apply(lambda row: create_grouped_columns(row, grouped_columns), axis=1)
        grouped_df = pd.DataFrame(grouped_dict.tolist())
        dataset = pd.concat([dataset, grouped_df], axis=1)
        columns_to_drop = grouped_columns[grouped_columns['property'] != 'parent']['property'].tolist()
        # Drop the columns from dataset, keeping the 'parent' column
        dataset = dataset.drop(columns=columns_to_drop).copy()
        for group in grouped_columns['group'].unique():
            linkContext.append(group)

    sq = range(0, len(dataset), batchSize)

    try:
        final_result = pd.DataFrame()
        dataset_match = pd.DataFrame()
        for s in sq:
            sub_dataset = dataset.iloc[s:s + batchSize].copy()
            max_row = len(sub_dataset) - 1 + s
            updateLog(f"log/{user}uploadProgress.txt", f"uploading {s} to {max_row} of {len(dataset)}", write = 'a')

            if CMID in sub_dataset.columns:
                if datasetID in sub_dataset.columns and Key in sub_dataset.columns:
                    if CMID in sub_dataset.columns:
                        sub_dataset = combine_properties(sub_dataset, [CMID, datasetID, Key])
                    else:
                        sub_dataset = combine_properties(sub_dataset, [datasetID, Key])
            
            if addDistrict:
                updateLog(f"log/{user}uploadProgress.txt", "Adding districts", write = 'a')
                matches = getQuery(params={'rows': sub_dataset[[datasetID]]}, q='DISTRICT QUERY', database=database, user='1')
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(matches, on=datasetID, how="left")
                    linkContext.append('country')

            if addRecordYear:
                updateLog(f"log/{user}uploadProgress.txt", "Adding record year", write = 'a')
                matches = getQuery(params={'rows': sub_dataset[[datasetID]]}, q='RECORD_YEAR QUERY', driver = driver)
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(matches, on=datasetID, how="left")
                    linkContext.append('recordStart')

            sub_dataset = sub_dataset.fillna('')

            node_columns = [CMName, uniqueID, label] + nodeContext
            node_columns = [col for col in node_columns if col in sub_dataset.columns]

            if isDataset:
                required_cols = list(set(["CMName", "shortName", "DatasetCitation", uniqueID, 'label'] + nodeContext))
                required_cols = [col for col in required_cols if col in sub_dataset.columns]
                nodes = sub_dataset[required_cols].drop_duplicates()
            else:
                if Name and CMID in sub_dataset.columns:
                    nodes = sub_dataset[sub_dataset[CMID] == ''][node_columns].drop_duplicates()
                elif Name in sub_dataset.columns:
                    nodes = sub_dataset[node_columns]
                    CMID = 'CMID'
                else:
                    nodes = pd.DataFrame()
                        
            if not nodes.empty:
                updateLog(f"log/{user}uploadProgress.txt", "Adding nodes", write = 'a')
                match = createNodes(nodes,database, user=user)
                match = pd.DataFrame(match)
                match = match.astype(str)
                sub_dataset = sub_dataset.astype(str)
                join_cols = list(set(sub_dataset.columns.intersection(match.columns)))
                dataset_match = pd.merge(sub_dataset, match, how = "outer",on=join_cols)
            else: 
                dataset_match = sub_dataset.copy()
            
            
            link_columns = [datasetID, CMName, CMID, Name, altNames, Key, uniqueID, label] + linkContext
            link_columns = [col for col in link_columns if col in dataset_match.columns]

            if not isDataset:
                updateLog(f"log/{user}uploadProgress.txt", "Adding USES relationships", write = 'a')

                links = dataset_match[link_columns].drop_duplicates().copy()

                links.rename(columns={datasetID: 'from', CMID: 'to'}, inplace=True)
                
                if Name and altNames is not None:
                    updateLog(f"log/{user}uploadProgress.txt", "Combining names and alternate names", write = 'a')
                    links = combine_names_and_altNames(links, Name, altNames)
                
                if linkContext is not None and 'geoCoords' in linkContext:
                    updateLog(f"log/{user}uploadProgress.txt", "updating geo coordinates", write = 'a')
                    # return links
                    links['geoCoords'] = links['geoCoords'].apply(convert_coordinates)

                if "parentContext" in linkContext:
                    updateLog(f"log/{user}uploadProgress.txt", "updating parentContext", write = 'a')
                    # return links

                    def filter_dict(d):
                        filtered_dict = ""
                        try:
                            d = json.loads(d)
                            filtered_dict = {k: v for k, v in d.items() if pd.notna(v) and v != ""}
                            # If 'parent' is the only key remaining, return an empty string
                            if list(filtered_dict.keys()) == ['parent']:
                                filtered_dict = ""
                        except json.JSONDecodeError:
                            return ""                          
                        
                        return filtered_dict

                    sub_links = links.copy()

                    # sub_links['parentContext'] = sub_links['parentContext'].apply(lambda x: json.loads(x))

                    sub_links['parentContext'] = sub_links['parentContext'].apply(filter_dict)

                    # Step 1: Convert parentContext dictionary to a JSON string
                    # Apply json.dumps to convert dictionaries to JSON strings
                    sub_links['parentContext'] = sub_links['parentContext'].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x)

                    # Step 2: Remove square brackets if present in strings
                    sub_links['parentContext'] = sub_links['parentContext'].apply(lambda x: re.sub(r'\[|\]', '', x) if isinstance(x, str) else x)

                    # Step 3: Unnest data (apply to each row)
                    sub_links = sub_links.explode('parentContext').reset_index(drop=True)

                    # Step 4: Handle missing parent values by setting parentContext to None where parent is NaN
                    sub_links['parentContext'] = sub_links.apply(lambda row: None if pd.isna(row['parent']) else row['parentContext'], axis=1)

                    # Step 5: Drop 'eventDate' and 'eventType' columns if they exist
                    sub_links = sub_links.drop(columns=[col for col in ['eventDate', 'eventType'] if col in sub_links.columns])

                    # Step 6: Group by 'from', 'to', and 'Key'
                    grouped_links = sub_links.groupby(['from', 'to', 'Key'])

                    # Step 7: Combine lists of parentContext and parent, keeping their JSON representations intact
                    sub_links = grouped_links.agg({
                        'parentContext': lambda x: list(x),
                        'parent': lambda x: list(x)
                    }).reset_index()

                    # Step 8: Convert lists of JSON strings to a semicolon-separated string
                    for index, row in sub_links.iterrows():
                        sub_links.at[index, 'parentContext'] = process_parent_context_element(row['parentContext'])
                        sub_links.at[index, 'parent'] = process_parent_context_element(row['parent'])

                    # Step 9: Merge the grouped data back into the original DataFrame
                    links = links.drop(columns=['parentContext', 'parent']).copy()
                    links = pd.merge(links, sub_links, on=['from', 'to', 'Key'], how='left')
                
                link_cols = ['from', 'to', 'Key'] + linkContext
                link_cols = [col for col in link_cols if col in links.columns]
                if Name:
                    link_cols.append(Name)
                if overwriteProperties:
                    updateLog(f"log/{user}uploadProgress.txt", "Overwriting property", write = 'a')
                    result = updateProperty(links[link_cols], database = database, user = user, updateType = "overwrite")
                elif updateProperties:
                    updateLog(f"log/{user}uploadProgress.txt", "Updating property", write = 'a')
                    result = updateProperty(links[link_cols], database = database, user = user, updateType = "update")
                else:
                    updateLog(f"log/{user}uploadProgress.txt", "Adding new USES relationships", write = 'a')
                    link_cols.append(label)
                    links = links[link_cols]
                    result = createUSES(links = links,database = database, user = user, create = "MERGE")
                updateLog(f"log/{user}uploadProgress.txt", "Completed updating USES relationships", write = 'a')

                updateLog(f"log/{user}uploadProgress.txt", "Processing returned CMIDs", write = 'a')
                try:
                    cmid_values = [link['to'] for link in result['links']]
                    updateAltNames(driver, CMID = cmid_values)
                    updateLog(f"log/{user}uploadProgress.txt", "updated alternate names", write = 'a')
                except KeyError as e:
                    updateLog(f"log/{user}uploadProgress.txt", f"Error updating alternate names: {e}", write = 'a')
                    continue

                result =  pd.DataFrame(result['links'])
                final_result = pd.concat([final_result,result], axis = 0)
            
            else:
                updateLog(f"log/{user}uploadProgress.txt", "Processing Dataset properties", write = 'a')
                cmids = dataset_match['CMID'].unique()
                processDATASETs(database = database, user = user, CMID = cmids)
                final_result = pd.concat([final_result,dataset_match], axis = 0)

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
        f.write("Completed dataset upload\n")

    if 'from' in final_result.columns and 'to' in final_result.columns:
        final_result.rename(columns={'from': 'datasetID', 'to': 'CMID'}, inplace=True)
    
    return final_result.drop_duplicates()

    
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
        
        result = getQuery(query = q, driver = driver, params = {"rows": links_dict})
        
        return {'result': result, 'links': links_dict}
    except Exception as e:
        return f"Error: {str(e)}"