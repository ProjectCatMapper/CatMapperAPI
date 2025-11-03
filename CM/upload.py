"""upload.py"""

from .utils import *
from .USES import *
from .keys import *
from .GIS import *
from .log import *
import json
import pandas as pd
from flask import jsonify
import numpy as np
import time
import re
import warnings
import math
import warnings

warnings.simplefilter("error", UserWarning)

data = [
    {
        "CMID": "test-1",
        "datasetID": "SD11",
        "Key": "test-1",
        "geoCoords": "yep",
        "yearStart": 2011,
    }
]

test_df = pd.DataFrame(data)

#Returns boolean if input is integer
def is_valid_integer(value):
    try:
        if pd.isna(value):
            value = ""
        if value == "":
            return True
        num = float(value)
        return num.is_integer()
    except (ValueError, TypeError):
        return False

#Returns boolean if input is float
def is_valid_float(value):
    try:
        if value == "":
            return True
        float(value)
        return True
    except (ValueError, TypeError):
        return False

#Returns invalid rows where End years are lesser than Start years
def get_invalid_ranges(df, col1, col2):   
    invalid_mask = (~df[col2].isna()) & (df[col2] != '') & (pd.to_numeric(df[col2], errors='coerce') < pd.to_numeric(df[col1], errors='coerce'))
    return df[invalid_mask][[col1, col2]]

#writes the log text to console and also saves logs to text file.
def updateLog(f, txt, write="a"):
    print(txt)
    try:
        with open(f, write) as file:
            file.write(txt + "\n")
    except Exception as e:
        print(e)

#Creates new nodes for functions 1 and 2
def createNodes(df, database,isDataset, user, uniqueID=None):
    try:

        driver = getDriver(database)

        df = df.copy()

        isDataset = isDataset

        #Assigns appropriate label(s) to node, adding CATEGORY if the label is a CATEGORY domain.
        idlabel = "CATEGORY"
        if isDataset:
            idlabel = "DATASET"
        else:
            df["label"] = df["label"].apply(lambda x: f"CATEGORY:{x}")

        # check for uniqueID
        if not uniqueID in df.columns or uniqueID is None:
            raise Exception("Error: there must be a uniqueID.")
        else: 
            # make sure there is a uniqueId for each row of the dataset
            if len(df[uniqueID].unique()) != len(df):
                raise Exception(
                    f"Error: {uniqueID} must be unique for each row, but found duplicates."
                )

        #Creates new nodes and assigns new_id to database
        updateLog(f"log/{user}uploadProgress.txt", "getting new ID", write="a")
        newID = getAvailableID(
            new_id="CMID", label=idlabel, n=len(df), database=database
        )

        df["CMID"] = newID

        updateLog(
            f"log/{user}uploadProgress.txt", "Converting variables to string", write="a"
        )

        df = df.astype(str)

        # Checks that all columns in df represent valid CatMapper node properties.
        # We get all columns that need to be set as properties for nodes, excludes label and uniqueID
        # This is useful when determining valid columns coming from the API, not relevant to UI.
        vars = [
            col for col in df.columns if "label" not in col and "uniqueID" not in col
        ]

        if isDataset:
            allowed_properties = getQuery(
                "MATCH (p:PROPERTY) WHERE p.nodeType CONTAINS 'DATASET' or p.nodeType='NO EDIT' return p.CMName as property", driver, type="list"
            )
        else:
            allowed_properties = getQuery(
                "MATCH (p:PROPERTY) WHERE p.nodeType CONTAINS 'CATEGORY' or p.nodeType='NO EDIT' return p.CMName as property", driver, type="list"
            )
        
        vars = [v for v in vars if v in allowed_properties] + ['importID']
        
        # missing_vars = [var for var in vars if var not in properties]

        # if "importID" in missing_vars:
        #     missing_vars.remove("importID")

        # if missing_vars:
        #     raise Exception(
        #         f"Error: The following columns are not in the allowed properties for the node type: {', '.join(missing_vars)}"
        #     )

        updateLog(
            f"log/{user}uploadProgress.txt", "Creating variable clauses", write="a"
        )
        set_clause = ", ".join([f"a.{var} = row.{var}" for var in vars])

        return_clause = ", ".join([f"a.{var} as {var}" for var in vars])

        updateLog(f"log/{user}uploadProgress.txt", "Creating query", write="a")
        q = f"""
        unwind $rows as row
        call apoc.cypher.doIt('
        MERGE (a:' + row.label + ' {{{uniqueID}: row.{uniqueID}}})
        ON CREATE SET
        {set_clause}
        return a',
        {{row: row}}) yield value
        with value.a as a
        return distinct elementId(a) as nodeID,
        {return_clause}
        """

        rows = df.to_dict(orient="records")
        updateLog(f"log/{user}uploadProgress.txt", q, write="a")

        updateLog(f"log/{user}uploadProgress.txt", "Running query", write="a")
        results = getQuery(query=q, driver=driver, params={"rows": rows})

        results_df = pd.DataFrame(results)

        updateLog(f"log/{user}uploadProgress.txt", "Updating log", write="a")

        log_entries = results_df[vars + ["nodeID"]].to_dict(orient="records")

        createLog(
            id=results_df["nodeID"].tolist(),
            type="node",
            log=[
                "created node with "
                + ", ".join([f"{k}: {str(v)}" for k, v in row.items() if k != "nodeID"])
                for row in log_entries
            ],
            user=user,
            driver=driver,
        )

        return results_df
    except Exception as e:
        updateLog(f"log/{user}uploadProgress.txt", str(e), write="a")
        raise

#Creates uses ties for nodes created in functions 1 and 2
#Currently relies on checks for duplicate rows from the main function,
#if used independently in the future, may need more precise internal error handling.
def createUSES(links, database, user):
    try:
        start_time = time.time()
        if "datasetID" not in links.columns or "CMID" not in links.columns or "Key" not in links.columns:
            raise ValueError("Must have 'datasetID','CMID' and 'Key' columns")

        links = links.copy()

        # Database connection assumed via driver
        driver = getDriver(database)

        # Checks that all columns in links represent valid CatMapper relationship properties.        
        # We get all columns that need to be set as properties for realtionships
        # This is useful when determining valid columns coming from the API, not relevant to UI.
        db_properties = getQuery(
            "MATCH (p:PROPERTY) WHERE p.type = 'relationship' RETURN p.CMName AS property",
            driver,
        )
        db_properties_list = [item["property"] for item in db_properties]
        missing_cols = [var for var in links.columns.tolist() if var not in db_properties_list and var not in ["CMID","datasetID","CMName"]]
        if missing_cols:
            raise Exception(
                f"Error: The following columns are not in properties: {', '.join(missing_cols)}"
            )

        # Convert all values to strings and replace NaN with empty strings
        links = links.fillna("").astype(str)

        #Removes required properties that either aren't added to uses tie (datasetID, CMID, CMName) or are added separately (Key)
        vars = links.columns.difference(["datasetID", "CMID", "Key", "CMName"])

        # query = """
        #     match (n:METADATA:PROPERTY)
        #     return n.property as property, n.type as type,
        #     n.relationship as relationship, n.description as description,
        #     n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
        #     """

        # metaTypes = getQuery(query, driver)
        metaTypes = getPropertiesMetadata(driver)
        metaTypeDict = {item["property"]: item["metaType"] for item in metaTypes}

        keys = []
        return_clause = []
        for var in vars:
            # Get the metaType for the given property
            metaType = metaTypeDict.get(var)

            keys.append(
                f"r.{var} = custom.formatProperties(['',row.{var}],'{metaType}',';')[0].prop"
            )

            return_clause.append(f"row.{var} as {var}")

        # Combine the cypher code (called keys) for each property into a single string for the Cypher query
        keys_string = ", ".join(keys)

        items = [k.split('=')[0].strip() for k in keys_string.split(',') if '=' in k]

        # Format each property as: r.prop AS prop - so the query works correctly - for return
        return_props = (
            items[0] + f" AS {items[0].split('.')[-1]}"
            if len(items) == 1
            else ', '.join([f"{item} AS {item.split('.')[-1]}" for item in items])
        )

        onCreate = "ON CREATE "

        # Create Cypher query for adding USES ties
        q = f"""
        UNWIND $rows AS row
        MATCH (a:DATASET) WHERE row.datasetID = a.CMID
        MATCH (b:CATEGORY) WHERE row.CMID = b.CMID
        MERGE (a)-[r:USES {{Key: row['Key']}}]->(b)
        {onCreate}SET r.status = 'update', {keys_string}
        RETURN elementId(b) AS nodeID, elementId(r) as relID, r.Key as Key, a.CMID as datasetID, b.CMID as CMID,b.CMName as CMName, {return_props}
        """

        # Get the number of USES ties before adding
        nRels = getQuery(
            "MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list"
        )

        # Execute the query and return results
        updateLog(f"log/{user}uploadProgress.txt", "Uploading new USES ties", write="a")
        links.to_csv(f"log/{user}uploadProgress.csv")
        links_dict = links.to_dict(orient="records")
        result = getQuery(q, driver, params={"rows": links_dict})

        if isinstance(result, dict):
            updateLog(f"log/{user}uploadProgress.txt", "Query successful", write="a")
        else:
            updateLog(f"log/{user}uploadProgress.txt", str(result), write="a")

        # Update alternate names
        CMIDs = [item["CMID"] for item in result]
        updateAltNames(driver, CMIDs)

        updateLog(
            f"log/{user}uploadProgress.txt", "adding logs to USES ties", write="a"
        )
        updateLog(f"log/{user}uploadProgress.txt", ", ".join(vars), write="a")
        result_df = pd.DataFrame(result)
        createLog(
            id=result_df["relID"].tolist(),
            type="relation",
            log=[
                "created relationship with "
                + ", ".join(
                    [
                        f"{k}: str({v})"
                        for k, v in row.items()
                        if not k in ["nodeID", "relID"]
                    ]
                )
                for row in result
            ],
            user=user,
            driver=driver,
        )

        updateLog(f"log/{user}uploadProgress.txt", " test 3 ", write="a")

        # Get the number of relationships after adding
        nRels2 = getQuery(
            "MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list"
        )
        new_rels = nRels2[0] - nRels[0]
        updateLog(
            f"log/{user}uploadProgress.txt",
            f"Number of new relationships in database: {new_rels}",
            write="a",
        )

        end_time = time.time()
        updateLog(
            f"log/{user}uploadProgress.txt",
            f"Elapsed time: {int(end_time - start_time)} seconds",
            write="a",
        )

        return {"result": result, "links": links_dict}

    except Exception as e:
        if isinstance(e, tuple):
            error_message = ", ".join(map(str, e))
        else:
            error_message = str(e)
            updateLog(f"log/{user}uploadProgress.txt", error_message, write="a")
        raise


# function to update or replace properties of USES ties or nodes, (functions 3 to 6)
# This function does 3 things : it creates the correct datatyper for calls from upload, it changes the database
# and it logs those changes.
# If no seperator is specified, quadruple stove pipe should lead to no parsing.
def updateProperty(df,optionalProperties,isDataset, database, user, updateType, propertyType="USES",sep = "||||"):
    try:
        # double checking for errors, if in future we call this function elsewhere outside this pipeline
        if not updateType in ["overwrite", "update"]:
            raise Exception("type must be update or overwrite.")
        
        if "importID" in df.columns:
            df = df.drop("importID")

        driver = getDriver(database)

        if propertyType == "USES":
            requiredCols = ["datasetID", "CMID", "Key"]
        elif propertyType == "NODE":
            requiredCols = ["CMID"]
        else:
            raise Exception("Invalid propertyType")

        for required in requiredCols:
            if required not in df.columns:
                raise ValueError(f"Missing required column {required}")

        # Every column excluding the required columns in the dataframe
        # This carries forwaard geoCoords and parentContext even though they are not in optionalProperties               
        vars = df.drop(columns=[col for col in requiredCols if col in df.columns]).columns.tolist()

        if "NewKey" in vars:
            for x in range(len(vars)):
                if vars[x] == "NewKey":
                    vars[x] = "Key"

        if not vars:
            raise ValueError("No columns to change were uploaded.")
                
        # End of error checking
        
        #get elementID of USES tie uniquely identified by CMID,Key and datasetID
        if propertyType == "USES":
            id_query = """UNWIND $rows AS row
                MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(b:CATEGORY {CMID: row.CMID})
                RETURN elementId(r) AS relID, row.CMID AS CMID, row.Key AS Key, row.datasetID AS datasetID
                """
            
            id_values = getQuery(
                query=id_query,
                driver=driver,
                params={"rows": df.to_dict(orient="records")}, type="df"
            )

            df = df.merge(
                    id_values,
                    on=["CMID", "Key", "datasetID"],
                    how="left"
                )
                        
        if "NewKey" in df.columns:
            df = df.rename(columns={
            'Key': 'OldKey',
            })
            df = df.rename(columns={
                'NewKey': 'Key',
            })
        
        # this code builds the cypher query
        #query = """
        #    match (n:PROPERTY)
        #   return n.property as property, n.type as type,
        #       n.relationship as relationship, n.description as description,
        #    n.display as display, n.metaType as metaType,n.translation as translation
        #    """

        # getting metatypes for properties
        #metaTypes = getQuery(query, driver)
        metaTypes = getPropertiesMetadata(driver)
        if propertyType == "USES":
            filteredItems = [item for item in metaTypes if item["type"] == "relationship"]
            node_or_tie = "r"
        elif propertyType == "NODE":
            filteredItems = [item for item in metaTypes if item["type"] == "node"]
            node_or_tie = "n"

        metaTypeDict = {item["property"]: item["metaType"] for item in filteredItems}

        props = []
                 
        for var in vars:
            # Get the metaType for the given property
            metaType = metaTypeDict.get(var)
            if updateType == "overwrite" and var != "log":
                props.append(
                    f"{node_or_tie}.{var} = custom.formatProperties(['',row.{var}],'{metaType}','{sep}')[0].prop"
                )
            else:
                props.append(
                    f"{node_or_tie}.{var} = custom.formatProperties([{node_or_tie}.{var},row.{var}],'{metaType}','{sep}')[0].prop"
                )
        

        old_props = ", ".join([f"`{var}`: COALESCE({node_or_tie}.{var}, 'None')" for var in vars])

        props = ", ".join(props)
      
        if propertyType == "USES":
            # get_old_vals_query = f"""
            # UNWIND $rows AS row
            # MATCH (a:DATASET {{CMID: row.datasetID}})-[r:USES {{Key: row.Key}}]->(b:CATEGORY {{CMID: row.CMID}})
            # RETURN elementId(r) AS relID, b.CMID AS CMID, row.Key AS Key, row.datasetID AS datasetID,
            #     {{ {old_keys} }} AS oldVals
            # """
            get_old_vals_query = f"""
            UNWIND $rows AS row
            MATCH (a:DATASET)-[r:USES]->(b:CATEGORY)
            WHERE elementId(r) = row.relID
            RETURN elementId(r) AS relID, b.CMID AS CMID, row.Key AS Key, row.datasetID AS datasetID,
                {{ {old_props} }} AS oldVals
            """
        elif propertyType == "NODE":
            get_old_vals_query = f"""
            UNWIND $rows AS row
            MATCH (n {{CMID: row.CMID}})
            RETURN elementId(n) AS nodeID, n.CMID AS CMID,{{ {old_props} }} AS oldVals
            """
        
        old_values = getQuery(
            query=get_old_vals_query,
            driver=driver,
            params={"rows": df.to_dict(orient="records")},
        )

        items = [k.split('=')[0].strip() for k in props.split(',') if '=' in k]

        # Format each property as: r.prop AS prop - for cypher query

        return_props = (
            items[0] + f" AS {items[0].split('.')[-1]}"
            if len(items) == 1
            else ', '.join([f"{item} AS {item.split('.')[-1]}" for item in items])
        )
        

        # Query branching based on uses ties or node properties
        if propertyType == "USES":
            # q = f"""
            # UNWIND $rows AS row
            # MATCH (a:DATASET {{CMID: row.datasetID}})-[r:USES {{Key: row.Key}}]->(b:CATEGORY {{CMID: row.CMID}})
            # WITH row, r, b
            # SET r.status = 'update', {props}
            # RETURN elementId(b) as nodeID,elementId(r) as relID, b.CMID as CMID, row.Key as Key, row.datasetID as datasetID, {return_props}
            # """

            if "Key" in vars:
                q = f"""
                UNWIND $rows AS row
                MATCH (a:DATASET)-[r:USES]->(b:CATEGORY)
                WHERE elementId(r) = row.relID
                WITH row, r, b
                SET r.status = 'update', {props}
                RETURN elementId(b) as nodeID,elementId(r) as relID, b.CMID as CMID, row.Key as Key, row.datasetID as datasetID
                """
            else:
                q = f"""
                UNWIND $rows AS row
                MATCH (a:DATASET)-[r:USES]->(b:CATEGORY)
                WHERE elementId(r) = row.relID
                WITH row, r, b
                SET r.status = 'update', {props}
                RETURN elementId(b) as nodeID,elementId(r) as relID, b.CMID as CMID, row.Key as Key, row.datasetID as datasetID, {return_props}
                """
        elif propertyType == "NODE":
            q = f"""
            UNWIND $rows AS row
            MATCH (n {{CMID: row.CMID}})
            SET {props}
            RETURN elementId(n) as nodeID, n.CMID as CMID
            """

        df_dict = df.to_dict(orient="records")

        result = getQuery(query=q, driver=driver, params={"rows": df_dict})

        logs = []

        # this section logs accordingly based on propertyType
        if propertyType == "USES":

            for old_row in old_values:
                old_vals = old_row["oldVals"]

                input_row = next(
                    (
                        r
                        for r in df_dict
                        if r.get("relID") == old_row.get("relID")
                        and r.get("CMID") == old_row.get("CMID")
                        and (
                            propertyType != "USES"
                            or r.get("datasetID") == old_row.get("datasetID")
                        )
                    ),
                    {},
                )

                changes = []

                if updateType == "overwrite":
                    var = vars[0]
                    old_val = old_vals.get(var, "")
                    new_val = input_row.get(var, "")
                    changes.append(f'changed "{var}" from "{old_val}" to "{new_val}"')

                elif updateType == "update":
                    for var in vars:
                        new_val = input_row.get(var, "")
                        changes.append(f'added "{var}" with value "{new_val}"')

                logs.append("; ".join(changes))

            createLog(
                id=[row["relID"] for row in result],
                type="relation",
                log=logs,
                user=user,
                driver=driver,
                isDataset = isDataset
            )

        elif propertyType == "NODE":

            node_logs= []
            for old_row in old_values:
                old_vals = old_row["oldVals"]

                input_row = next(
                    (r for r in df_dict if r.get("CMID") == old_row.get("CMID")), {}
                )

                changes = []
                
                if updateType == "update":
                    for var in vars:
                        new_val = input_row.get(var, "")
                        changes.append(f'added "{var}" with value "{new_val}"')

                elif updateType == "overwrite":
                    old_val = old_vals.get(vars[0], "")
                    new_val = input_row.get(vars[0], "")
                    changes.append(
                        f'changed "{vars[0]}" from "{old_val}" to "{new_val}"'
                    )


                node_logs.append("; ".join(changes))

            createLog(
                id=[row["nodeID"] for row in result],
                type="node",
                log=node_logs,
                user=user,
                driver=driver,
                isDataset = isDataset
            )


        if "geoCoords" in df.columns:
            updateLog(
                f"log/{user}uploadProgress.txt", "Updating geo coordinates", write="a"
            )
            CMIDs = df["CMID"].unique()
            correct_geojson(CMID=CMIDs, database=database)

        # this format is how it is recieved at the function call, dont remove or alter.
        # df_dict is for direct API call returns.
        # ask robert
        return {"result": result, "df": df_dict}
    except Exception as e:
        return f"Error: {str(e)}"

#collapses rows by the group_by_cols variable and joins properties from seperate rows by ;
def combine_properties(df, group_by_cols, string_cols, driver):
    
    # Puts values from different rows in a single list and for string-value columns it checks if there is more than one value
    # then changes the list to a ; delimited string
    def combine_column(colname, values):
        vals = [str(x).strip() for x in values if pd.notna(x)]
        unique_vals = sorted(set(vals))
        
        # strict check
        if colname in string_cols and len(unique_vals) > 1:
            raise ValueError(
                f"Column '{colname}' has multiple values in one group: {unique_vals}"
            )
        
        # join otherwise
        return "; ".join(unique_vals)

    grouped_df = df.groupby(group_by_cols, as_index=False).agg(lambda x: x.tolist())

    print(grouped_df)

    for col in grouped_df.columns:
        if col not in group_by_cols:
            grouped_df[col] = grouped_df[col].apply(
                lambda vals, c=col: combine_column(c, vals)
            )

    # def combine_column(column):
    #     if isinstance(column, list):
    #         return "; ".join(
    #             sorted(set([str(x).strip() for x in column if pd.notna(x)]))
    #         )
    #     return column

    # grouped_df = df.groupby(group_by_cols, as_index=False).agg(lambda x: x.tolist())

    # for col in grouped_df.columns:
    #     if col not in group_by_cols:
    #         grouped_df[col] = grouped_df[col].apply(combine_column)

    return grouped_df

#When functions 1-4 have altNames, this combines altNames with Name
def combine_names_and_altNames(df, name_col, alt_name_col):
    df["Name"] = df.apply(
        lambda row: "; ".join(
            filter(
                pd.notna,
                [row[name_col]]
                + ([row[alt_name_col]] if pd.notna(row[alt_name_col]) else []),
            )
        ),
        axis=1,
    )
    return df

#converts the coordinates into a Point or a  MultiPoint format
#when it encounters multiple points in the same entry, makes its a Multipoint
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
        geojson_dict = {"type": "Point", "coordinates": [longitude, latitude]}
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
            "coordinates": [[longs[i], lats[i]] for i in range(len(lats))],
        }
    else:
        return ""

    # Convert the dictionary to a GeoJSON string
    return json.dumps(geojson_dict)

# parses the coordinates and passes it to the above function
def convert_coordinates(geo):
    # Return "NA" if geo is None or the string "NA"
    if geo in (None, ""):
        return ""

    try:
        # Check if geo is a string and contains multiple coordinate entries
        if isinstance(geo, str):
            # Split the string by semicolons to handle multiple JSON objects
            geo_entries = geo.split(";")
            coordinates = []

            for entry in geo_entries:
                entry = entry.strip()  # Remove any leading/trailing spaces
                try:
                    # Parse each JSON string to a dictionary
                    geo_dict = json.loads(entry)

                    lat = geo_dict.get("latitude")
                    lon = geo_dict.get("longitude")

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
        return ""

    except Exception:
        # Catch any unexpected errors and return "NA"
        return ""

# for complex properties, groups component properties by the superLabel into a single complex property
def create_grouped_columns(row, grouped_columns):
    grouped_data = {}

    # Iterate over each unique group (e.g., 'parentContext', 'geoCoords')
    for group in grouped_columns["group"].unique():
        # Find the columns that belong to this group
        group_cols = grouped_columns[grouped_columns["group"] == group]["property"]

        # Collect the non-null values from these columns into a dictionary
        group_data = {col: row[col] for col in group_cols if pd.notna(row[col])}

        # Store this as a separate column for each group
        if group_data:  # Only add if there are valid entries
            grouped_data[group] = json.dumps(group_data)  # Store as a JSON string

    return grouped_data
    
#cleans parentContext json strings and removes parentcontext if parent is the only component
def filter_dict(d):
    filtered_dict = ""
    try:
        d = json.loads(d)

        if pd.isna(d.get("parent", None)) or d.get("parent", "") == "":
            filtered_dict = ""
        else:
            filtered_dict = {
            k: v
            for k, v in d.items()
            if pd.notna(v) and v != ""
            }

            # If 'parent' is the only key remaining, return an empty string
            if list(filtered_dict.keys()) == ["parent"]:
                filtered_dict = ""
        
    except json.JSONDecodeError:
        return ""
    
    return filtered_dict

def is_non_empty(x):
    return pd.notna(x) and str(x).strip() != ''

#Checks whether labels in parent_labels have same group_label as labels in child_labels.arguments must be lists
def validate_labels(uploadOption,driver,parent_labels, child_labels):
    all_group_labels = getQuery("MATCH (n:LABEL) RETURN DISTINCT n.groupLabel AS groupLabel", driver, type="dict")

    all_group_labels = {item['groupLabel'] for item in all_group_labels if item['groupLabel'] != "CATEGORY"}

    if uploadOption == "add_node":
        query = """MATCH (n:LABEL)
                    RETURN n.CMName AS key, n.groupLabel AS value
                    """
        
        result = getQuery(query=query,driver=driver)
        label_dict = {row["key"]: row["value"] for row in result}

        child_labels = [[label_dict.get(i[0])] for i in child_labels]
            
    for idx, (i, j) in enumerate(zip(parent_labels, child_labels)):
        if  len(i) == 0:
            continue
            
        singular_parent_grouplabel = all_group_labels.intersection(set(i))
        singular_child_grouplabel = all_group_labels.intersection(set(j))
        
        if "GENERIC" not in singular_parent_grouplabel:
            if singular_parent_grouplabel != singular_child_grouplabel:
                raise ValueError(
                    f"Mismatch at row {idx+1}: Parent node labels dont match that of the child node.\n"
                    f"Parent Labels: {i}\n"
                    f"Child Labels: {j}"
                )

def input_Nodes_Uses(
    dataset,
    database,
    uploadOption,
    formatKey=False,
    optionalProperties=None,
    user=None,
    addDistrict=False,
    addRecordYear=False,
    geocode=False,
    batchSize=1000,
):
       
    updateLog(f"log/{user}uploadProgress.txt", "Starting database upload", write="w")

    if user is None:
        raise ValueError("Error: user must be specified")
    
    if uploadOption in [
        "add_node",
        "add_uses",
        "update_add",
        "update_replace",
        "node_add",
        "node_replace",
    ]:
        updateLog(
            f"log/{user}uploadProgress.txt",
            f"upload option is {uploadOption}",
            write="a",
        )
    else:
        raise ValueError("Error: invalid upload option'")
    
    dataset = pd.DataFrame(dataset)

    #database must be either SocioMap or ArchaMap
    if database.lower() == "sociomap":
        database = "SocioMap"
    elif database.lower() == "archamap":
        database = "ArchaMap"
    else:
        raise ValueError(
            f"database must be either 'SocioMap' or 'ArchaMap', but value was '{database}'"
        )
    
    driver = getDriver(database)

    # adhoc ID used for joining and filtering output purposes.
    updateLog(f"log/{user}uploadProgress.txt", "Creating import ID", write="a")
    getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver)
    uniqueID = "importID"
    dataset["importID"] = dataset.index + 1

    #dataset_for_results is copy of original input used to merge into upload status download spreadsheet
    dataset_for_results = dataset.copy(deep=True)
    dataset_for_results = dataset_for_results.astype(str)
    if "CMID" in dataset_for_results.columns:
        dataset_for_results['CMID'] = dataset_for_results['CMID'].replace('nan', '')

    # trim whitespace
    dataset = dataset.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    dataset = dataset.dropna(how="all").reset_index(drop=True).copy()

    updateLog(f"log/{user}uploadProgress.txt", f"working on data validation", write="a")

    # Determines if the upload is for Categories or datasets
    # Input is dataset if: 1) label is DATASET (function 1), or 
    # 2) all CMIDs have dataset format (functions 5,6)
    isDataset = False
    if "label" in dataset.columns:
        if dataset["label"].iloc[0] == "DATASET":
            isDataset = True
    elif "CMID" in dataset.columns:
        if dataset["CMID"].astype(str).str.startswith(("SD", "AD")).all():
            isDataset = True

    if isDataset:
        updateLog(
            f"log/{user}uploadProgress.txt", "upload is for DATASET nodes", write="a"
        )
    else:
        updateLog(
            f"log/{user}uploadProgress.txt", "upload is for CATEGORY nodes", write="a"
        )
        
    # defining node and link properties based on METADATA types
    
    node_query = "MATCH (p:PROPERTY) WHERE p.type='node' RETURN p.CMName as property"
    
    with driver.session() as session:
                results = session.run(node_query)
                nodeProperties = [record["property"] for record in results.data() if record["property"] in optionalProperties]
    
    link_query = "MATCH (p:PROPERTY) WHERE p.type='relationship' RETURN p.CMName as property"
    
    with driver.session() as session:
                results = session.run(link_query)
                linkProperties = [record["property"] for record in results.data() if record["property"] in optionalProperties]
    
    # If we are editing Key and NewKey is in optional Columns, add that to linkProperties since the database doesnt have a NewKey
    if "NewKey" in optionalProperties:
        linkProperties.append("NewKey")

    # When uploading category nodes, if CMName is absent, use Name column to populate CMName and vice versa.
    if uploadOption == "add_node" and not isDataset:
        if "CMName" in dataset.columns and "Name" not in dataset.columns:
            dataset["Name"] = dataset["CMName"]
        
        if "Name" in dataset.columns and "CMName" not in dataset.columns:
            dataset["CMName"] = dataset["Name"]

    
    """............................"""
    """ Error checking starts here """
    """............................"""

    # When eventType or eventDate have non-empty values, there can only be one parent in that row  
    if "eventType" in dataset.columns or "eventDate" in dataset.columns:
        # Create boolean mask for rows where A or B are non-empty
        if "eventType" in dataset.columns:
            mask = dataset['eventType'].apply(is_non_empty)
        if "eventDate" in dataset.columns:
            mask = dataset['eventDate'].apply(is_non_empty)

        # Find violations: rows where C contains ';' and A or B is non-empty
        invalid_rows = dataset.loc[mask & dataset['parent'].str.contains(';', na=False)]

        if not invalid_rows.empty:
            raise ValueError(f"When eventType or eventDate have non-empty values, there can only be one parent in that row:\n{invalid_rows}")
    
    # check if any optional property columns are completely empty
    for i in optionalProperties:
        if dataset[i].replace("",pd.NA).isna().all():
            raise ValueError(f"{dataset[i]} has all empty values")
    
    # For function 5 and 6, if the CMID column has duplicates, throws an error
    if uploadOption == "node_add" or uploadOption == "node_replace":
        duplicate_CMIDs = dataset[dataset['CMID'].duplicated(keep=False)]
        duplicate_CMIDs = duplicate_CMIDs['CMID'].tolist()
        if not duplicate_CMIDs.empty:
            raise ValueError(f"Duplicate CMIDs found in CMID column: \n{duplicate_CMIDs}")
    
    # When uploading category nodes, need to make sure that CMName is added to names in case, it is not included in Name column.
    if uploadOption == "add_node" and not isDataset:
        dataset['Name'] = dataset.apply(
                        lambda row: row['Name'] if pd.isna(row['CMName']) or str(row['CMName']) in str(row['Name'])
                        else f"{row['Name']},{row['CMName']}",
                        axis=1
                    )
            
    # When uploading keys or new keys, need to make sure they follow the standard convention
        
    pattern = re.compile(r"^\s*[^:;]+?\s*:\s*[^:;]+?(?:\s*;\s*[^:;]+?\s*:\s*[^:;]+?)*\s*$")

    if (uploadOption == "add_node" and not isDataset) or uploadOption == "add_uses":
        invalid_rows = dataset.index[~dataset["Key"].apply(lambda x: isinstance(x, str) and bool(pattern.match(x)))].map(lambda x:x+1).tolist()

        if invalid_rows:
            raise ValueError(f"Invalid 'Key' format in rows:\n{invalid_rows}. Must be of form VARIABLE : VALUE")
        
    
    if uploadOption == "update_replace":
        if "NewKey" in dataset.columns:
            invalid_rows = dataset.index[~dataset["NewKey"].apply(lambda x: isinstance(x, str) and bool(pattern.match(x)))].tolist()

            if invalid_rows:
                raise ValueError(f"Invalid 'NewKey' format in rows:\n{invalid_rows}. Must be of form VARIABLE : VALUE")

    # When uploading uses ties, if we need to create new nodes and CMName is not present, then create CMName from Name
    if uploadOption == "add_uses":
        if dataset['CMID'].isna().any():
            if "CMName" not in dataset.columns:
                dataset['CMName'] = dataset['Name']
            else:
                dataset['CMName'] = dataset['CMName'].fillna(dataset['Name'])
    
    # When adding a new node, CMName is required    
    if uploadOption == "add_node" or uploadOption == "add_uses":
        mask = pd.Series(False, index=dataset.index)

        if uploadOption == "add_uses" and dataset['CMID'].isna().any() :
            mask = dataset['CMID'].isna() & dataset['CMName'].isna()
        elif uploadOption == "add_node":
            mask = dataset['CMName'].isna()

        if mask.any():
            invalid_rows = dataset[mask]
            raise ValueError(f"When adding new nodes, new node must have non-empty Name or CMName. Check : {invalid_rows}")

    """checks if all required columns are present"""

    updateLog(f"log/{user}uploadProgress.txt", "checking column names", write="a")

    column_names = []
    required = []
    if isDataset:
        if uploadOption == "add_node":
            required = ["CMName", "label", "shortName", "DatasetCitation"]
        elif uploadOption == "node_add" or uploadOption == "node_replace":
            required = ["CMID"]
        else:
            raise ValueError("Invalid upload option")
    else:
        if uploadOption == "add_node":
            required = ["CMName", "Name", "label", "Key", "datasetID"]
        elif uploadOption == "add_uses":
            required = ["Name", "CMID", "Key", "datasetID", "label"]
        elif uploadOption == "update_add" or uploadOption == "update_replace":
            required = ["CMID", "Key", "datasetID"]
        elif uploadOption == "node_replace":
            required = ["CMID"]
        else:
            raise ValueError("Invalid upload option")
    column_names = required + nodeProperties + linkProperties

    # Remove None values
    column_names = [col for col in column_names if col is not None]

    errors = [
        f"{col} must be in dataset"
        for col in column_names
        if col not in dataset.columns
    ]

    if len(errors) > 0:
        updateLog(f"log/{user}uploadProgress.txt", "\n".join(errors), write="a")
        raise ValueError("\n".join(errors))
        
    #checking label validity
    if "label" in column_names:

        labels = getQuery(
            "MATCH (l:LABEL) return l.CMName as label", driver, type="list"
        )

        invalid_labels = [label for label in dataset["label"].unique() if label not in labels]
        if invalid_labels:
            raise Exception(f"Error: label is not valid. Maybe check the spelling. Make sure you are in the right platform. Invalid labels: {invalid_labels}")

                
        if "CATEGORY" in dataset["label"].values:
            raise Exception("Error: label must be more specific than CATEGORY")
                

    """Numeric checks"""

    # checks if the following column values are integer
    #Dan. Should simply have this info in the metadata nodes and do a query to get them?
    columns_to_check = [
        "sampleSize",
        "yearStart",
        "yearEnd",
        "recordStart",
        "recordEnd",
        "eventDate",
        "yearPublished",
    ]

    columns_to_check = [col for col in columns_to_check if col in linkProperties]

    invalid_values = {}
    for col in columns_to_check:
        dataset[col] = dataset[col].fillna("")
        invalid_rows = dataset[~dataset[col].apply(is_valid_integer)]
        if not invalid_rows.empty:
            invalid_values[col] = invalid_rows[[col]].values.flatten()
    
    if invalid_values:
        error_msg = "Invalid integer values found:\n"
        for col, values in invalid_values.items():
            error_msg += f" - Column '{col}': {values}\n"
        raise ValueError(error_msg)
            
    # Convert numeric columns to integers if they are valid
    for col in columns_to_check:
        if col in dataset.columns:
            dataset[col] = dataset[col].apply(
                lambda x: x if pd.isna(x) or x == '' else str(int(float(x))))
    
    # checks if following column values are float values
    #Dan. Should simply have this info in the metadata nodes and do a query to get them?
    columns_to_check = ["populationEstimate", "latitude", "longitude"]

    columns_to_check = [col for col in columns_to_check if col in linkProperties]

    invalid_values = {}
    for col in columns_to_check:
        invalid_rows = dataset[~dataset[col].apply(is_valid_float)]
        if not invalid_rows.empty:
            invalid_values[col] = invalid_rows[[col]].values.flatten()
    
    if invalid_values:
        error_msg = "Invalid float values found:\n"
        for col, values in invalid_values.items():
            error_msg += f" - Column '{col}': {values}\n"
        raise ValueError(error_msg)
    
    # checks for year validities
    if "recordEnd" in dataset.columns:
        invalid_rows = get_invalid_ranges(dataset, "recordStart", "recordEnd")
        if not invalid_rows.empty:
            raise ValueError(f"Found {len(invalid_rows)} invalid rows where 'recordEnd' < 'recordStart'")
    
    if "yearEnd" in dataset.columns:
        invalid_rows = get_invalid_ranges(dataset, "yearStart", "yearEnd")
        if not invalid_rows.empty:
            raise ValueError(f"Found {len(invalid_rows)} invalid rows where 'yearEnd' < 'yearStart'")
                
    #Confirms that all latitude and longitudes are in range
    if {"latitude", "longitude"}.issubset(dataset.columns):
        for index, row in dataset.iterrows():
            try:
                lat = float(row["latitude"])
                if lat < -90 or lat > 90:
                    raise ValueError(f"Latitude at row {index} is illogical (value: {lat}).")
            except (ValueError, TypeError):
                raise ValueError(f"Latitude at row {index} is not a valid number (value: {row['latitude']}).")

            try:
                lon = float(row["longitude"])
                if lon < -180 or lon > 180:
                    raise ValueError(f"Longitude at row {index} is illogical (value: {lon}).")
            except (ValueError, TypeError):
                raise ValueError(f"Longitude at row {index} is not a valid number (value: {row['longitude']}).")
            
    """ Replaces nan/NA values with None and then replaces all none with "" """
    """ Also converts everything to string """
    dataset = dataset.replace({np.nan: None, pd.NA: None})
    dataset = dataset.astype(str)
    dataset = dataset.replace({"nan": "", "<NA>": "", "None": ""})

    """ CMID checks """
    #data_dict is created as a “records” data dictionary from dataset for the purpose of error checking.
    # also used for functions 5 and 6
    data_dict = dataset.to_dict(orient="records")

    # checks for all CMIDS to be either category or dataset
    if "CMID" in dataset.columns:
        cmids = dataset["CMID"].astype(str)[(dataset["CMID"] != '')]
        pattern = r'^(SM|AM|AD|SD)\d+$'

        for i in cmids:
            if not bool(re.match(pattern, i)):
                raise ValueError("There is a malformed CMID in the CMID column.")

        if (
            not cmids.str.startswith(("SD", "AD")).all()
            and not cmids.str.startswith(("SM", "AM")).all()
        ):
            raise ValueError("Category or Dataset CMIDs cant be mixed in the CMID column.")

    """Checks for existence of CMID values in the database."""

    #Dan. Should simply have this info in the metadata nodes and do a query to get them?
    multi_value_columns = [
        "language",
        "district",
        "District",
        "country",
        "religion",
        "parent",
        "period",
        "culture",
        "polity",
    ]
    
    error_columns = ["CMID", "datasetID"] + multi_value_columns

    for i in error_columns:
        if i in dataset.columns:
            updateLog(
                f"log/{user}uploadProgress.txt", f"validating column {i}", write="a"
            )

            search_label = "CATEGORY"

            if isDataset and (i == "CMID" or  i == "parent"):
                search_label = "DATASET"
            
            if i == "datasetID":
                search_label = "DATASET"
            
            print(search_label)

            query = f"""
            UNWIND $rows AS row
            OPTIONAL MATCH (n:{search_label} {{CMID: row.value}})
            RETURN row.value AS value, COUNT(n) AS count
            """
            rows_to_check = []
            seen_values = set()
            for row in data_dict:
                if row.get(i):
                    if i in multi_value_columns:
                        values = [
                            val.strip() for val in row[i].split(";") if val.strip()
                        ]
                    else:
                        values = [str(row[i])]
            
                    for v in values:
                        if v not in seen_values:
                            seen_values.add(v)
                            rows_to_check.append({"value": v})

            if not rows_to_check:
                continue

            print(rows_to_check)

            with driver.session() as session:
                results = session.run(query, rows=rows_to_check)
                missing_values = [r["value"] for r in results.data() if r["count"] == 0]
            
            print(missing_values)

            if missing_values:
                if i == "datasetID":
                    raise ValueError(
                        f"Please confirm the following datasetID(s) are correct and try again: {', '.join(missing_values)}."
                    )
                else:
                    no_prefix = [
                        v
                        for v in missing_values
                        if not any(v.startswith(p) for p in ["AM", "AD", "SM", "SD"])
                    ]
                    with_prefix = [v for v in missing_values if v not in no_prefix]

                    message_parts = []
                    if no_prefix:
                        message_parts.append(
                            f"Please use valid CatMapper IDs for: {', '.join(no_prefix)}."
                        )
                    if with_prefix:
                        message_parts.append(
                            f"Please check CatMapper IDs for: {', '.join(with_prefix)} and try again."
                        )

                    raise ValueError(" ".join(message_parts))
                               
    #adding Grouplabels into the dataset
    if "label" in dataset.columns:
        updateLog(
            f"log/{user}uploadProgress.txt",
            f"adding grouplabel to dataset",
            write="a",
        )

        distinct_labels = dataset['label'].dropna().unique().tolist()

        query = """
        UNWIND $labels AS labelValue
        MATCH (n:LABEL)
        WHERE n.CMName = labelValue
        RETURN labelValue, n.groupLabel AS groupLabel
        """

        with driver.session() as session:
            result = session.run(query, labels=distinct_labels)
        
            label_to_grouplabel = {record['labelValue']: record['groupLabel'] for record in result}

            dataset['groupLabel'] = dataset['label'].map(label_to_grouplabel)
                        
    # checking if Grouplabel of CMID in spreadsheet matches Grouplabel in database
    # 1) Grouplabel for CMID in CMID column matches Grouplabel
    if uploadOption == "add_uses":
        if "label" in dataset.columns and "CMID" in dataset.columns:
            updateLog(
                f"log/{user}uploadProgress.txt",
                f"checking if label column matches CMID column",
                write="a",
            )
            if uploadOption == "add_node" and "parent" in dataset.columns:
                combine = dict(zip(dataset["parent"], dataset["groupLabel"]))
            else:
                combine = dict(zip(dataset["CMID"], dataset["groupLabel"]))
                    
            query = """
            UNWIND keys($rows) AS cmid
            MATCH (n:CATEGORY {CMID: cmid})
            where not $rows[cmid] in labels(n)
            RETURN n.CMID AS CMID
            LIMIT 1
            """
            with driver.session() as session:
                term_mismatch = session.run(query, rows=combine)
                mismatch = term_mismatch.single()

            if mismatch:
                raise ValueError(
                    f"Label provided in file doesnt match the labels in the database for CMID: {mismatch['CMID']}"
                )

    # 2) Grouplabel for CMID in property matches property
    # 3) Grouplabel for CMID for parent matches Grouplabel of child. 
    for i in multi_value_columns:
        if i in dataset.columns:
            rows_to_check = []
            for row in data_dict:
                if row.get(i):
                    values = [val.strip() for val in row[i].split(";") if val.strip()]
                    rows_to_check.extend([{"value": v} for v in values])
            
            if not rows_to_check:
                continue

            # checks for validity of non-parent labels
            if i != "parent":

                if i == "country":
                    check_label = "DISTRICT"
                elif i == "language":
                    check_label = "LANGUOID"
                else:
                    check_label = i.upper()

                query = """UNWIND $rows AS row
                        MATCH (n:CATEGORY {CMID: row.value})
                        WHERE NOT $label IN labels(n)
                        RETURN row.value AS value
                        """

                with driver.session() as session:
                    results = session.run(query, rows=rows_to_check, label=check_label)
                    wrong_labels = [r["value"] for r in results.data()]

                if wrong_labels:
                    raise ValueError(
                        f"Error: Wrong labels in database for column '{i}': {wrong_labels}"
                    )
            elif i == "parent":
                if uploadOption == "add_node":
                    child_column = "label"
                else:
                    child_column = "CMID"

                combined = []
                
                for _, row in dataset.iterrows():
                    child_value = row[child_column]
                    parent_values = str(row['parent']).split(';')
                    
                    for i in parent_values:
                        i = i.strip()
                        # if i:  # skip empty strings
                        #     combined.append((child_value, i))
                        combined.append((child_value, i))
                
                dict_with_index = {i: {child_column: a, 'parent': b} for i, (a, b) in enumerate(combined)}

                query = """
                        MATCH (n)
                        WHERE n.CMID IN $rows
                        RETURN n.CMID as CMID, labels(n) AS labels
                        """

                parent_values = list({row['parent'] for row in dict_with_index.values()})
                child_values = list({row[child_column] for row in dict_with_index.values()})

                with driver.session() as session:
                    result = session.run(query, rows=parent_values)
                    parent_dict = {record["CMID"]: record["labels"] for record in result}

                    if uploadOption != "add_node":
                        result= session.run(query,rows=child_values)
                        child_dict = {record["CMID"]: record["labels"] for record in result}

                for idx, row in dict_with_index.items():
                        row['parent_label'] = parent_dict.get(row['parent'], [])

                if uploadOption == "add_node":
                    for idx, row in dict_with_index.items():
                        row['child_label'] = [row['label']]
                else:
                    for idx, row in dict_with_index.items():
                        row['child_label'] = child_dict.get(row['CMID'], [])
                    
                parent_labels = []
                child_labels = []
                
                for row in dict_with_index.values():
                    parent_labels.append(row["parent_label"])
                    child_labels.append(row['child_label'])
                                       
                validate_labels(uploadOption,driver,parent_labels, child_labels)
    
    # checks if the eventType value is valid

    if "eventType" in dataset.columns:

        valid_event_types = {
            "SPLIT",
            "MERGED",
            "SPLITMERGE",
            "HIERARCHY",
            "FOLLOWS",
            "",
        }

        invalid_event_types = dataset.loc[
            ~dataset["eventType"].isin(valid_event_types), ["eventType"]
        ]

        invalid_event_entries = []
        for idx, row in invalid_event_types.iterrows():
            invalid_event_entries.append(("eventType", idx, row["eventType"]))

        if invalid_event_entries:
            error_message = "Invalid 'eventType' values found:\n" + "\n".join(
                [
                    f"Row {idx}, Column '{col}': {val}"
                    for col, idx, val in invalid_event_entries
                ]
            )
            raise ValueError(error_message)

    if "eventType" in dataset.columns and "eventDate" not in dataset.columns:
        dataset["eventDate"] = np.nan
    
    # checks for the existence of CMID, key and datasetID triplets in the database for function 3 and 4
    if uploadOption == "update_add" or uploadOption == "update_replace":
        error_query = """
    UNWIND $rows AS row
    OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(b:CATEGORY {CMID: row.CMID})
    RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.Key AS Key, COUNT(r) AS rel_count
    """

        with driver.session() as session:
            results = session.run(error_query, rows=data_dict)
            missing = [
                (r["CMID"], r["datasetID"], r["Key"])
                for r in results.data()
                if r["rel_count"] == 0
            ]

            if missing:
                raise ValueError(
                    f"Error: Invalid CMID or Key or datasetID for {missing}"
                )
            
    if uploadOption == "add_node" or "label" in dataset.columns:
        pass
    elif (
        uploadOption == "add_uses"
        or uploadOption == "update_add"
        or uploadOption == "update_replace"
    ):
        dataset["label"] = "CATEGORY"
    elif uploadOption == "node_add" or uploadOption == "node_replace":
        dataset["label"] = "DATASET"

    if formatKey is True:
        dataset = createKey(dataset, "Key").copy()

    if geocode is True:
        raise Exception("Error: geocode must be False")

    updateLog(
        f"log/{user}uploadProgress.txt",
        "checking whether upload is for DATASET nodes",
        write="a",
    )

    # prevents adding shortName if the node has the property
    if isDataset and uploadOption == "add_node":
        query = "unwind $rows as row match (d:DATASET {shortName: row.shortName}) return d.shortName as shortName"
        shortNames = getQuery(
            query,
            driver,
            params={"rows": dataset[["shortName"]].to_dict(orient="records")},
            type="list",
        )
        if len(shortNames) > 0:
            raise ValueError(
                "Error: shortName already exists for: " + ", ".join(shortNames)
            )
    
    #Check if (datasetID, CMID, Key) triplet already exists when creating a new Key in one of two ways:
    # 1) Creating new uses tie for existing node (function 2)
    # 2) Replacing Key in function 4
    if uploadOption == "add_uses" or (uploadOption == "update_replace" and optionalProperties[0] == "NewKey"):
        # only check rows with non-empty CMID value
        if uploadOption == "add_uses":
            CMID_df = dataset[dataset["CMID"].notna() & (dataset["CMID"] != "")]
            CMID_df = CMID_df.reset_index(drop=True)
            CMID_dict = CMID_df.to_dict(orient="records")
        
        # We only check for triplet in function 4, because triplet consisting of new key should not exist already
        if uploadOption == "update_replace":
            CMID_df = dataset[["CMID","NewKey","datasetID"]]
            CMID_df.rename(columns={"NewKey": "Key"}, inplace=True)
            CMID_dict = CMID_df.to_dict(orient="records")

        query = """UNWIND $rows AS row
                OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(b:CATEGORY {CMID: row.CMID})
                RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.Key AS Key, COUNT(r) AS rel_count"""
        
        with driver.session() as session:
            results = session.run(query, rows=CMID_dict)
            keyExists = [
                (r["CMID"], r["datasetID"], r["Key"])
                for r in results.data()
                if r["rel_count"] >= 1
            ]

            if keyExists:
                raise ValueError(
                    f"Error:CMID, Key and datasetID triplet already exists for {keyExists}"
                )
    

    query = """MATCH (n:PROPERTY) WHERE n.type="relationship" and n.metaType="string" RETURN n.CMName as n"""

    string_cols = getQuery(
            query,
            driver,
            type="list",
        )
        
    # For function 2, if two rows have the same uses tie(CMID,Key,datasetID) triplet and they both contain different values for
    # string type variable, throw an error
    if uploadOption == "add_uses":

        group_cols = ["CMID", "Key", "datasetID"]

        present_String_cols = [col for col in string_cols if col in optionalProperties]

        violations = (dataset[dataset["CMID"].notna() & (dataset["CMID"] != "")].groupby(group_cols)[present_String_cols].nunique(dropna=False).reset_index())

        invalid_rows = violations[(violations[present_String_cols] > 1).any(axis=1)]

        if not invalid_rows.empty:
            raise ValueError(f"Rows with same CMID, Key and datasetID cannot have different values for string type variables:\n{invalid_rows[group_cols].to_dict(orient='records')}")
          
    # For function 3, if a non-null value in a string-value column already exists in the database for a given triplet of (CMID,Key and datasetID),
    # throws an error
    if uploadOption == "update_add":
        updateLog(
        f"log/{user}uploadProgress.txt",
        "checking for existing data in string columns",
        write="a",
    )
        for i in string_cols:
            # only checks for properties that are selected to be added
            if i in optionalProperties:
                query = """UNWIND $rows AS row
                    OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(b:CATEGORY {CMID: row.CMID})
                    RETURN r.[$column] AS existing_value, row"""

                result = getQuery(
                    query,
                    driver,
                    params={"rows": dataset[["CMID","Key","datasetID",i]].to_dict(orient="records"),"column":i},
                    type="dict",
                    )
                
                for j in result:
                    if j.get("existing_value") is not None:
                        raise ValueError(
                            f"Property '{i}' already exists for USES tie between "
                            f"DATASET {j['row']['datasetID']} and CATEGORY {j['row']['CMID']} with Key {j['row']['Key']}"
                        )

    updateLog(
        f"log/{user}uploadProgress.txt",
        "obtaining string-type node properties",
        write="a",
    )                
    query = """MATCH (n:PROPERTY) WHERE n.type="node" and n.metaType="string" RETURN n.CMName"""

    node_string_cols = getQuery(
            query,
            driver,
            type="list",
        )
    
    # For function 5, if a non-null value in a string-value column already exists in the database for a given CMID,
    # throws an error
    if uploadOption == "node_add":
    
        for i in node_string_cols:
            if i in dataset.columns:
                query = """UNWIND $rows AS row
                    OPTIONAL MATCH (a {CMID: row.CMID})
                    RETURN a[$column] AS existing_value, row"""
                
                result = getQuery(
                            query,
                            driver,
                            params={"rows": dataset[["CMID",i]].to_dict(orient="records"),"column":i},
                            type="list",
                        )
                
                for j in result:
                    if j["existing_value"] is not None:
                        raise ValueError(
                    f"Property '{i}' already exists for CMID {result['row']['CMID']} "
                )
          
    '''Error checking ends here'''

    '''Data pre-processing starts'''

    # removes the mentioned control characters and trailing\leading spaces from each cell in the dataframe
    updateLog(
        f"log/{user}uploadProgress.txt",
        "removing control characters and leading/trailing spaces",
        write="a",
    )
    dataset[dataset.columns] = dataset[dataset.columns].applymap(
        lambda x: (
            re.sub(r"[\t\n\r\f\v]", "", x).strip() if isinstance(x, str) else x
        )
    )

    updateLog(
        f"log/{user}uploadProgress.txt",
        "Dropping NA columns",
        write="a",
    )
    dataset = dataset.dropna(axis=1, how="all")

    properties = getPropertiesMetadata(driver)
    properties = pd.DataFrame(properties)

    # Grouping linkproperties for a common super label.
    # for complex properties, groups component properties by the superLabel into a single complex property
    if not isDataset:
        updateLog(
            f"log/{user}uploadProgress.txt", "Combining paired properties", write="a"
        )
        paired = properties.merge(
            pd.DataFrame({"property": dataset.columns}), on="property"
        )
        grouped_columns = paired[paired["group"].notna()][["property", "group"]]
        grouped_dict = dataset.apply(
            lambda row: create_grouped_columns(row, grouped_columns), axis=1
        )
        grouped_df = pd.DataFrame(grouped_dict.tolist())
        dataset = pd.concat([dataset, grouped_df], axis=1)
        columns_to_drop = grouped_columns[grouped_columns["property"] != "parent"][
            "property"
        ].tolist()
        # Drop the columns from dataset, keeping the 'parent' column
        dataset = dataset.drop(columns=columns_to_drop).copy()
        for group in grouped_columns["group"].unique():
            linkProperties.append(group)
        linkProperties = list(set(linkProperties))
               
    # if user chooses to upload district and recordyear information from dataset
    if addDistrict:
        updateLog(
            f"log/{user}uploadProgress.txt", "Adding districts", write="a"
        )
        matches = getQuery(
            params={"rows": dataset[["datasetID"]]},
            q="DISTRICT QUERY",
            database=database,
            user="1",
        )
        if not matches.empty:
            dataset = dataset.merge(matches, on="datasetID", how="left")
            linkProperties.append("country")

    if addRecordYear:
        updateLog(
            f"log/{user}uploadProgress.txt", "Adding record year", write="a"
        )
        matches = getQuery(
            params={"rows": dataset[["datasetID"]]},
            q="RECORD_YEAR QUERY",
            driver=driver,
        )
        if not matches.empty:
            dataset = dataset.merge(matches, on="datasetID", how="left")
            linkProperties.append("recordStart")
    
    if "Name" in linkProperties and "altNames" in linkProperties:
        updateLog(
            f"log/{user}uploadProgress.txt",
            "Combining names and alternate names",
            write="a",
        )
        dataset = combine_names_and_altNames(dataset, "Name", "altNames")

    #cleans parentContext json strings by removing parentContext if no parent or no eventData or eventType
    if "parentContext" in dataset.columns: 
        dataset["parentContext"] = dataset["parentContext"].apply(filter_dict)

        # Dataframe store data as objects by default, hence we need to convert back
        # to a JSON string for processing.
        # Step 1: Apply json.dumps to convert dictionaries to JSON strings
        dataset["parentContext"] = dataset["parentContext"].apply(
            lambda x: (
                json.dumps(x, ensure_ascii=False)
                if isinstance(x, dict)
                else x
            )
        )

        # Step 2: Remove square brackets if present in strings
        # Sometimes jsons can be represented as lists, and this converts json lists to strings
        dataset["parentContext"] = dataset["parentContext"].apply(
            lambda x: (
                re.sub(r"\[|\]", "", x) if isinstance(x, str) else x
            )
        )

        # Step 3: Drop 'eventDate' and 'eventType' columns if they exist
        dataset = dataset.drop(
            columns=[
                col
                for col in ["eventDate", "eventType"]
                if col in dataset.columns
            ]
        )
    
    print(dataset)

    #For function 1, each row creates a new node
    #For function 2, each row without a CMID creates a new node.  All rows with a CMID are grouped by the CMID, datasetID, key triplet
    #For function 3 & 4, all rows are grouped by the CMID, datasetID, key triplet.
    #For function 5 & 6, any rows with the same CMID will operate on the same CMID

    if "CMID" in dataset.columns:     
        if (dataset['CMID'] == "").any():
            mask = dataset['CMID'].isna() | (dataset['CMID'].astype(str).str.strip() == '')

            # Step 2: Generate increasing numbers starting from 1
            fill_values = range(1, mask.sum() + 1)

            # Step 3: Assign those numbers to the empty cells
            dataset.loc[mask, 'CMID'] = [f"auto_{i}" for i in fill_values]
    
        if uploadOption == "update_add" or uploadOption == "update_replace" or uploadOption == "add_uses":
            dataset = combine_properties(dataset, ["CMID", "datasetID", "Key"], string_cols, driver)
        
        dataset['CMID'] = dataset['CMID'].replace(to_replace=r'^auto_\d+$', value='', regex=True)
            
    #convert geoCoords to the Point and Multipoint formats
    if linkProperties is not None and "geoCoords" in linkProperties:
        updateLog(
            f"log/{user}uploadProgress.txt",
            "updating geo coordinates",
            write="a",
        )
        dataset["geoCoords"] = dataset["geoCoords"].apply(convert_coordinates)

    """End of error checking and data pre-processing. Begin batch upload."""

    '''Start batch processing for functions 1 to 6'''

    sq = range(0, len(dataset), batchSize)

    try:
        final_result = pd.DataFrame()
        dataset_match = pd.DataFrame()

        for s in sq:
            sub_dataset = dataset.iloc[s : s + batchSize].copy()
            max_row = len(sub_dataset) - 1 + s
            updateLog(
                f"log/{user}uploadProgress.txt",
                f"uploading {s} to {max_row} of {len(dataset)}",
                write="a",
            )
            sub_dataset = sub_dataset.fillna("")

            '''Begin node creation'''

            #You cant use non-node properties to create Nodes
            #This restricts columns to node properties (and importID).
            if isDataset:
                node_columns = ["CMName", "label", "shortName", "DatasetCitation","importID"] + nodeProperties
            else:
                node_columns = ["CMName","label","importID"] + nodeProperties

            #nodes will contain rows for nodes that need to be created.            
            nodes = pd.DataFrame()

            if uploadOption == "add_node":
                nodes = sub_dataset[node_columns]
                
            if uploadOption == "add_uses":
                #this condition is used to check if there are empty CMID rows for function 2
                if "CMID" in sub_dataset.columns and not sub_dataset["CMID"].astype(str).str.strip().ne("").all():
                    nodes = sub_dataset[sub_dataset["CMID"] == ""][node_columns].drop_duplicates()
            
            if not nodes.empty:
                updateLog(
                    f"log/{user}uploadProgress.txt",
                    "Adding nodes with columns: " + ", ".join(nodes.columns),
                    write="a",
                )

                newly_created_nodes = createNodes(nodes, database, isDataset, user=user, uniqueID="importID")
                newly_created_nodes = pd.DataFrame(newly_created_nodes)
                newly_created_nodes = newly_created_nodes.astype(str)
                sub_dataset = sub_dataset.astype(str)
                print(newly_created_nodes)

                #Merge CMIDs for new nodes back into sub_dataset. New merged dataset is called dataset_match
                #If function 2, then also add new CMIDs into dataset_for_results
                #Dan. Why not just add new CMIDS into dataset_for_results for function 1 as well?
                dataset_match = sub_dataset.merge(
                            newly_created_nodes[["importID", "CMID", "nodeID"]],
                            on="importID",
                            how="left",
                            suffixes=('', '_new')
                        )
                
                if isDataset and uploadOption == "add_node":
                    dataset_for_results = dataset_for_results.merge(
                            newly_created_nodes[["importID", "CMID"]],
                            on="importID",
                            how="left",
                            suffixes=('', '_new')
                        )
                
                if uploadOption == "add_uses":
                    dataset_match["CMID"] = dataset_match["CMID"].where(
                                    dataset_match["CMID"].astype(str).str.strip() != '',
                                    dataset_match["CMID_new"]
                                )
                    dataset_match = dataset_match.drop(columns=["CMID_new"])

                    dataset_for_results = dataset_for_results.merge(
                            newly_created_nodes[["importID", "CMID"]],
                            on="importID",
                            how="left",
                            suffixes=('', '_new')
                        )
                    
                    dataset_for_results["CMID"] = dataset_for_results["CMID"].where(
                                    dataset_for_results["CMID"].astype(str).str.strip() != '',
                                    dataset_for_results["CMID_new"]
                                )
                    
                    dataset_for_results = dataset_for_results.drop(columns=["CMID_new"])
            else:
                dataset_match = sub_dataset.copy()

            '''Ending Node creation process.'''

            #Begin USES tie creation process.
            link_columns = [
                "datasetID",
                "CMName",
                "CMID",
                "Name",
                "altNames",
                "Key",
                "label",
            ] + linkProperties
            link_columns = [col for col in link_columns if col in dataset_match.columns]
            link_columns = list(dict.fromkeys(link_columns))

            links = dataset_match[link_columns].copy()

            #For functions 1 to 4 Categories, creates USES ties or updates USES ties
            if not isDataset and uploadOption in ["add_node","add_uses","update_add","update_replace"]:
                #Now that CMID has been created for add_node, need include CMID as required column.
                if uploadOption == "add_node":
                    required_for_operation = required + ["CMID"]
                else:
                    required_for_operation = required

                link_cols = required_for_operation + linkProperties
                link_cols = list(set(link_cols))
                link_cols = [col for col in link_cols if col in links.columns]

                if uploadOption == "update_replace":
                    updateLog(
                        f"log/{user}uploadProgress.txt",
                        "Overwriting USES property",
                        write="a",
                    )
                    result = updateProperty(
                        links[link_cols],
                        optionalProperties,
                        isDataset,
                        database=database,
                        user=user,
                        updateType="overwrite",
                        sep=";"
                    )
                elif uploadOption == "update_add":
                    updateLog(
                        f"log/{user}uploadProgress.txt", "Updating USES property", write="a"
                    )
                    result = updateProperty(
                        links[link_cols],
                        optionalProperties,
                        isDataset,
                        database=database,
                        user=user,
                        updateType="update",
                        sep=";"
                    )
                elif uploadOption == "add_node" or uploadOption == "add_uses":
                    updateLog(
                        f"log/{user}uploadProgress.txt",
                        "Adding new USES relationships",
                        write="a",
                    )
                    links = links[link_cols]
                    result = createUSES(
                        links=links, database=database, user=user
                    )
                if isinstance(result, str):
                    updateLog(f"log/{user}uploadProgress.txt", result, write="a")
                    raise ValueError(result)

                updateLog(
                    f"log/{user}uploadProgress.txt",
                    "Processing returned CMIDs",
                    write="a",
                )
                cmids_from_result = [link["CMID"] for link in result["result"]]
                try:
                    # Final checks that CMIDs returned from result equal inputted CMID
                    if set(cmids_from_result) != set(links["CMID"]):
                        raise KeyError(
                            f"These inputted CMIDs have not been returned in result: {set(links['CMID']) - set(cmids_from_result)}"
                        )
                except KeyError as e:
                    updateLog(
                        f"log/{user}uploadProgress.txt",
                        f"Error updating alternate names: {e}",
                        write="a",
                    )
                    continue

                updateLog(
                        f"log/{user}uploadProgress.txt",
                        "adding CMName to Name parameter and updating alternate names",
                        write="a",
                    )
                # adds CMName to the Name parameter if missing
                addCMNameRel(database, CMID=cmids_from_result)
                
                updateLog(
                    f"log/{user}uploadProgress.txt",
                    "Completed updating USES relationships",
                    write="a",
                )


            #For function 5 and 6. Categories and Datasets
            if uploadOption in ["node_replace","node_add"]:
                required_for_operation = required + ["CMID"]
                node_columns = list(set(required_for_operation + nodeProperties))
                node_columns = [
                    col for col in node_columns if col in dataset_match.columns
                ]
                nodes = dataset_match[node_columns].drop_duplicates()
                if uploadOption == "node_replace":
                    updateLog(
                        f"log/{user}uploadProgress.txt",
                        "overwriting Node properties",
                        write="a",
                    )
                    result = updateProperty(
                        nodes,
                        optionalProperties,
                        isDataset,
                        database=database,
                        user=user,
                        updateType="overwrite",
                        propertyType="NODE",
                        sep=";"
                    )
                elif uploadOption == "node_add":
                    updateLog(
                        f"log/{user}uploadProgress.txt",
                        "updating Node properties",
                        write="a",
                    )
                    result = updateProperty(
                        nodes,
                        optionalProperties,
                        isDataset,
                        database=database,
                        user=user,
                        updateType="update",
                        propertyType="NODE",
                        sep=";"
                    )

                updateLog(
                    f"log/{user}uploadProgress.txt",
                    "processing Node properties",
                    write="a",
                )
                
            if isDataset:
                cmids = dataset_match["CMID"].unique()
                for cmid in cmids:
                    processDATASETs(database=database, user=user, CMID=cmid)
            
            #Appending result of batch to previous batch results (final_result)
            updateLog(
                    f"log/{user}uploadProgress.txt", "combining results", write="a"
                )
            
            #since function 1 for datasets doesnt create results, use dataset_for_results from node creation            
            if isDataset and uploadOption == "add_node":
                final_result = pd.concat([final_result, dataset_for_results], axis=0)
            else:
                result = pd.DataFrame(result["result"])
                final_result = pd.concat([final_result, result], axis=0)

            updateLog(
                f"log/{user}uploadProgress.txt", "results combined", write="a"
            )

            if uniqueID == "importID":
                getQuery(
                    "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL",
                    driver=driver,
                )

            updateLog(f"log/{user}uploadProgress.txt", "End of batch", write="a")

            # this is also called in waitingUSES, but we call now to get these done quickly
            # if not isDataset and uploadOption not in ["node_replace","node_add"]:
            #     updateAltNames(CMID=CMID, database=database)
            #     updateLabels(CMID=CMID, database=database)

    except Exception as e:
        try:
            if isinstance(e, tuple):
                error_message = ", ".join(map(str, e))
            else:
                error_message = str(e)
            warnings.warn(error_message)
            with open(f"log/{user}uploadProgress.txt", "a") as f:
                f.write(f"Error: {error_message}\n")

            # Return None
        except Exception as internal_error:
            warnings.warn(f"Failed to process the exception: {internal_error}")
            with open(f"log/{user}uploadProgress.txt", "a") as f:
                f.write(f"Failed to process the exception: {internal_error}\n")
        return None
    
    ''' Download spreadsheet '''

    # final_result has one column per property which was changed, 
    # it should contain CMID, Key, Dataset (need to be verified)

    # drops rows which only have null entries
    final_result = final_result.dropna(axis=1, how="any")
    final_result = final_result.dropna(how="all").reset_index(drop=True).copy()

    for col in final_result.columns:
        final_result[col] = final_result[col].apply(lambda x: ", ".join(map(str, x)) if isinstance(x, list) else x)


    if uploadOption == "add_node":

        desired_order = []
        
        """with open(f"log/{user}uploadProgress.txt", 'a') as f:
            f.write("Completed dataset upload\n")"""

        return final_result,desired_order
    
    elif uploadOption == "node_replace" or uploadOption == "node_add":

        dataset_for_results = dataset_for_results.rename(columns={col: f"{col}_input" for col in dataset_for_results.columns if col not in ["importID", "CMID"]})

        final_result = final_result.rename(columns={col: f"{col}_new" for col in final_result.columns if col not in ["importID", "CMID"]})

        desired_order = ['CMID'] + [col for col in dataset_for_results.columns.tolist() + final_result.columns.tolist() if col != 'CMID']

        final_result = pd.merge(dataset_for_results, final_result, how="left", on="CMID")

        final_result = final_result.drop_duplicates(subset='importID', keep='first')
        
        """with open(f"log/{user}uploadProgress.txt", 'a') as f:
            f.write("Completed dataset upload\n")"""

        return final_result,desired_order
    
    else:
        dataset_for_results = dataset_for_results.rename(columns={col: f"{col}_input" for col in dataset_for_results.columns if col not in ["importID", "CMID","Key","datasetID","nodeID","relID"]})

        final_result = final_result.rename(columns={col: f"{col}_new" for col in final_result.columns if col not in ["importID", "CMID","Key","datasetID","nodeID","relID"]})

        desired_order = ['CMID',"Key","datasetID"] + [col for col in dataset_for_results.columns.tolist() + final_result.columns.tolist() if col not in ['CMID',"Key","datasetID"]]

        final_result = pd.merge(dataset_for_results, final_result, how="left", on=["CMID","Key","datasetID"])

        final_result = final_result.drop_duplicates(subset='importID', keep='first')
        final_result = final_result.fillna("")

        """with open(f"log/{user}uploadProgress.txt", 'a') as f:
            f.write("Completed dataset upload\n")"""

        return final_result,desired_order