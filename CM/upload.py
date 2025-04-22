''' upload.py '''

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
warnings.simplefilter('error', UserWarning)

data = [{"CMID": "test-1", "datasetID": "SD11",
         "Key": "test-1", "geoCoords": "yep", "yearStart": 2011}]
df = pd.DataFrame(data)


def is_valid_integer_float(value):
    try:
        if value == "":
            return True
        num = float(value)
        return num.is_integer()
    except (ValueError, TypeError):
        return False


def is_valid_float(value):
    try:
        if value == "":
            return True
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def is_valid_cmid(column, value, database, label):
    if not isinstance(value, str):
        return False

    if label == "":
        error_message = "Missing label value"
        raise ValueError(error_message)

    if column == "datasetID" or (label == "DATASET" and (column == "CMID" or column == "parent")):

        data_patterns = {
            "SocioMap": r"SD\d+",
            "ArchaMap": r"AD\d+"
        }

        data_pattern = data_patterns.get(database)

        if not data_pattern:
            return False

        if not re.match(data_pattern, value):
            return False
    else:
        patterns = {
            "SocioMap": r"^SM\d+$",
            "ArchaMap": r"^AM\d+$"
        }

        # future - for all selected columns do this for Link properties.
        if column == "parent" and value == "":
            return True

        pattern = patterns.get(database)

        if not pattern:
            return False

        # Split the value by semicolons and trim spaces from each part
        split_values = [part.strip() for part in value.split(';')]

        for part in split_values:
            if not re.match(pattern, part):
                return False

        # if "label" == "DATASET":
        #     if database == "sociomap" and not value.startswith("SD"):
        #         return False
        #     if database == "archamap" and not value.startswith("AD"):
        #         return False

    return True


def add_error_column(df, user):
    if 'nodeID' not in df.columns:
        updateLog(f"log/{user}uploadProgress.txt",
                  "nodeID not found in final_results", write='a')
        raise Exception("Error: nodeID not found in final_results")

    # Add the "Error" column based on the condition
    df['Error'] = df['nodeID'].apply(
        lambda x: "CMID not found" if pd.isna(x) else "")
    return df


def updateLog(f, txt, write='a'):
    print(txt)
    try:
        with open(f, write) as file:
            file.write(txt + "\n")
    except Exception as e:
        print(e)


def createNodes(df, database, user, uniqueID=None):
    try:

        driver = getDriver(database)

        labels = getQuery(
            "MATCH (l:LABEL) return l.label as label", driver, type="list")

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
            raise Exception(
                "Error: label is not valid. Maybe check the spelling")

        idlabel = 'CATEGORY'
        if isDataset:
            required = ["CMName", "label", "DatasetCitation", "shortName"]
            idlabel = 'DATASET'
        else:
            required = ["CMName", "label"]
            df['label'] = df['label'].apply(lambda x: f"CATEGORY:{x}")
        
        missing = [column for column in required if column not in df.columns]
        if missing:
            raise Exception(f"Error: missing required columns to create new node: {missing}")


        # if not all(column in df.columns for column in required):
        #     raise Exception(
        #         "Error: missing required columns to create new node.")

        # ad-hoc column for row recognition on query return
        if not uniqueID in df.columns or uniqueID is None:
            getQuery(
                "MATCH (c) where not c.uniqueID is null set c.uniqueID = NULL", driver)
            distinct_nodes = df.drop_duplicates(subset='CMName')
            if len(distinct_nodes) != len(df):
                raise Exception(
                    "Error: there must be a unique name for each new node.")
            else:
                df['uniqueID'] = df.index

        updateLog(f"log/{user}uploadProgress.txt", "getting new ID", write='a')
        newID = getAvailableID(new_id="CMID", label=idlabel,
                               n=len(df), database=database)

        df["CMID"] = newID

        updateLog(f"log/{user}uploadProgress.txt",
                  "Converting variables to string", write='a')
        df = df.astype(str)

        vars = [
            col for col in df.columns if 'label' not in col and 'uniqueID' not in col]

        properties = getQuery(
            "MATCH (p:PROPERTY) return p.property as property", driver, type="list")

        missing_vars = [var for var in vars if var not in properties]

        if "importID" in missing_vars:
            missing_vars.remove("importID")

        if missing_vars:
            raise Exception(
                f"Error: The following vars are not in properties: {', '.join(missing_vars)}")

        updateLog(f"log/{user}uploadProgress.txt",
                  "Creating variable clauses", write='a')
        set_clause = ', '.join([f"a.{var} = row.{var}" for var in vars])

        return_clause = ', '.join([f"a.{var} as {var}" for var in vars])

        updateLog(f"log/{user}uploadProgress.txt", "Creating query", write='a')
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

        rows = df.to_dict(orient='records')
        updateLog(f"log/{user}uploadProgress.txt", q, write='a')

        updateLog(f"log/{user}uploadProgress.txt", "Running query", write='a')
        results = getQuery(query=q, driver=driver, params={"rows": rows})

        if isinstance(results, dict):
            updateLog(f"log/{user}uploadProgress.txt",
                      "Query successful", write='a')
        else:
            updateLog(f"log/{user}uploadProgress.txt", str(results), write='a')

        results_df = pd.DataFrame(results)

        updateLog(f"log/{user}uploadProgress.txt",
                  "Updating log", write='a')

        log_entries = results_df[vars + ['nodeID']].to_dict(orient='records')

        createLog(
            id=results_df['nodeID'].tolist(),
            type="node",
            log=[
                "created node with " + ", ".join(
                    [f"{k}: {str(v)}" for k, v in row.items() if k != "nodeID"]
                )
                for row in log_entries
            ],
            user=user,
            driver=driver
        )

        return results_df
    except Exception as e:
        updateLog(f"log/{user}uploadProgress.txt", str(e), write='a')
        raise


def createUSES(links, database, user, create="MERGE"):
    try:
        start_time = time.time()
        if 'datasetID' not in links.columns or 'CMID' not in links.columns:
            raise ValueError("Must have 'datasetID' and 'CMID' columns")

        if 'Key' not in links.columns:
            raise ValueError("Must have 'Key' column")

        links = links.copy()

        # Split 'datasetID' and 'CMID' on "; " and trim whitespace change to properties not datasetID and to -- maybe update combineProperties function in Neo4j to automatically split using a separator
        # links['datasetID'] = links['datasetID'].apply(lambda x: x.split('; ') if isinstance(x, str) else []).apply(lambda x: [item.strip() for item in x]).apply(lambda x: '; '.join(x))
        # links['CMID'] = links['CMID'].apply(lambda x: x.split('; ') if isinstance(x, str) else []).apply(lambda x: [item.strip() for item in x]).apply(lambda x: '; '.join(x))

        # Database connection assumed via driver
        driver = getDriver(database)

        # if 'label' not in links.columns:
        #     raise ValueError("Must have 'label' column")

        if create.lower() not in ['merge', 'create']:
            raise ValueError("create must be either 'merge' or 'create'")

        # Remove duplicates
        links = links.drop_duplicates()

        # Fetch properties from the database
        db_properties = getQuery(
            "MATCH (p:PROPERTY) WHERE p.type = 'relationship' RETURN p.property AS property", driver)
        db_properties_list = [item['property'] for item in db_properties]
        existing_columns = list(set(db_properties_list)
                                & set(links.columns.tolist()))
        updateLog(f"log/{user}uploadProgress.txt",
                  ", ".join(existing_columns), write='a')
        links = links.loc[:, ~links.columns.duplicated()].copy()
        links[existing_columns] = links[existing_columns].applymap(
            lambda x: re.sub(r'[\t\n\r\f\v]', '', x).strip() if isinstance(x, str) else x)

        # Convert all values to strings and replace NaN with empty strings
        links = links.fillna("").astype(str)

        # Select the appropriate columns based on the relationship type
        vars = links.columns.difference(['datasetID', 'CMID', 'Key', 'CMName'])

        query = """
match (n:METADATA:PROPERTY)
return n.property as property, n.type as type,
n.relationship as relationship, n.description as description,
n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
"""

        metaTypes = getQuery(query, driver)
        metaTypeDict = {item['property']: item['metaType']
                        for item in metaTypes}

        keys = []
        return_clause = []
        for var in vars:
            # Get the metaType for the given property
            metaType = metaTypeDict.get(var)

            keys.append(
                f"r.{var} = custom.formatProperties(['',row.{var}],'{metaType}',';')[0].prop")

            return_clause.append(f"row.{var} as {var}")

        # Combine the keys into a single string for the Cypher query
        keys_string = ", ".join(keys)
        return_clause_string = ", ".join(return_clause)

        onCreate = "" if create.lower() == "create" else "ON CREATE "

        # Create Cypher query for adding relationships
        q = f"""
        UNWIND $rows AS row
        MATCH (a:DATASET) WHERE row.datasetID = a.CMID
        MATCH (b:CATEGORY) WHERE row.CMID = b.CMID
        {create} (a)-[r:USES {{Key: row['Key']}}]->(b)
        {onCreate}SET r.status = 'update', {keys_string}
        RETURN elementId(b) AS nodeID, elementId(r) as relID, a.datasetID as datasetID, b.CMID as CMID, {return_clause_string}
        """

        # Get the number of relationships before adding
        nRels = getQuery(
            "MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list")

        # Execute the query and return results
        updateLog(f"log/{user}uploadProgress.txt",
                  "Uploading new USES ties", write='a')
        links.to_csv(f"log/{user}uploadProgress.csv")
        # updateLog(f"log/{user}uploadProgress.txt", ", ".join(links.columns.values), write = 'a')
        links_dict = links.to_dict(orient='records')
        # updateLog(f"log/{user}uploadProgress.txt", jsonify(links_dict), write = 'a')
        result = getQuery(q, driver, params={'rows': links_dict})

        if isinstance(result, dict):
            updateLog(f"log/{user}uploadProgress.txt",
                      "Query successful", write='a')
        else:
            updateLog(f"log/{user}uploadProgress.txt", str(result), write='a')

        # Update alternate names
        CMIDs = [item['CMID'] for item in result]
        updateAltNames(driver, CMIDs)

        updateLog(f"log/{user}uploadProgress.txt",
                  "adding logs to USES ties", write='a')
        updateLog(f"log/{user}uploadProgress.txt",
                  ", ".join(vars), write='a')
        result_df = pd.DataFrame(result)
        createLog(id=result_df['relID'].tolist(),
                  type="relation",
                  log=["created relationship with " + ", ".join(
                      [f"{k}: str({v})" for k, v in row.items() if not k in ["nodeID", "relID"]]) for row in result],
                  user=user,
                  driver=driver)

        updateLog(f"log/{user}uploadProgress.txt",
                  " test 3 ", write='a')

        # Get the number of relationships after adding
        nRels2 = getQuery(
            "MATCH ()-[r]->() RETURN count(*) AS count", driver, type="list")
        new_rels = nRels2[0] - nRels[0]
        updateLog(f"log/{user}uploadProgress.txt",
                  f"Number of new relationships in database: {new_rels}", write='a')

        end_time = time.time()
        updateLog(f"log/{user}uploadProgress.txt",
                  f"Elapsed time: {int(end_time - start_time)} seconds", write='a')

        return {"result": result, "links": links_dict}

    except Exception as e:
        if isinstance(e, tuple):
            error_message = ', '.join(map(str, e))
        else:
            error_message = str(e)
            updateLog(f"log/{user}uploadProgress.txt",
                      error_message, write='a')
        raise


def updateProperty(links, database, user, updateType, propertyType="USES"):
    try:
        # double checking for errors, if in future we call this function elsewhere outside this pipeline
        if not updateType in ['overwrite', 'update']:
            raise Exception("type must be update or overwrite.")

        driver = getDriver(database)

        if propertyType == "USES":
            requiredCols = ["datasetID", "CMID", "Key"]
        elif propertyType == "DATASET":
            requiredCols = ["CMID"]
        else:
            raise Exception("Invalid propertyType")

        for required in requiredCols:
            if required not in links.columns:
                raise ValueError(f"Missing required column {required}")

        vars = links.drop(
            columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

        # log update
        if updateType == "update":
            links['log'] = links.apply(
                lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: updated properties {', '.join([str(var) for var in vars])}", axis=1)
        else:
            links['log'] = links.apply(
                lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: overwrote properties {', '.join([str(var) for var in vars])}", axis=1)

        vars = links.drop(
            columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

        query = """
match (n:METADATA:PROPERTY)
return n.property as property, n.type as type,
n.relationship as relationship, n.description as description,
n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
"""

        # getting metatypes for properties
        metaTypes = getQuery(query, driver)
        metaTypeDict = {item['property']: item['metaType']
                        for item in metaTypes}

        keys = []
        for var in vars:
            # Get the metaType for the given property
            metaType = metaTypeDict.get(var)
            if updateType == "overwrite" and var != 'log':
                keys.append(
                    f"r.{var} = custom.formatProperties(['',row.{var}],'{metaType}',';')[0].prop")
            else:
                keys.append(
                    f"r.{var} = custom.formatProperties([r.{var},row.{var}],'{metaType}',';')[0].prop")

        keys = ", ".join(keys)

        # Query branching based on uses ties or node properties
        if propertyType == "USES":
            q = f"""
            UNWIND $rows AS row
            MATCH (a:DATASET {{CMID: row.datasetID}})-[r:USES {{Key: row.Key}}]->(b:CATEGORY {{CMID: row.CMID}})
            WITH row, r, b
            SET {keys}, r.status = "update"
            RETURN elementId(b) as nodeID, b.CMID as CMID, row.Key as Key, row.datasetID as datasetID, row.parent as parent, row.parentContext as parentContext
            """
        else:
            q = f"""
            UNWIND $rows AS row
            MATCH (r:DATASET {{CMID: row.CMID}})
            SET {keys}, r.status = "update"
            RETURN elementId(r) as nodeID, r.CMID as CMID
            """

        links_dict = links.to_dict(orient="records")

        result = getQuery(query=q, driver=driver, params={"rows": links_dict})

        if 'geoCoords' in links.columns:
            updateLog(f"log/{user}uploadProgress.txt",
                      "Updating geo coordinates", write='a')
            CMIDs = links['CMID'].unique()
            correct_geojson(CMID=CMIDs, database=database)

        return {'result': result, 'links': links_dict}
    except Exception as e:
        return f"Error: {str(e)}"


def combine_properties(df, group_by_cols):

    def combine_column(column):
        if isinstance(column, list):
            return "; ".join(sorted(set([str(x).strip() for x in column if pd.notna(x)])))
        return column

    grouped_df = df.groupby(group_by_cols, as_index=False).agg(
        lambda x: x.tolist())

    for col in grouped_df.columns:
        if col not in group_by_cols:
            grouped_df[col] = grouped_df[col].apply(combine_column)

    return grouped_df


def combine_names_and_altNames(df, name_col, alt_name_col):
    df['Name'] = df.apply(
        lambda row: "; ".join(
            filter(pd.notna, [row[name_col]] + ([row[alt_name_col]]
                   if pd.notna(row[alt_name_col]) else []))
        ), axis=1
    )
    print(df['Name'])
    return df

# todo: add lat long out of range check? RJB


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
            "coordinates": [longitude, latitude]
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
        group_cols = grouped_columns[grouped_columns['group']
                                     == group]['property']

        # Collect the non-null values from these columns into a dictionary
        group_data = {col: row[col]
                      for col in group_cols if pd.notna(row[col])}

        # Store this as a separate column for each group
        if group_data:  # Only add if there are valid entries
            grouped_data[group] = json.dumps(
                group_data)  # Store as a JSON string

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
                     uploadOption,
                     formatKey=False,
                     nodeProperties=None,
                     linkProperties=None,
                     user=None,
                     addDistrict=False,
                     addRecordYear=False,
                     geocode=False,
                     batchSize=1000,
                     ):

    updateLog(f"log/{user}uploadProgress.txt",
              "Starting database upload", write='w')

    if user is None:
        raise ValueError("Error: user must be specified")

    if uploadOption in ["add_node", "add_uses", "update_add", "update_replace", "node_add", "node_replace"]:
        updateLog(f"log/{user}uploadProgress.txt",
                  f"upload option is {uploadOption}", write='a')
    else:
        raise ValueError("Error: invalid upload option'")

    dataset = pd.DataFrame(dataset)

    dataset_dup = dataset.copy(deep=True)

    # trim whitespace
    dataset = dataset.applymap(
        lambda x: x.strip() if isinstance(x, str) else x)

    if nodeProperties is None:
        nodeProperties = []

    if linkProperties is None:
        linkProperties = []

    dataset = dataset.dropna(how='all').reset_index(drop=True).copy()

    if database.lower() == "sociomap":
        database = "SocioMap"
    elif database.lower() == "archamap":
        database = "ArchaMap"
    else:
        raise ValueError(
            f"database must be either 'SocioMap' or 'ArchaMap', but value was '{database}'")

    updateLog(f"log/{user}uploadProgress.txt",
              f"working on data validation", write='a')

    if 'eventDate' in dataset.columns:
        updateLog(f"log/{user}uploadProgress.txt",
                  f"checking eventDate", write='a')
        dataset['eventDate'] = pd.to_numeric(dataset['eventDate'], errors='coerce').astype(
            'Int64')  # Use 'Int64' to support NaNs
    dataset = dataset.replace({np.nan: None, pd.NA: None})
    dataset = dataset.astype(str)
    dataset = dataset.replace({None, ""})
    dataset = dataset.replace({"nan": "", "<NA>": "", "None": ""})

    data_dict = dataset.to_dict(orient="records")
    driver = getDriver(database)

    error_columns = ["CMID", "datasetID", "language",
                     "district", "country", "religion"]
    multi_value_columns = {"language", "district",
                           "country", "religion", "parent"}

    for i in error_columns:
        if i in dataset.columns:
            updateLog(f"log/{user}uploadProgress.txt",
                      f"validating column {i}", write='a')
            query = """
            UNWIND $rows AS row
            OPTIONAL MATCH (n {CMID: row.value})
            RETURN row.value AS value, COUNT(n) AS count
            """

            rows_to_check = []
            for row in data_dict:
                if row.get(i):
                    if i in multi_value_columns:
                        values = [val.strip()
                                  for val in row[i].split(";") if val.strip()]
                        rows_to_check.extend([{"value": v} for v in values])
                    else:
                        rows_to_check.append({"value": row[i]})

            if not rows_to_check:
                continue

            with driver.session() as session:
                results = session.run(query, rows=rows_to_check)
                missing_values = [r["value"]
                                  for r in results.data() if r["count"] == 0]

            if missing_values:
                raise ValueError(
                    f"Error: Missing values in database for column '{i}': {missing_values}")

    # checking if label column matches CMID column
    if "label" in dataset.columns and "CMID" in dataset.columns:
        updateLog(f"log/{user}uploadProgress.txt",
                  f"checking if label column matches CMID column", write='a')
        if uploadOption == "add_node" and 'parent' in dataset.columns:
            combine = dict(zip(dataset['parent'], dataset["label"]))
        else:
            combine = dict(zip(dataset['CMID'], dataset["label"]))
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
                f"Lable provided in file doesnt match for CMID: {mismatch['CMID']}")

    # checking labels for columns
    for i in multi_value_columns:
        if i in dataset.columns:
            rows_to_check = []
            for row in data_dict:
                if row.get(i):
                    values = [val.strip()
                              for val in row[i].split(";") if val.strip()]
                    rows_to_check.extend([{"value": v} for v in values])

            if not rows_to_check:
                continue

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
                    results = session.run(
                        query, rows=rows_to_check, label=check_label)
                    wrong_labels = [r["value"] for r in results.data()]

                if wrong_labels:
                    raise ValueError(
                        f"Error: Wrong labels in database for column '{i}': {wrong_labels}")
            else:
                if uploadOption == "add_node":
                    continue
                else:
                    query = """
                    UNWIND keys($rows) AS cmid
                    MATCH (n {CMID: cmid})
                    MATCH (m {CMID: $rows[cmid]})
                    RETURN labels(n) AS parent_labels, labels(m) AS child_labels
                    """

                    parent_labels = []
                    child_labels = []

                    combine = dict(zip(dataset["CMID"], rows_to_check))

                    with driver.session() as session:
                        results = session.run(query, rows=combine)
                        for record in results:
                            parent_labels.append(record["parent_labels"])
                            child_labels.append(record["child_labels"])

                    required_labels = {"LANGUOID",
                                       "RELIGION", "ETHNICITY", "DISTRICT"}

                    def validate_labels(parent_labels, child_labels):
                        for idx, (i, j) in enumerate(zip(parent_labels, child_labels)):
                            value_has_required = required_labels.intersection(
                                set(i))
                            cmid_has_required = required_labels.intersection(
                                set(j))

                            if value_has_required != cmid_has_required:
                                raise ValueError(f"Mismatch at row {idx}: Parent node labels dont match that of the child node.\n"
                                                 f"Parent Labels: {i}\n"
                                                 f"Child Labels: {j}")

                    validate_labels(parent_labels, child_labels)

    if uploadOption == "node_add":

        from functools import reduce

        updated_dfs = []  # Store updated data for each property

        with driver.session() as session:
            for prop in linkProperties:  # Loop through each property
                update_query = '''
                    UNWIND $rows AS row
                    MATCH (n {CMID: row.CMID})
                    CALL apoc.create.setProperty(n, $prop,
                        CASE
                            WHEN apoc.meta.cypher.types(n[$prop]) = "STRING" THEN
                                CASE
                                    WHEN apoc.meta.cypher.types(row[$prop]) = "STRING" THEN [n[$prop], row[$prop]]
                                    ELSE [n[$prop]] + row[$prop]
                                END
                            ELSE n[$prop] +
                                CASE
                                    WHEN apoc.meta.cypher.types(row[$prop]) = "STRING" THEN [row[$prop]]
                                    ELSE row[$prop]
                                END
                        END
                    ) YIELD node
                    RETURN node.CMID AS CMID, node[$prop] AS updated_value
                    '''

                results = session.run(update_query, rows=data_dict, prop=prop)
                updated_data = pd.DataFrame([
                    {'CMID': record['CMID'], f'updated_{prop}': record['updated_value']} for record in results
                ])

                # Store each updated DataFrame
                updated_dfs.append(updated_data)

        # Merge all updated DataFrames together on CMID
        if updated_dfs:
            merged_updates = reduce(lambda left, right: pd.merge(
                left, right, on='CMID', how='outer'), updated_dfs)
            merged_df = pd.merge(dataset, merged_updates,
                                 on='CMID', how='left')

        return merged_df

    if uploadOption == "node_replace":
        linkProperties = linkProperties[0]
        print(linkProperties)
        update_query = """UNWIND $rows AS row
                MATCH (n {CMID: row.CMID})
                CALL apoc.create.setProperty(n, $prop, row[$prop]) YIELD node
                RETURN n.CMID AS CMID, n[$prop] AS updated_value"""

        with driver.session() as session:
            results = session.run(
                update_query, rows=data_dict, prop=linkProperties)
            updated_data = pd.DataFrame(
                [{'CMID': record['CMID'], 'updated_'+linkProperties: record['updated_value']} for record in results])

            merged_df = pd.merge(dataset, updated_data, on='CMID', how='left')

        return merged_df

    if uploadOption == "update_add" or uploadOption == "update_replace":
        error_query = """
    UNWIND $rows AS row
    OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.Key}]->(b:CATEGORY {CMID: row.CMID})
    RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.Key AS Key, COUNT(r) AS rel_count
    """

        with driver.session() as session:
            results = session.run(error_query, rows=data_dict)
            missing = [(r["CMID"], r["datasetID"], r["Key"])
                       for r in results.data() if r["rel_count"] == 0]
            print(missing)

            if missing:
                raise ValueError(
                    f"Error: Invalid CMID or Key or datasetID for {missing}")

    # columns_to_check = ['parent', 'country', 'district', 'religion', 'CMID', 'datasetID', 'language']
    columns_to_check = ['parent', 'CMID']
    invalid_entries = []

    for column in columns_to_check:
        if column in dataset.columns:
            updateLog(f"log/{user}uploadProgress.txt",
                      f"checking column {column} for invalid CMID", write='a')
            # if column == "CMID" or column == "PARENT":
            #     invalid_entries= dataset.loc[
            #         ~dataset.apply(lambda row: is_valid_cmid(row[column], "sociomap", row['label']), axis=1),
            #         [column, 'label']
            #     ]
            #     for idx, row in invalid_entries.iterrows():
            #         invalid_entries.append((column, idx, row[column], row['label']))
            # elif column in ['COUNTRY', 'DISTRICT', 'RELIGION', 'LANGUAGE']:
            #     invalid_values = dataset.loc[~dataset[column].apply(is_non_empty_string), [column]]
            #     for idx, row in invalid_values.iterrows():
            #         invalid_entries.append((column, idx, row[column]))
            # elif column == "DATASETID":
            #     invalid_values = dataset.loc[~dataset['DATASETID'].apply(is_valid_integer), ['DATASETID']]
            #     for idx, row in invalid_values.iterrows():
            #         invalid_entries.append(('DATASETID', idx, row['DATASETID']))

            if uploadOption == "add_node" or 'label' in dataset.columns:
                continue
            elif uploadOption == "add_uses" or uploadOption == "update_add" or uploadOption == "update_replace":
                dataset['label'] = "CATEGORY"
            elif uploadOption == "node_add" or uploadOption == "node_replace":
                dataset['label'] = "DATASET"
            else:
                error_message = "Cannot determine upload method."
                raise ValueError(error_message)

            invalid_entries_for_column = dataset.loc[
                ~dataset.apply(lambda row: is_valid_cmid(
                    column, row[column], database, row.get('label', '')), axis=1),
                column
            ]

            for idx, row in invalid_entries_for_column.items():
                invalid_entries.append((column, idx, row))

    if invalid_entries:
        error_message = "Invalid entries found:\n" + "\n".join(
            [f"Row {idx}, Column '{col}': {val}" for col,
                idx, val in invalid_entries]
        )
        raise ValueError(error_message)

    columns_to_check = ['sampleSize', 'yearStart',
                        'yearEnd', 'recordStart', 'recordEnd']

    columns_to_check = [
        col for col in columns_to_check if col in linkProperties]

    invalid_values = {}
    for col in columns_to_check:
        invalid_rows = dataset[~dataset[col].apply(is_valid_integer_float)]
        if not invalid_rows.empty:
            invalid_values[col] = invalid_rows[[col]].values.flatten()

    if 'populationEstimate' in dataset.columns:
        invalid_population = dataset.loc[~dataset['populationEstimate'].apply(
            is_valid_float), ['populationEstimate']]
        for idx, row in invalid_population.iterrows():
            invalid_values.append(
                ('populationEstimate', idx, row['populationEstimate']))

    if formatKey is True:
        dataset = createKey(dataset, "Key").copy()

    if geocode is True:
        raise Exception("Error: geocode must be False")

    if 'eventType' in dataset.columns:

        valid_event_types = {"SPLIT", "MERGED", "SPLITMERGE", "HIERARCHY", "BECAME",""}

        invalid_event_types = dataset.loc[~dataset['eventType'].isin(valid_event_types), [
            'eventType']]

        invalid_event_entries = []
        for idx, row in invalid_event_types.iterrows():
            invalid_event_entries.append(('eventType', idx, row['eventType']))

        if invalid_event_entries:
            error_message = "Invalid 'eventType' values found:\n" + "\n".join(
                [f"Row {idx}, Column '{col}': {val}" for col,
                    idx, val in invalid_event_entries]
            )
            raise ValueError(error_message)

    if 'eventType' in dataset.columns and 'eventDate' not in dataset.columns:
        dataset['eventDate'] = np.nan

    updateLog(f"log/{user}uploadProgress.txt",
              "checking whether upload is for DATASET nodes", write='a')

    isDataset = False
    if "label" in dataset.columns:
        if dataset['label'].iloc[0] == "DATASET":
            isDataset = True

    if isDataset:
        updateLog(f"log/{user}uploadProgress.txt",
                  "upload is for DATASET nodes", write='a')
    else:
        updateLog(f"log/{user}uploadProgress.txt",
                  "upload is for CATEGORY nodes", write='a')

    if isDataset and uploadOption == "add_node":
        query = "unwind $rows as row match (d:DATASET {shortName: row.shortName}) return d.shortName as shortName"
        shortNames = getQuery(query, driver, params={
            "rows": dataset[['shortName']].to_dict(orient='records')}, type="list")
        if len(shortNames) > 0:
            raise ValueError(
                "Error: shortName already exists for: " + ", ".join(shortNames))

    dataset = dataset.dropna(axis=1, how='all')

    updateLog(f"log/{user}uploadProgress.txt",
              "checking column names", write='a')

    column_names = []
    required = []
    if isDataset:
        if uploadOption == "add_node":
            required = ["CMName", "label", "shortName", "DatasetCitation"]
        else:
            required = ['CMID']
    else:
        if uploadOption == "add_node":
            required = ["CMName", "Name", "label", "Key", "datasetID"]
        elif uploadOption == "add_uses":
            required = ["Name", "CMID", "Key", "datasetID", 'label']
        else:
            required = ["CMID", "Key", "datasetID"]
    column_names = required + nodeProperties + linkProperties

    # Remove None values
    column_names = [col for col in column_names if col is not None]

    errors = [
        f"{col} must be in dataset" for col in column_names if col not in dataset.columns]

    if len(errors) > 0:
        updateLog(f"log/{user}uploadProgress.txt",
                  "\n".join(errors), write='a')
        raise ValueError("\n".join(errors))

    properties = getPropertiesMetadata(driver)
    properties = pd.DataFrame(properties)

    updateLog(f"log/{user}uploadProgress.txt", "Creating import ID", write='a')
    getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver)

    # Grouping linkproperties for a common super label.
    if not isDataset:
        updateLog(f"log/{user}uploadProgress.txt",
                  "Combining paired properties", write='a')
        paired = properties.merge(pd.DataFrame(
            {'property': dataset.columns}), on='property')
        grouped_columns = paired[paired['group'].notna()][[
            'property', 'group']]
        grouped_dict = dataset.apply(
            lambda row: create_grouped_columns(row, grouped_columns), axis=1)
        grouped_df = pd.DataFrame(grouped_dict.tolist())
        dataset = pd.concat([dataset, grouped_df], axis=1)
        columns_to_drop = grouped_columns[grouped_columns['property']
                                          != 'parent']['property'].tolist()
        # Drop the columns from dataset, keeping the 'parent' column
        dataset = dataset.drop(columns=columns_to_drop).copy()
        for group in grouped_columns['group'].unique():
            linkProperties.append(group)
        linkProperties = list(set(linkProperties))

    uniqueID = 'importID'
    dataset['importID'] = dataset.index + 1

    # Combining columns and merging rows
    if "CMID" in dataset.columns and (not 'parentContext' in dataset.columns or 'geoCoords' in dataset.columns):
        if "datasetID" in dataset.columns and "Key" in dataset.columns:
            if "CMID" in dataset.columns:
                dataset = combine_properties(
                    dataset, ["CMID", "datasetID", "Key"])
            else:
                dataset = combine_properties(dataset, ["datasetID", "Key"])

    # updating the format of the mention columns.
    if 'yearStart' in dataset.columns or 'yearEnd' in dataset.columns or 'recordStart' in dataset.columns or 'recordEnd' in dataset.columns or 'sampleSize' in dataset.columns:
        updateLog(f"log/{user}uploadProgress.txt",
                  "updating integer columns", write='a')
        date_columns = ['yearStart', 'yearEnd',
                        'recordStart', 'recordEnd', 'sampleSize']
        for col in date_columns:
            if col in dataset.columns:
                dataset[col] = dataset[col].apply(
                    lambda x: x if pd.isna(x) or x == '' else str(int(float(x))))

    sq = range(0, len(dataset), batchSize)

    try:
        final_result = pd.DataFrame()
        dataset_match = pd.DataFrame()

        # check CMID, and datasetID, country, district, parent, language, religion, for SocioMap
        # CMID, datasetID, period, country, district, parent, for ArchaMap

        for s in sq:
            sub_dataset = dataset.iloc[s:s + batchSize].copy()
            max_row = len(sub_dataset) - 1 + s
            updateLog(f"log/{user}uploadProgress.txt",
                      f"uploading {s} to {max_row} of {len(dataset)}", write='a')

            if addDistrict:
                updateLog(f"log/{user}uploadProgress.txt",
                          "Adding districts", write='a')
                matches = getQuery(params={'rows': sub_dataset[[
                    "datasetID"]]}, q='DISTRICT QUERY', database=database, user='1')
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(
                        matches, on="datasetID", how="left")
                    linkProperties.append('country')

            if addRecordYear:
                updateLog(f"log/{user}uploadProgress.txt",
                          "Adding record year", write='a')
                matches = getQuery(
                    params={'rows': sub_dataset[["datasetID"]]}, q='RECORD_YEAR QUERY', driver=driver)
                if not matches.empty:
                    sub_dataset = sub_dataset.merge(
                        matches, on="datasetID", how="left")
                    linkProperties.append('recordStart')

            sub_dataset = sub_dataset.fillna('')

            node_columns = ["CMName", uniqueID, "label"] + nodeProperties
            node_columns = [
                col for col in node_columns if col in sub_dataset.columns]
            node_columns = list(dict.fromkeys(node_columns))

            nodes = pd.DataFrame()
            if isDataset and not 'CMID' in sub_dataset.columns:
                required_cols = list(set(
                    ["CMName", "shortName", "DatasetCitation", uniqueID, 'label'] + nodeProperties))
                required_cols = [
                    col for col in required_cols if col in sub_dataset.columns]
                nodes = sub_dataset[required_cols].drop_duplicates()
            elif not isDataset:
                if "Name" and "CMID" in sub_dataset.columns:
                    nodes = sub_dataset[sub_dataset["CMID"]
                                        == ''][node_columns].drop_duplicates()
                elif "Name" in sub_dataset.columns:
                    nodes = sub_dataset[node_columns]

            if not nodes.empty:
                if uploadOption =="add_uses":
                    if "CMName" in dataset_dup.columns:
                        cm_mapping = dataset_dup["CMName"].reset_index()
                        cm_mapping["importID"] = cm_mapping["index"] + 1
                        cm_mapping = cm_mapping[["importID", "CMName"]]

                        # Ensure importID types match
                        cm_mapping["importID"] = cm_mapping["importID"].astype(str)
                        nodes["importID"] = nodes["importID"].astype(str)

                        # Merge onto nodes
                        nodes1 = nodes.merge(cm_mapping, on="importID", how="left")
                print(nodes1)
                updateLog(f"log/{user}uploadProgress.txt",
                          "Adding nodes with columns: " + ", ".join(nodes.columns), write='a')
                match = createNodes(
                    nodes, database, user=user, uniqueID=uniqueID)
                match = pd.DataFrame(match)
                match = match.astype(str)
                sub_dataset = sub_dataset.astype(str)
                join_cols = list(
                    set(sub_dataset.columns.intersection(match.columns)))
                dataset_match = pd.merge(
                    sub_dataset, match, how="outer", on=join_cols)
            else:
                dataset_match = sub_dataset.copy()

            link_columns = ["datasetID", "CMName", "CMID",
                            "Name", "altNames", "Key", "label"] + linkProperties
            link_columns = [
                col for col in link_columns if col in dataset_match.columns]
            link_columns = list(dict.fromkeys(link_columns))

            if not isDataset:
                updateLog(f"log/{user}uploadProgress.txt",
                          "Adding USES relationships", write='a')

                links = dataset_match[link_columns].drop_duplicates().copy()

                if "Name" in links.columns and "altNames" in links.columns:
                    updateLog(f"log/{user}uploadProgress.txt",
                              "Combining names and alternate names", write='a')
                    links = combine_names_and_altNames(
                        links, "Name", "altNames")

                if linkProperties is not None and 'geoCoords' in linkProperties:
                    updateLog(f"log/{user}uploadProgress.txt",
                              "updating geo coordinates", write='a')
                    # return links
                    links['geoCoords'] = links['geoCoords'].apply(
                        convert_coordinates)

                if "parentContext" in linkProperties:
                    updatePC = True
                    test = links[links['parentContext'].notna()
                                 ]['parentContext']
                    if not test.empty:
                        # first_row_value = test.loc[0, 'parentContext']
                        first_row_value = test.iloc[0]
                        val = first_row_value.split("; ")[0]
                        # if is_valid_json(val):
                        #     updateLog(f"log/{user}uploadProgress.txt", "parentContext is already formatted", write = 'a')
                        #     updatePC = False

                    updateLog(f"log/{user}uploadProgress.txt",
                              "updating parentContext", write='a')
                    # return links
                    if updatePC:
                        def filter_dict(d):
                            filtered_dict = ""
                            try:
                                d = json.loads(d)
                                filtered_dict = {
                                    k: v for k, v in d.items() if pd.notna(v) and v != ""}
                                # If 'parent' is the only key remaining, return an empty string
                                if list(filtered_dict.keys()) == ['parent']:
                                    filtered_dict = ""
                            except json.JSONDecodeError:
                                return ""

                            return filtered_dict

                        sub_links = links.copy()

                        # sub_links['parentContext'] = sub_links['parentContext'].apply(lambda x: json.loads(x))

                        sub_links['parentContext'] = sub_links['parentContext'].apply(
                            filter_dict)

                        # Step 1: Convert parentContext dictionary to a JSON string
                        # Apply json.dumps to convert dictionaries to JSON strings
                        sub_links['parentContext'] = sub_links['parentContext'].apply(
                            lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, dict) else x)

                        # Step 2: Remove square brackets if present in strings
                        sub_links['parentContext'] = sub_links['parentContext'].apply(
                            lambda x: re.sub(r'\[|\]', '', x) if isinstance(x, str) else x)

                        # Step 3: Unnest data (apply to each row)
                        sub_links = sub_links.explode(
                            'parentContext').reset_index(drop=True)

                        # Step 4: Handle missing parent values by setting parentContext to None where parent is NaN
                        sub_links['parentContext'] = sub_links.apply(
                            lambda row: None if pd.isna(row['parent']) else row['parentContext'], axis=1)

                        # Step 5: Drop 'eventDate' and 'eventType' columns if they exist
                        sub_links = sub_links.drop(
                            columns=[col for col in ['eventDate', 'eventType'] if col in sub_links.columns])

                        # Step 6: Group by 'datasetID', 'CMID', and 'Key'
                        grouped_links = sub_links.groupby(
                            ['datasetID', 'CMID', 'Key'])

                        # Step 7: Combine lists of parentContext and parent, keeping their JSON representations intact
                        sub_links = grouped_links.agg({
                            'parentContext': lambda x: list(x),
                            'parent': lambda x: list(x)
                        }).reset_index()

                        # Step 8: Convert lists of JSON strings to a semicolon-separated string
                        for index, row in sub_links.iterrows():
                            sub_links.at[index, 'parentContext'] = process_parent_context_element(
                                row['parentContext'])
                            sub_links.at[index, 'parent'] = process_parent_context_element(
                                row['parent'])

                        # Step 9: Merge the grouped data back into the original DataFrame
                        links = links.drop(
                            columns=['parentContext', 'parent']).copy()
                        links = pd.merge(links, sub_links, on=[
                            'datasetID', 'CMID', 'Key'], how='left')

                    # Replace values that do not contain 'eventDate' or 'eventType' with an empty string
                    links['parentContext'] = links['parentContext'].apply(
                        lambda x: x if 'eventDate' in x or 'eventType' in x else ""
                    )

                updateLog(f"log/{user}uploadProgress.txt",
                          str(links.columns), write='a')

                if uploadOption == "add_node":
                    required_for_operation = required + ["CMID"]
                else:
                    required_for_operation = required

                # link_cols = ['datasetID', 'CMID', 'Key'] + linkProperties
                link_cols = required_for_operation + linkProperties
                link_cols = list(set(link_cols))
                link_cols = [col for col in link_cols if col in links.columns]

                if uploadOption == "update_replace":
                    updateLog(f"log/{user}uploadProgress.txt",
                              "Overwriting property", write='a')
                    result = updateProperty(
                        links[link_cols], database=database, user=user, updateType="overwrite")
                elif uploadOption == "update_add":
                    updateLog(f"log/{user}uploadProgress.txt",
                              "Updating property", write='a')
                    result = updateProperty(
                        links[link_cols], database=database, user=user, updateType="update")
                else:
                    updateLog(f"log/{user}uploadProgress.txt",
                              "Adding new USES relationships", write='a')
                    # link_cols.append("label")
                    links = links[link_cols]
                    result = createUSES(
                        links=links, database=database, user=user, create="MERGE")
                    print(result)
                if isinstance(result, str):
                    updateLog(f"log/{user}uploadProgress.txt",
                              result, write='a')
                    raise ValueError(result)

                updateLog(f"log/{user}uploadProgress.txt",
                          "Processing returned CMIDs", write='a')
                try:
                    cmid_values = [link['CMID'] for link in result['result']]
                    if len(cmid_values) < len(result['result']):
                        missing_links = [
                            link for link in result['result'] if 'CMID' not in link]
                        raise KeyError(
                            f"Missing 'CMID' in {len(missing_links)} link(s): {missing_links}")
                    updateLog(f"log/{user}uploadProgress.txt",
                              "adding CMName to Name parameter", write='a')
                    addCMNameRel(database, CMID=cmid_values)
                    updateLog(f"log/{user}uploadProgress.txt",
                              "updating alternate names", write='a')
                    updateAltNames(driver, CMID=cmid_values)
                    updateLog(f"log/{user}uploadProgress.txt",
                              "updated alternate names", write='a')
                except KeyError as e:
                    updateLog(f"log/{user}uploadProgress.txt",
                              f"Error updating alternate names: {e}", write='a')
                    continue

                updateLog(f"log/{user}uploadProgress.txt",
                          "combining results", write='a')
                result = pd.DataFrame(result['result'])
                final_result = pd.concat([final_result, result], axis=0)
                updateLog(f"log/{user}uploadProgress.txt",
                          "results combined", write='a')

                updateLog(f"log/{user}uploadProgress.txt",
                          "Completed updating USES relationships", write='a')

            else:
                required_for_operation = required + ["CMID"]
                node_columns = list(
                    set(required_for_operation + nodeProperties))
                node_columns = [
                    col for col in node_columns if col in dataset_match.columns]
                nodes = dataset_match[node_columns].drop_duplicates()
                if uploadOption == "node_replace":
                    updateLog(f"log/{user}uploadProgress.txt",
                              "overwriting Dataset properties", write='a')
                    updateProperty(nodes, database=database, user=user,
                                   updateType="overwrite", propertyType="DATASET")
                elif uploadOption == "node_add":
                    updateLog(f"log/{user}uploadProgress.txt",
                              "updating dataset properties", write='a')
                    updateProperty(nodes, database=database, user=user,
                                   updateType="update", propertyType="DATASET")

                updateLog(f"log/{user}uploadProgress.txt",
                          "processing Dataset properties", write='a')
                cmids = dataset_match['CMID'].unique()
                processDATASETs(database=database, user=user, CMID=cmids)
                final_result = pd.concat([final_result, dataset_match], axis=0)

            if uniqueID == 'importID':
                getQuery(
                    "MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver=driver)

            updateLog(f"log/{user}uploadProgress.txt",
                      "End of batch", write='a')

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

    dup_result = final_result.copy(deep=True)

    final_result = final_result.loc[:, ~final_result.columns.duplicated()]
    final_result = final_result.drop_duplicates()
    final_result = final_result.dropna(axis=1, how='any')
    final_result = final_result.dropna(how='all').reset_index(drop=True).copy()
    cols = list({x for x in required_for_operation if x in dataset.columns})
    cols = list({x for x in cols if x in final_result.columns})
    df = dataset[cols]

    # secondary = pd.concat([dataset, dup_result.add_suffix('_new')], axis=1)

    # secondary.to_excel("sec.xlsx",index = False)

    final_result = pd.merge(df, final_result, how='left', on=cols)

    final_result = add_error_column(final_result, user)
    final_result = final_result.fillna("")
    final_result = final_result.drop_duplicates()

    '''with open(f"log/{user}uploadProgress.txt", 'a') as f:
        f.write("Completed dataset upload\n")'''

    return final_result
