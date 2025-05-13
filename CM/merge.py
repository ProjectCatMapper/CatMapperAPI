"""merge.py"""

from .utils import *
from .keys import *
from .translate import *
import pandas as pd
from flask import jsonify
import os
import re
from datetime import datetime
import hashlib
import base64
import zipfile
import numpy as np


def generate_unique_hash():
    now = datetime.utcnow().isoformat()
    return base64.urlsafe_b64encode(
        hashlib.sha256(now.encode()).digest()
    ).decode()[:16]


def joinDatasets(database, joinLeft, joinRight):
    try:

        joinLeft = pd.DataFrame(joinLeft)
        joinRight = pd.DataFrame(joinRight)

        if 'datasetID' not in joinLeft.columns:
            raise ValueError(
                "The 'datasetID' column is missing from the joinLeft DataFrame.")

        if 'datasetID' not in joinRight.columns:
            raise ValueError(
                "The 'datasetID' column is missing from the joinRight DataFrame.")

        driver = getDriver(database)

        joinLeft = pd.DataFrame(joinLeft)
        joinRight = pd.DataFrame(joinRight)

        # Drop 'CMID' and 'CMName' only if they exist in the columns
        joinLeft = joinLeft.drop(
            columns=[col for col in ['CMID', 'CMName'] if col in joinLeft.columns]).copy()
        joinRight = joinRight.drop(
            columns=[col for col in ['CMID', 'CMName'] if col in joinRight.columns]).copy()

        datasetID_left = joinLeft['datasetID'].unique()
        datasetID_right = joinRight['datasetID'].unique()

        # Query keys for left dataset
        match_left_query = """
        UNWIND $datasetID AS id
        MATCH (d:DATASET {CMID: id})-[r:USES]->()
        WITH d, split(r.Key, ';') AS Key
        WITH d, [i IN Key | split(i, ':')[0]] AS Key
        RETURN DISTINCT d.CMID AS datasetID, Key
        """
        match_left = getQuery(match_left_query, driver, {
                              "datasetID": datasetID_left})

        # Query keys for right dataset
        match_right_query = """
        UNWIND $datasetID AS id
        MATCH (d:DATASET {CMID: id})-[r:USES]->()
        WITH d, split(r.Key, ';') AS Key
        WITH d, [i IN Key | split(i, ':')[0]] AS Key
        RETURN DISTINCT d.CMID AS datasetID, Key
        """
        match_right = getQuery(match_right_query, driver, {
                               "datasetID": datasetID_right})

        match_left = pd.DataFrame(match_left)
        match_right = pd.DataFrame(match_right)

        left_keys = match_left['Key'].explode(
        ).unique() if 'Key' in match_left else []
        right_keys = match_right['Key'].explode(
        ).unique() if 'Key' in match_right else []

        # Check for available columns
        found_left_keys = [key for key in joinLeft.columns if key in left_keys]
        found_right_keys = [
            key for key in joinRight.columns if key in right_keys]

        # Throw an error only if none of the keys are found
        if not found_left_keys:
            print(
                {"error": "Cannot continue with merge: no matching required columns found in 'joinLeft'"})
        if not found_right_keys:
            print(
                {"error": "Cannot continue with merge: no matching required columns found in 'joinRight'"})

        # Convert only the found columns to string type
        joinLeft[found_left_keys] = joinLeft[found_left_keys].astype(
            str, errors='ignore')
        joinRight[found_right_keys] = joinRight[found_right_keys].astype(
            str, errors='ignore')

        merge_left = joinLeft[['datasetID'] + found_left_keys].copy()
        merge_left = createKey(merge_left, cols=found_left_keys).rename(
            columns={'Key': 'term', 'datasetID': 'dataset'})
        translate_left = translate(database=database, property="Key", domain="CATEGORY", term="term", table=merge_left,
                                   key='false', country=None, context=None, dataset='dataset', yearStart=None, yearEnd=None, query='false', uniqueRows=False)
        translate_left = translate_left.rename(
            columns=lambda x: x.replace('_term', ''))
        merge_left = translate_left[['term', 'CMID', 'CMName', 'dataset']].merge(merge_left, on=['term', 'dataset']).drop(
            columns='term').rename(columns={'dataset': 'datasetID'}).drop_duplicates()

        # merge right
        merge_right = joinRight[['datasetID'] + found_right_keys].copy()
        merge_right = createKey(merge_right, cols=found_right_keys).rename(
            columns={'Key': 'term', 'datasetID': 'dataset'})
        translate_right = translate(
            database=database,
            property="Key",
            domain="CATEGORY",
            term="term",
            table=merge_right,
            key='false',
            country=None,
            context=None,
            dataset='dataset',
            yearStart=None,
            yearEnd=None,
            query='false',
            uniqueRows=False
        )
        translate_right = translate_right.rename(
            columns=lambda x: x.replace('_term', ''))
        merge_right = (
            translate_right[['term', 'CMID', 'CMName', 'dataset']]
            .merge(merge_right, on=['term', 'dataset'])
            .drop(columns='term')
            .rename(columns={'dataset': 'datasetID'})
            .drop_duplicates()
        )

        # Final joining
        # Step 1: Identify overlapping columns between merge_left and merge_right, excluding CMID and CMName
        overlapping_columns = [
            col for col in merge_left.columns if col in merge_right.columns and col not in ['CMID', 'CMName']]

        # Step 2: Perform the first merge between merge_left and merge_right with suffixes for overlapping columns
        link_file = merge_left.merge(
            merge_right,
            on=['CMID', 'CMName'],
            suffixes=('_left', '_right')
        )

        # Step 3: Update found_left_keys and found_right_keys to include suffixes for the identified overlapping columns
        found_left_keys_with_suffix = [
            f"{key}_left" if key in overlapping_columns else key for key in found_left_keys]
        found_right_keys_with_suffix = [
            f"{key}_right" if key in overlapping_columns else key for key in found_right_keys]

        # Step 4: Rename datasetID in joinLeft and joinRight for consistent merging
        joinLeft = joinLeft.rename(columns={'datasetID': 'datasetID_left'})
        joinRight = joinRight.rename(columns={'datasetID': 'datasetID_right'})

        left_rename_mapping = dict(
            zip(found_left_keys, found_left_keys_with_suffix))
        right_rename_mapping = dict(
            zip(found_right_keys, found_right_keys_with_suffix))
        joinLeft = joinLeft.rename(columns=left_rename_mapping)
        joinRight = joinRight.rename(columns=right_rename_mapping)

        # Step 5: Merge link_file with joinLeft without adding further suffixes for overlapping columns
        link_file = link_file.merge(
            joinLeft,
            left_on=['datasetID_left'] + found_left_keys_with_suffix,
            right_on=['datasetID_left'] + found_left_keys_with_suffix,
            how='left',
            # Prevents adding additional _x suffixes
            suffixes=('_left', '_right')
        )

        # Step 6: Merge link_file with joinRight without adding further suffixes for overlapping columns
        link_file = link_file.merge(
            joinRight,
            left_on=['datasetID_right'] + found_right_keys_with_suffix,
            right_on=['datasetID_right'] + found_right_keys_with_suffix,
            how='left',
            suffixes=('_left', '_right')
        )

        # Step 7: Final clean-up to drop duplicates and sort by specified columns
        link_file = link_file.drop_duplicates().sort_values(
            by=['datasetID_left', 'datasetID_right', 'CMName', 'CMID'])

        # replace NaN with empty string
        link_file = link_file.fillna("")

        desired_order = ['CMID', 'CMName',
                         'datasetID_left', 'datasetID_right']
        remaining_cols = [
            col for col in link_file.columns if col not in desired_order]
        link_file = link_file[desired_order + remaining_cols]

        return link_file.to_dict(orient='records')

    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500


def proposeMerge(dataset_choices, category_label, criteria, database, intersection, ncontains=2):

    try:
        driver = getDriver(database)

        if len(dataset_choices) < 1:
            return jsonify({"message": "Please select more options"}), 400

        if criteria == "standard":

            query = f"""
                            UNWIND $datasets AS dataset
                            MATCH (c:{category_label})<-[r:USES]-(d:DATASET {{CMID: dataset}})
                            RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMName AS CMName, c.CMID AS CMID,
                                            apoc.text.join(apoc.coll.toSet(r.Name), '; ') AS Name
                            ORDER BY CMName
                    """

            merged = getQuery(query, driver=driver, params={
                              'datasets': dataset_choices})

            if "Neo.ClientError.Statement.SyntaxError" in merged[0]:
                raise Exception(merged[0])

            merged = pd.DataFrame(merged)

            if not merged.empty:
                # Pivot wider equivalent
                merged_df = merged.pivot_table(
                    index=['CMName', 'CMID'],
                    columns='datasetID',
                    values=['Key', 'Name'],
                    aggfunc=lambda x: '; '.join(filter(None, set(x)))
                )

                merged_df.columns = [
                    f"{col[0]}_{col[1]}" for col in merged_df.columns]
                merged_df.reset_index(inplace=True)

                # Flatten lists, filter keys if intersection is off
                if intersection:
                    for col in merged_df.columns:
                        if 'Key' in col:
                            merged_df = merged_df[merged_df[col].notna()]

                merged = merged_df.fillna("")
                merged = merged.to_dict(orient='records')
                return merged
            else:
                return jsonify({"message": "No data found"}), 404

        elif criteria == "extended":
            if len(dataset_choices) > 2:
                return jsonify({"message": "Please select only two datasets"}), 400

            query = generate_cypher_query(unlist(category_label), ncontains)

            matches = getQuery(query, driver, {"datasets": dataset_choices})

            matches = pd.DataFrame(matches)

            if matches.empty:
                return jsonify({"message": "No data found"}), 404

            # Split into groups by 'datasetID'
            matches_grp = [group for _, group in matches.groupby("datasetID")]

            # Perform inner join on the first two groups
            merge_how = "inner" if intersection else "outer"
            result = pd.merge(
                matches_grp[0].drop(columns=["datasetID"]),
                matches_grp[1].drop(columns=["datasetID"]),
                on=["LCA_CMID", "LCA_CMName"],
                how=merge_how,
                suffixes=(f"_{dataset_choices[0]}", f"_{dataset_choices[1]}")
            ).drop_duplicates()

            if result.empty:
                return jsonify({"message": "No common ancestors found"}), 404

            result["nTie"] = result.filter(like="tie").sum(axis=1, skipna=True)

            key1 = f"Key_{dataset_choices[0]}"
            key2 = f"Key_{dataset_choices[1]}"

            min_nTie_1 = result.groupby(key1)["nTie"].transform("min")
            min_nTie_2 = result.groupby(key2)["nTie"].transform("min")

            df_filtered = result[(result["nTie"] == min_nTie_1) | (
                result["nTie"] == min_nTie_2)]

            df_filtered = df_filtered.drop_duplicates(
                subset=[key1, key2], keep="first")

            # filter df nTie to exclude values greater than ncontains
            df_filtered = df_filtered[df_filtered["nTie"] <= ncontains]

           # Reorder columns
            cols = ["LCA_CMID", "LCA_CMName", "nTie"] + \
                [col for col in result.columns if col not in [
                    "LCA_CMID", "LCA_CMName", "nTie"]]
            result = df_filtered[cols]
            result = result.fillna("")

            return result.to_dict(orient='records')

        else:
            raise Exception("Invalid criteria")

    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500


def generate_cypher_query(domain, nContains):
    if not isinstance(domain, str):
        raise ValueError("domain must be a string")
    if nContains < 1:
        raise ValueError("nContains must be at least 1")
    elif nContains > 4:
        raise ValueError("nContains must be at most 4")
    base_query = f"""
    UNWIND $datasets AS dataset
    MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{domain})
    RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
    c.CMID as LCA_CMID, c.CMName as LCA_CMName,
    apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, 0 as tie
    """

    union_queries = []
    for i in range(1, nContains + 1):
        union_query = f"""
        UNION ALL
        UNWIND $datasets AS dataset
        MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{domain})
        MATCH (c)<-[rc:CONTAINS*{i}]-(p:{domain})
        WHERE isEmpty([i in rc WHERE i.generic = true])
        RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
        p.CMID as LCA_CMID, p.CMName as LCA_CMName,
        apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, {i} as tie
        """
        union_queries.append(union_query)

    full_query = base_query + "\n".join(union_queries)
    return full_query


def transform_variables_r(variables):
    variables["transform"] = variables["transform"].str.replace(
        "~", "!", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "=", "==", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "!==", "!=", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "concat", "paste0", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        r',0\)', ',na.rm = True', regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "in", "%in%", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "na.rm == T", "na.rm = True", regex=True)
    variables["transform"] = variables["transform"].str.replace(
        "== as.numeric", "= as.numeric", regex=True)
    return variables


def load_r_syntax_template(filename, replacements):
    """ Reads R syntax template and replaces placeholders """
    try:
        with open(filename, "r") as file:
            content = file.read()
        for key, value in replacements.items():
            content = content.replace(key, value)
        return content
    except FileNotFoundError:
        print(f"Error: {filename} not found. Ensure the file exists.")
        return None


def zip_output_files(files, dirpath, zip_filename="output.zip"):
    """ Zip all generated files into a single archive """
    zip_path = os.path.join(dirpath, zip_filename)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            if os.path.exists(file):  # Ensure file exists before adding
                # Store without full path
                zipf.write(file, os.path.basename(file))
                print(f"Added to ZIP: {file}")
            else:
                print(
                    f"Warning: {file} does not exist and was not added to the ZIP.")

    print(f"ZIP file created: {zip_path}")
    return zip_path


def getMergingTemplate(datasetID, database):
    try:
        driver = getDriver(database)

        query = """
            RETURN "" as mergingID, "" as stackID, "" as datasetID, "" as datasetName, "Please enter the working directory as the first filepath" as filePath
            UNION ALL 
            MATCH (m:DATASET {CMID: $datasetID})-[:MERGING]->(s:DATASET)-[:MERGING]->(d:DATASET)
            RETURN
            m.CMID as mergingID, s.CMID as stackID, d.CMID as datasetID, d.CMName as datasetName, "" as filePath
            """
        data = getQuery(query, driver=driver, params={
            "datasetID": datasetID})

        if data[0].get("error"):
            return jsonify({"message": "No data found"}), 404

        desired_order = ["mergingID", "stackID",
                         "datasetID", "datasetName", "filePath"]

        template = [
            {key: item.get(key, "") for key in desired_order}
            for item in data
        ]

        return jsonify(template)

    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500


def createSyntax(template, database="SocioMap", domain="ETHNICITY",
                 syntax="R", dirpath=None, download=True):
    try:

        try:
            template = pd.DataFrame(template)
        except Exception as e:
            raise ValueError("Template must be a pandas DataFrame.")

        if template.empty:
            raise ValueError("Template DataFrame is empty.")

        if "datasetID" not in template.columns:
            raise ValueError(
                "Template DataFrame must contain 'datasetID' column.")

        if "mergingID" not in template.columns:
            raise ValueError(
                "Template DataFrame must contain 'mergingID' column.")

        if "stackID" not in template.columns:
            raise ValueError(
                "Template DataFrame must contain 'stackID' column.")

        if dirpath is None:
            dirpath = "./tmp"

        driver = getDriver(database)

        if "filePath" not in template.columns:
            raise ValueError(
                "Must upload a list of datasets with the filePath column before generating syntax.")

        wd = template.iloc[0]["filePath"]

        if re.match(r"^[a-zA-Z]:\\\\", wd) or "\\" in wd:
            print("Detected Windows path. Converting to compatible format...")

            # Convert backslashes to forward slashes
            wd = wd.replace("\\", "/")

            # Ensure proper formatting (R escape sequences)
            wd = wd.replace(" ", "\\ ")  # Escape spaces if needed

        template = template.iloc[1:]

        # verify CMIDs
        cols = ["mergingID", "stackID", "datasetID"]
        cols = [col for col in cols if col in template.columns]
        CMIDs = list(set(template[cols].values.flatten().tolist()))

        check = getQuery(
            """
            UNWIND $CMIDs as cmid
            match (a:DATASET {CMID: cmid})
            return a.CMID as CMID, a.CMName as CMName
            """, driver=driver,
            params={"CMIDs": CMIDs},
            type="df"
        )

        missing = set(CMIDs) - set(check["CMID"].tolist())
        missing = [str(m) + "\n" for m in missing]

        if len(check) != len(CMIDs):
            raise ValueError(
                "Error: One or more CMIDs not found in the database\nMissing CMIDs: ", missing)
        else:
            print("All CMIDs found in the database.")
        db_query = """
            unwind $rows as row
            match (m:DATASET {CMID: row.mergingID})-[rs:MERGING]->(s:DATASET {CMID: row.stackID})-[rm:MERGING]->(v:VARIABLE)<-[ru:USES]-(d:DATASET {CMID: row.datasetID})
            return
            m.CMID as mergingID, m.CMName as mergingName, s.CMID as stackID, s.CMName as stackName, d.CMID as datasetID, d.CMName as datasetName, rs.aggBy as aggBy, v.CMID as variableCMID, head(apoc.coll.flatten(collect(rm.varName),true)) as varName, rm.transform as transform, rm.Rtransform as Rtransform, rm.Rfunction as Rfunction, rm.summaryStatistic as summaryStatistic, ru.Key as Key
            """
        data = getQuery(db_query, driver=driver, params={
                        "rows": template.to_dict(orient='records')}, type="df")

        # pd.set_option('display.max_rows', None)

        # print(data.head(10))

        if "transform" in data.columns:
            print("transforming variables")

            if "Rtransform" in data.columns:
                data['transform'] = data['Rtransform'].combine_first(
                    data['transform'])
                data = transform_variables_r(data)
        data = data.reset_index()

        variables = data[["datasetID", "Key"]].copy()
        variables = variables.drop_duplicates()
        # variables = extract_key(variables, col="Key")
        variables[['variable', 'value']] = variables['Key'].str.split(
            ': ', n=1, expand=True)

        data = pd.merge(data, variables, on=["datasetID", "Key"], how="left")

        data["variable"] = data["variable"].str.lower()
        data = data.astype(str)
        data.replace("None", np.nan, inplace=True)
        data = pd.merge(
            data, template[["datasetID", "filePath"]], on="datasetID", how="left")
        # print(dirpath)
        data.to_excel(os.path.join(dirpath, "data.xlsx"), index=False)
        cat_query = f"""
            unwind $rows as row
            match (d:DATASET {{CMID: row.datasetID}})-[ru:USES]->(c:{domain}) optional match (c)-[:EQUIVALENT]->(e:{domain})
            return d.CMID as datasetID, ru.Key as Key, c.CMID as CMID, c.CMName as CMName, e.CMID as equivalentCMID, e.CMName as equivalentCMName
        """
        categories = getQuery(cat_query, driver=driver, params={
            "rows": template.to_dict(orient='records')}, type="df")

        print(len(categories))
        # number of categories that have equivalent categories
        print(len(categories[categories["equivalentCMID"].notnull()]))
        categories.columns
        keys_df = categories[["datasetID", "Key"]].copy()
        keys_df = keys_df.drop_duplicates()
        # keys_df = extract_key(keys_df, col="Key")
        # keys_df = keys_df.melt(
        #     id_vars=["datasetID",'Key'],
        #     var_name='variable',
        #     value_name='value'
        # )
        # print(keys_df.head(10))
        keys_df['Key2'] = keys_df['Key'].str.split('; ')
        keys_df = keys_df.explode('Key2').reset_index(drop=True)
        keys_df[['variable', 'value']] = keys_df['Key2'].str.split(
            ': ', n=1, expand=True)
        keys_df.drop(columns=["Key2"], inplace=True)
        categories = pd.merge(categories, keys_df, on=[
                              "datasetID", "Key"], how="left")
        categories = categories.drop_duplicates(
            subset=["datasetID", "Key", "CMID", "variable", "value"])
        categories["variable"] = categories["variable"].str.lower()
        categories = categories.astype(str)
        categories.replace("None", np.nan, inplace=True)
        # len(categories)
        # print(categories.head(100))
        categories.to_excel(os.path.join(
            dirpath, "categories.xlsx"), index=False)
        r_syntax_template = "syntax/R_syntax.txt"
        replacements = {
            # Functions applied
            "${f}": "\n".join(data['transform'].dropna()),
            "${wd}": wd,  # Working directory
            "${database}": database  # Database name
        }

        # print(replacements)
        if syntax == "R":
            r_syntax = load_r_syntax_template(r_syntax_template, replacements)
            with open(os.path.join(dirpath, "syntax.R"), "w") as f:
                f.write(r_syntax)
        else:
            raise ValueError("Invalid syntax type. Only 'R' is supported.")

        files = []
        files.extend([
            os.path.join(dirpath, "data.xlsx"),
            os.path.join(dirpath, "categories.xlsx"),
            os.path.join(dirpath, "syntax.R")

        ])

        if download == True:
            hash_id = generate_unique_hash()
            zip_filename = f"merged_output_{hash_id}.zip"
        else:
            hash_id = ""
            zip_filename = "merged_output.zip"
        zip_path = zip_output_files(files, dirpath, zip_filename)

        return {"zip": zip_path, "hash": hash_id}

    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500
