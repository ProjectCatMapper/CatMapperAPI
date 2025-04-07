"""merge.py"""

from .utils import *
from .keys import *
from .translate import *
import pandas as pd
from flask import jsonify


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
                                   key='false', country=None, context=None, dataset='dataset', yearStart=None, yearEnd=None, query='false')
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
            query='false'
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
                if not intersection:
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


class createSyntax:
    def __init__(self, database, template, syntax="R", dirpath=None):
        self.syntax = syntax
        self.template = template
        self.database = database
        self.driver = getDriver(database)
        self.dirpath = dirpath if dirpath else os.path.abspath("./tmp")
        os.makedirs(self.dirpath, exist_ok=True)
        self.files = []

    def run_query(self, query, parameters=None):
        return getQuery(query, driver=self.driver, params=parameters, type="df")

    def transform_variables_r(self, variables):
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

    def load_r_syntax_template(self, filename, replacements):
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

    def zip_output_files(self, zip_filename="output.zip"):
        """ Zip all generated files into a single archive """
        zip_path = os.path.join(self.dirpath, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in self.files:
                if os.path.exists(file):  # Ensure file exists before adding
                    # Store without full path
                    zipf.write(file, os.path.basename(file))
                    print(f"Added to ZIP: {file}")
                else:
                    print(
                        f"Warning: {file} does not exist and was not added to the ZIP.")

        print(f"ZIP file created: {zip_path}")
        return zip_path

    def process(self):
        if "filePath" not in self.template.columns:
            raise ValueError(
                "Must upload a list of datasets with the filePath column before generating syntax.")

        wd = self.template.iloc[0]["filePath"]

        if re.match(r"^[a-zA-Z]:\\\\", wd) or "\\" in wd:
            print("Detected Windows path. Converting to compatible format...")

            # Convert backslashes to forward slashes
            wd = wd.replace("\\", "/")

            # Ensure proper formatting (R escape sequences)
            wd = wd.replace(" ", "\\ ")  # Escape spaces if needed

        self.template = self.template.iloc[1:]

        # verify CMIDs
        cols = ["mergingID", "stackID", "datasetID"]
        cols = [col for col in cols if col in self.template.columns]
        CMIDs = list(set(self.template[cols].values.flatten().tolist()))

        check = self.run_query(
            """
            UNWIND $CMIDs as cmid
            match (a:DATASET {CMID: cmid})
            return a.CMID as CMID, a.CMName as CMName
            """,
            parameters={"CMIDs": CMIDs}
        )

        missing = set(CMIDs) - set(check["CMID"].tolist())
        missing = [str(m) + "\n" for m in missing]

        if len(check) != len(CMIDs):
            raise ValueError(
                "Error: One or more CMIDs not found in the database\nMissing CMIDs: ", missing)
        else:
            print("All CMIDs found in the database.")
        self.template = self.template.drop(columns=["datasetName"])
        self.template = pd.merge(
            self.template, check, how="left", left_on="mergingID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "mergingName"})
        self.template = pd.merge(
            self.template, check, how="left", left_on="stackID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "stackName"})
        self.template = pd.merge(
            self.template, check, how="left", left_on="datasetID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "datasetName"})
        self.template = self.template.drop(
            columns=["CMID_x", "CMID_y", "CMID"])
        cols = ["mergingID", "mergingName", "stackID",
                "stackName", "datasetID", "datasetName", "filePath"]
        self.template = self.template[cols]

        variable_list = []
        for s in self.template["stackID"].unique():
            print("getting variables for stackID: ", s)
            if self.syntax == "R":
                result = self.run_query(
                    """
                unwind $stackID as id
                match (v:VARIABLE)<-[r:MERGING]-(s:DATASET {CMID: id}) 
                where not r.varName is null 
                return distinct head($mergingID) as mergingID, s.CMID as stackID,
                v.CMID as variableCMID, 
                r.varName as varName, r.transform as transform, 
                r.Rtransform as Rtransform, r.Rfunction as Rfunction, 
                r.summaryStatistic as summaryStatistic
                """,
                    parameters={
                        "stackID": s, "mergingID": self.template["mergingID"].tolist()}
                )
            else:
                raise ValueError("Error: syntax not yet supported.")
                # variables =  self.run_query(
                #         """
                #         unwind $stackIDs as id
                #         match (s:DATASET {CMID: id})-[:MERGING]->(d2:DATASET)
                #         return head($mergingID) as mergingID,
                #         s.datasetID as stackID, d2.datasetID as datasetID,
                #         d2.datasetName as datasetName order by datasetName
                #         """,
                #         parameters={"stackIDs": self.template["stackID"].tolist(), "mergingID": self.template["mergingID"].tolist()}
                #     )
            result = result.explode('varName', ignore_index=True)
            if "transform" in result.columns:
                print("transforming variables")

                if "Rtransform" in result.columns:
                    result['transform'] = result['Rtransform'].combine_first(
                        result['transform'])

                CMshort = "SM" if self.database == "SocioMap" else "AM"

                # Fill NaNs with empty strings
                result['transform'] = result['transform'].fillna('')

                # Safely apply regex to extract CMIDs
                result['CMID'] = result['transform'].apply(
                    lambda x: list(
                        set(re.findall(f'{CMshort}\\d+', x))) if x else []
                )

                # Replace empty lists with [NaN] to retain rows upon explosion
                result['CMID'] = result['CMID'].apply(
                    lambda x: x if x else [np.nan])

                # Explode, preserving all rows
                result = result.explode('CMID', ignore_index=True)

                variable_list.append(result)

            variables = pd.concat(variable_list, ignore_index=True)

            # Convert list-type columns to tuples (safe for hashing)
            for col in variables.columns:
                if variables[col].apply(lambda x: isinstance(x, list)).any():
                    variables[col] = variables[col].apply(
                        lambda x: tuple(x) if isinstance(x, list) else x)

            # Now drop duplicates safely
            variables = variables.drop_duplicates(ignore_index=True)

        if variables.empty:
            raise ValueError("Error: No variables retrieved from Neo4j.")

        # Check if "summaryStatistic" is a column in the variables DataFrame
        if "summaryStatistic" in variables.columns:
            print("Getting summary statistic")

            # Select unique (mergingID, stackID) pairs where mergingID is not empty
            temp_stacks = (
                self.template[['mergingID', 'stackID']]
                .drop_duplicates()
                .query("mergingID != ''")
            )

            agg_by_list = []

            query = """
            UNWIND $rows as row
            MATCH (m:DATASET {CMID: row.mergingID})-[r:MERGING]->(s:DATASET {CMID: row.stackID})
            return s.CMID as stackID, r.aggBy as aggBy
            """
            tempStacks = self.template[[
                'mergingID', 'stackID']].drop_duplicates()
            aggBy = self.run_query(
                query, parameters={"rows": tempStacks.to_dict(orient='records')})
            if aggBy is not None and not aggBy.empty:
                variables = pd.merge(
                    variables, aggBy, on='stackID', how='left')

            # Combine all query results into one DataFrame
            agg_by = pd.concat(
                agg_by_list, ignore_index=True) if agg_by_list else pd.DataFrame()

            # Left join with variables on "stackID"
            if not agg_by.empty:
                variables = variables.merge(agg_by, on='stackID', how='left')

        categories = self.run_query(
            """
            UNWIND $CMID AS cmid
            MATCH (:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY) 
            WHERE NOT 'VARIABLE' IN labels(c)
            RETURN DISTINCT c.CMID AS CMID, c.CMName AS CMName, r.Key AS Key, apoc.text.join(r.Dataset, " || ") AS datasetID
            """,
            parameters={"CMID": self.template["mergingID"].tolist()}
        )

        # Debugging step: Check if categories dataframe is empty
        if categories.empty:
            raise ValueError("Error: No categories retrieved from Neo4j.")
        else:
            print("Processing categories")
            categories = extract_key(categories, col="Key")
            categories = categories.drop(columns=["Key"])
            categories = categories.drop_duplicates()
            categories = categories.reset_index()
            categories = categories.melt(
                id_vars=['index', 'CMID', 'CMName', 'datasetID'],
                var_name='variable',
                value_name='value'
            )
            categories = categories.dropna(subset=["value"])
            # make "variable" lowercase
            categories["variable"] = categories["variable"].str.lower()
            # convert all columns to string
            categories = categories.astype(str)
            categories.replace("None", np.nan, inplace=True)
            categories.to_csv(os.path.join(
                self.dirpath, "mergingCategories.csv"), index=False, na_rep='')

        metadata = self.run_query(
            """
            UNWIND $id AS id
            MATCH (d:DATASET {CMID: id})
            RETURN DISTINCT d.CMID AS datasetID, d.ApplicableYears AS year, apoc.text.join(d.District," || ") AS District
            """,
            parameters={"id": self.template["datasetID"].tolist()}
        )

        if not metadata.empty:
            metadata = metadata.astype(str)
            metadata.replace("None", np.nan, inplace=True)
            metadata.to_csv(os.path.join(
                self.dirpath, "metadata.csv"), index=False, na_rep='')
            print("metadata.csv saved successfully!")
        else:
            print("Warning: No metadata found, metadata.csv not created.")

        stack_vars_list = []

        print("Getting stack variables")
        print(self.template['stackID'].unique())

        for s in self.template['stackID'].unique():
            print("Getting stack variables for stackID: ", s)
            ds = self.template[self.template['stackID'] == s]

            query = (
                "unwind $variableIDs as vid "
                "unwind $datasetIDs as did "
                "match (d:DATASET {CMID: did})-[r:USES]->(v:VARIABLE {CMID: vid}) "
                "return distinct r.Key as Key, v.CMID as variableCMID, d.CMID as datasetID"
            )
            print(variables.columns)
            parameters = {
                "variableIDs": variables['CMID'].dropna().unique().tolist(),
                "datasetIDs": ds['datasetID'].dropna().unique().tolist()
            }

            dfvar = self.run_query(query, parameters)

            if dfvar is not None and not dfvar.empty:
                dfvar['Key'] = dfvar['Key'].str.replace(
                    r"variable:\s*", "", regex=True).str.lower()
                dfvar['stackID'] = s
                stack_vars_list.append(dfvar)

        if len(stack_vars_list) > 0:
            stack_vars = pd.concat(stack_vars_list, ignore_index=True)
            stack_vars = stack_vars.drop_duplicates()
            stack_vars = stack_vars.astype(str)
            stack_vars.replace("None", np.nan, inplace=True)
            stack_vars.to_csv(os.path.join(
                self.dirpath, "stackVariables.csv"), index=False, na_rep='')
            print("stackVariables.csv saved successfully!")
        else:
            print("Warning: No stack variables found, stackVariables.csv not created.")

        dataset_variables_list = []

        for idx, row in self.template.iterrows():
            print("Getting individual dataset variables for stackID: ", row.stackID)
            tmp_vars = (
                variables[variables['transform'].isna()]
                .query("stackID == @row.stackID")
                .loc[:, ['varName', 'variableCMID']]
                .drop_duplicates()
            )

            if not tmp_vars.empty:
                rows = pd.DataFrame({
                    'vid': tmp_vars['variableCMID'],
                    'id': row['datasetID']
                })

                try:
                    query = (
                        "unwind $rows as row "
                        "match (v:VARIABLE {CMID: row.vid})<-[r:USES]-(d:DATASET {CMID: row.id}) "
                        "return v.CMID as variableCMID, tolower(replace(r.Key,'variable: ','')) as transform"
                    )

                    tmp_result = self.run_query(
                        query, parameters={"rows": rows.to_dict(orient='records')})

                    if not tmp_result.empty:
                        tmp_result = (
                            tmp_result
                            .merge(tmp_vars, on='variableCMID', how='left')
                            .assign(datasetID=row['datasetID'])
                        )
                except Exception as e:
                    print("Error getting individual dataset variables")
                    tmp_result = pd.DataFrame()
            else:
                tmp_result = pd.DataFrame()

            dataset_variables_list.append(tmp_result)

        # Combine dataset-specific variables
        dataset_variables = pd.concat(
            dataset_variables_list, ignore_index=True)

        # Filter and tag stack-level variables
        stack_vars = (
            variables[variables['transform'].notna()]
            .copy()
        )
        stack_vars['varLevel'] = "stack"
        stack_vars['variableCMID'] = stack_vars['variableCMID'].astype(str)
        stack_vars = stack_vars.astype(str)

        # Tag dataset-level variables
        dataset_variables['varLevel'] = "dataset"
        dataset_variables = dataset_variables.astype(str)

        # Combine and finalize
        variables = (
            pd.concat([stack_vars, dataset_variables], ignore_index=True)
            .drop(columns=['CMID'], errors='ignore')
            .drop_duplicates()
            .astype(str)
        )

        # Save files if syntax is R
        if self.syntax == "R":
            self.template = self.template.astype(str)
            self.template.replace("None", np.nan, inplace=True)
            self.template.to_csv(os.path.join(
                self.dirpath, "template.csv"), index=False, na_rep='')
            if self.syntax == "R":
                variables = self.transform_variables_r(variables)
            variables = variables.drop_duplicates()
            variables = variables.astype(str)
            variables.replace("None", np.nan, inplace=True)
            variables.to_csv(os.path.join(
                self.dirpath, "variables.csv"), index=False, na_rep='')

            # Create R syntax file
            # Ensure this file exists in the working directory
            r_syntax_template = "syntax/Rsyntax.txt"
            replacements = {
                # Functions applied
                "${f}": "\n".join(variables['transform'].dropna()),
                "${wd}": wd,  # Working directory
                "${database}": self.database  # Database name
            }

            r_syntax = self.load_r_syntax_template(
                r_syntax_template, replacements)
            if r_syntax:
                with open(os.path.join(self.dirpath, "syntax.R"), "w") as f:
                    f.write(r_syntax)

                self.files.extend([
                    os.path.join(self.dirpath, "template.csv"),
                    os.path.join(self.dirpath, "variables.csv"),
                    os.path.join(self.dirpath, "syntax.R"),
                    os.path.join(self.dirpath, "mergingCategories.csv"),
                    os.path.join(self.dirpath, "metadata.csv")
                ])

                zip_path = self.zip_output_files(
                    "merged_output.zip")  # Zip them
                return zip_path
            else:
                raise ValueError("Error: R syntax not found.")
        else:
            raise ValueError("Error: syntax not yet supported.")
