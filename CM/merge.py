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
            raise ValueError("The 'datasetID' column is missing from the joinLeft DataFrame.")

        if 'datasetID' not in joinRight.columns:
            raise ValueError("The 'datasetID' column is missing from the joinRight DataFrame.")

        driver = getDriver(database)

        joinLeft = pd.DataFrame(joinLeft)
        joinRight = pd.DataFrame(joinRight)
                
        # Drop 'CMID' and 'CMName' only if they exist in the columns
        joinLeft = joinLeft.drop(columns=[col for col in ['CMID', 'CMName'] if col in joinLeft.columns]).copy()
        joinRight = joinRight.drop(columns=[col for col in ['CMID', 'CMName'] if col in joinRight.columns]).copy()

        
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
        match_left = getQuery(match_left_query, driver, {"datasetID": datasetID_left})

        # Query keys for right dataset
        match_right_query = """
        UNWIND $datasetID AS id 
        MATCH (d:DATASET {CMID: id})-[r:USES]->() 
        WITH d, split(r.Key, ';') AS Key 
        WITH d, [i IN Key | split(i, ':')[0]] AS Key 
        RETURN DISTINCT d.CMID AS datasetID, Key
        """
        match_right = getQuery(match_right_query, driver, {"datasetID": datasetID_right})

        match_left = pd.DataFrame(match_left)
        match_right = pd.DataFrame(match_right)

        left_keys = match_left['Key'].explode().unique() if 'Key' in match_left else []
        right_keys = match_right['Key'].explode().unique() if 'Key' in match_right else []

        # Check for available columns
        found_left_keys = [key for key in joinLeft.columns if key in left_keys]
        found_right_keys = [key for key in joinRight.columns if key in right_keys]

        # Throw an error only if none of the keys are found
        if not found_left_keys:
            print({"error": "Cannot continue with merge: no matching required columns found in 'joinLeft'"})
        if not found_right_keys:
            print({"error": "Cannot continue with merge: no matching required columns found in 'joinRight'"})


        # Convert only the found columns to string type
        joinLeft[found_left_keys] = joinLeft[found_left_keys].astype(str, errors='ignore')
        joinRight[found_right_keys] = joinRight[found_right_keys].astype(str, errors='ignore')

        merge_left = joinLeft[['datasetID'] + found_left_keys].copy()
        merge_left = createKey(merge_left, cols=found_left_keys).rename(columns={'Key': 'term', 'datasetID': 'dataset'})
        translate_left = translate(database = database, property = "Key", domain = "CATEGORY", term = "term", table = merge_left, key = 'false', country = None, context = None, dataset = 'dataset', yearStart = None, yearEnd = None, query = 'false')
        translate_left = translate_left.rename(columns=lambda x: x.replace('_term', ''))
        merge_left = translate_left[['term', 'CMID', 'CMName', 'dataset']].merge(merge_left, on=['term', 'dataset']).drop(columns='term').rename(columns={'dataset': 'datasetID'}).drop_duplicates()

        # merge right
        merge_right = joinRight[['datasetID'] + found_right_keys].copy()
        merge_right = createKey(merge_right, cols=found_right_keys).rename(columns={'Key': 'term', 'datasetID': 'dataset'})
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
        translate_right = translate_right.rename(columns=lambda x: x.replace('_term', ''))
        merge_right = (
            translate_right[['term', 'CMID', 'CMName', 'dataset']]
            .merge(merge_right, on=['term', 'dataset'])
            .drop(columns='term')
            .rename(columns={'dataset': 'datasetID'})
            .drop_duplicates()
        )


        # Final joining
        # Step 1: Identify overlapping columns between merge_left and merge_right, excluding CMID and CMName
        overlapping_columns = [col for col in merge_left.columns if col in merge_right.columns and col not in ['CMID', 'CMName']]

        # Step 2: Perform the first merge between merge_left and merge_right with suffixes for overlapping columns
        link_file = merge_left.merge(
            merge_right, 
            on=['CMID', 'CMName'], 
            suffixes=('_left', '_right')
        )

        # Step 3: Update found_left_keys and found_right_keys to include suffixes for the identified overlapping columns
        found_left_keys_with_suffix = [f"{key}_left" if key in overlapping_columns else key for key in found_left_keys]
        found_right_keys_with_suffix = [f"{key}_right" if key in overlapping_columns else key for key in found_right_keys]

        # Step 4: Rename datasetID in joinLeft and joinRight for consistent merging
        joinLeft = joinLeft.rename(columns={'datasetID': 'datasetID_left'})
        joinRight = joinRight.rename(columns={'datasetID': 'datasetID_right'})

        left_rename_mapping = dict(zip(found_left_keys, found_left_keys_with_suffix))
        right_rename_mapping = dict(zip(found_right_keys, found_right_keys_with_suffix))
        joinLeft = joinLeft.rename(columns=left_rename_mapping)
        joinRight = joinRight.rename(columns=right_rename_mapping)

        # Step 5: Merge link_file with joinLeft without adding further suffixes for overlapping columns
        link_file = link_file.merge(
            joinLeft, 
            left_on=['datasetID_left'] + found_left_keys_with_suffix, 
            right_on=['datasetID_left'] + found_left_keys_with_suffix, 
            how='left',
            suffixes=('_left', '_right')  # Prevents adding additional _x suffixes
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
        link_file = link_file.drop_duplicates().sort_values(by=['datasetID_left', 'datasetID_right', 'CMName', 'CMID'])

        return link_file.to_dict(orient='records')
    
    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500
        

def proposeMerge(dataset_choices,category_label,criteria,database,intersection, ncontains = 2):

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
        elif criteria == "extended":
            qContains = ""
            qResult = ""
            if ncontains > 1:
                 for i in range(1, ncontains + 1):
                    qContains = qContains + f"optional match (c)<-[:CONTAINS*..{i}]-(p{i}:CATEGORY) " 
                    qResult = qResult + f", p{i}.CMID as parent{i} "

            query = f"""
UNWIND $datasets AS dataset
MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{category_label}) 
optional match (c)<-[:CONTAINS]-(p:CATEGORY) 
{qContains}
RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMName AS CMName, c.CMID AS CMID,
                apoc.text.join(apoc.coll.toSet(r.Name), '; ') AS Name, p.CMID as parent
                {qResult}
ORDER BY CMName
            """

        else:
            raise Exception("Invalid criteria")

        merged = getQuery(query, driver = driver,params = {'datasets': dataset_choices})

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

                
                merged_df.columns = [f"{col[0]}_{col[1]}" for col in merged_df.columns]
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
                return jsonify({"message": "No results"}), 204
    
    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500