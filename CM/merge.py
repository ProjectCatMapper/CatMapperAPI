"""merge.py"""

from .utils import *
from .keys import *
from .translate import *
import pandas as pd


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