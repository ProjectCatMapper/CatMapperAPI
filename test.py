from CM.merge import *
from CM.utils import *
from CM.keys import *
from CM.translate import *
import pandas as pd

autoLeft = pd.read_excel('Afrobarometer 5.xlsx')
autoRight = pd.read_excel('Afrobarometer 6.xlsx')
autoLeft = autoLeft.drop(columns=['CMID', 'CMName']).copy()
autoRight = autoRight.drop(columns=['CMID', 'CMName']).copy()
autoLeft
autoRight

# result = CM.joinDatasets(database = 'SocioMap', autoLeft = autoLeft, autoRight = autoRight)
database = 'SocioMap'

driver = getDriver(database)

autoLeft = pd.DataFrame(autoLeft)
autoRight = pd.DataFrame(autoRight)

datasetID_left = autoLeft['datasetID'].unique()
datasetID_right = autoRight['datasetID'].unique()

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
found_left_keys = [key for key in autoLeft.columns if key in left_keys]
found_right_keys = [key for key in autoRight.columns if key in right_keys]

# Throw an error only if none of the keys are found
if not found_left_keys:
    print({"error": "Cannot continue with merge: no matching required columns found in 'autoLeft'"})
if not found_right_keys:
    print({"error": "Cannot continue with merge: no matching required columns found in 'autoRight'"})


# Convert only the found columns to string type
autoLeft[found_left_keys] = autoLeft[found_left_keys].astype(str, errors='ignore')
autoRight[found_right_keys] = autoRight[found_right_keys].astype(str, errors='ignore')

merge_left = autoLeft[['datasetID'] + found_left_keys].copy()
merge_left = createKey(merge_left, cols=found_left_keys).rename(columns={'Key': 'term', 'datasetID': 'dataset'})
translate_left = translate(database = database, property = "Key", domain = "CATEGORY", term = "term", table = merge_left, key = 'false', country = None, context = None, dataset = 'dataset', yearStart = None, yearEnd = None, query = 'false')
translate_left = translate_left.rename(columns=lambda x: x.replace('_term', ''))
merge_left = translate_left[['term', 'CMID', 'CMName', 'dataset']].merge(merge_left, on=['term', 'dataset']).drop(columns='term').rename(columns={'dataset': 'datasetID'}).drop_duplicates()

# merge right
merge_right = autoRight[['datasetID'] + found_right_keys].copy()
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

# Step 4: Rename datasetID in autoLeft and autoRight for consistent merging
autoLeft = autoLeft.rename(columns={'datasetID': 'datasetID_left'})
autoRight = autoRight.rename(columns={'datasetID': 'datasetID_right'})

left_rename_mapping = dict(zip(found_left_keys, found_left_keys_with_suffix))
right_rename_mapping = dict(zip(found_right_keys, found_right_keys_with_suffix))
autoLeft = autoLeft.rename(columns=left_rename_mapping)
autoRight = autoRight.rename(columns=right_rename_mapping)

# Step 5: Merge link_file with autoLeft without adding further suffixes for overlapping columns
link_file = link_file.merge(
    autoLeft, 
    left_on=['datasetID_left'] + found_left_keys_with_suffix, 
    right_on=['datasetID_left'] + found_left_keys_with_suffix, 
    how='left',
    suffixes=('', '')  # Prevents adding additional _x suffixes
)

# Step 6: Merge link_file with autoRight without adding further suffixes for overlapping columns
link_file = link_file.merge(
    autoRight, 
    left_on=['datasetID_right'] + found_right_keys_with_suffix, 
    right_on=['datasetID_right'] + found_right_keys_with_suffix, 
    how='left',
    suffixes=('_left', '_right') 
)

# Step 7: Final clean-up to drop duplicates and sort by specified columns
link_file = link_file.drop_duplicates().sort_values(by=['datasetID_left', 'datasetID_right', 'CMName', 'CMID'])



from CM.USES import *
from CM.utils import *
import pandas as pd
CMID = None
database = "SocioMap"
driver = getDriver(database)

if CMID is not None:
        getQuery("unwind $CMID as cmid match (a {CMID: cmid})<-[:USES]-(:DATASET) set a:CATEGORY",driver, params = {"CMID":CMID})
else:
        getQuery("match (a)<-[:USES]-(:DATASET) set a:CATEGORY",driver)

if CMID is not None:
        qFiltera = "unwind $cmid as cmid"
        qFilterb = "a.CMID = cmid and"
        qFilterC = "with l, cmid"
else: 
        qFiltera = ""
        qFilterb = ""
        qFilterC = "with l"

query = f"""
match (l:LABEL)
{qFiltera}
{qFilterC}
match (a:CATEGORY)<-[r:USES]-(:DATASET)
where 
{qFilterb}
r.label is not null
with a, r, l
with a, collect(distinct l.label) + "CATEGORY" as l, apoc.coll.flatten(collect(distinct r.label),true) as labels
with a, [i in labels where i in l] as labels
with a, labels as labels
call apoc.create.setLabels(a,labels) yield node
return count(*)
"""

result = getQuery(query = query, driver = driver, params = {"cmid":CMID})

labels = getLabelsMetadata(driver = driver)

for label,groupLabel in zip([item['label'] for item in labels if 'label' in item],[item['groupLabel'] for item in labels if 'groupLabel' in item]):
        query = f"match (a:{label}) set a:{groupLabel}"
        getQuery(driver = driver, query = query)