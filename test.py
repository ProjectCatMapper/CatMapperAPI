from CM.utils import *
from CM.upload import *

import pandas as pd

df = pd.read_excel('fixes.xlsx')
df.columns
dataset = df
database = 'SocioMap'
CMName=None
Name='Name'
CMID='CMID'
altNames=None
Key='Key'
formatKey=False
datasetID='datasetID'
label='label'
uniqueID=None
uniqueProperty=None 
nodeContext=None
linkContext=['parent','country','log','label','Name','parent','parentContext'] 
user='1'
overwriteProperties=True
updateProperties=False
addDistrict=False
addRecordYear=False
geocode=False
batchSize=1000

result = input_Nodes_Uses(dataset = df,
                     database = 'SocioMap',
                 CMName=None,
                 Name='Name',
                 CMID='CMID',
                 altNames=None,
                 Key='Key',
                 formatKey=False,
                 datasetID='datasetID',
                 label='label',
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=None, 
                 linkContext=nodeContext,
                 user='1',
                 overwriteProperties=True,
                 updateProperties=False,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=1000,
                 )

result

# dataset = result['df']
# final_result  = result['result']
# cols = list({x for x in ['CMID','CMName'] if x in dataset.columns})
# df = dataset[cols]
# df
# final_result = pd.merge(df, final_result, how='left', on=cols)


from CM.validation import *
from CM.utils import *
from CM.GIS import *


results = validateJSON(database = 'SocioMap', property = 'geoCoords', path ="invalid_json.xlsx" )
results
CMID = [x['CMID'] for x in results]
correct_geojson(CMID, database = 'SocioMap')
# driver = getDriver(database = 'SocioMap')
# updateLabels(driver)

from CM.GIS import *

convert_to_multipoint('{"type":"Point","coordinates":[55.6,-21.1]}; {"type":"Point","coordinates":[55.6,-21.1]}')


from CM.USES import *
from CM.utils import *
driver = getDriver(database = 'SocioMap')
updateLabels(driver = driver)


from CM.utils import *
from CM.keys import *
from CM.translate import *
import pandas as pd
from flask import jsonify

dataset_choices = ['SD5','SD6']
category_label = 'ETHNICITY'
criteria = 'contains'
database = 'SocioMap'
intersection = True
ncontains = 3


driver = getDriver(database)

# if len(dataset_choices) < 1:
    # return jsonify({"message": "Please select more options"}), 400

if criteria == "standard":

    query = f"""
                    UNWIND $datasets AS dataset
                    UNWIND $categoryLabel AS categoryLabel
                    MATCH (c:{category_label})<-[r:USES]-(d:DATASET {{CMID: dataset}}) 
                    RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMName AS CMName, c.CMID AS CMID,
                                    apoc.text.join(r.Name, '; ') AS Name
                    ORDER BY CMName
            """
elif criteria == "contains":
    qContains = ""
    qResult = ""
    for i in range(1, ncontains + 1):
        qContains = qContains + f"optional match (c)<-[:CONTAINS*{i}]-(p{i}:CATEGORY) where not 'GENERIC' in labels(p{i})" 
        qResult = qResult + f", p{i}.CMID as parent{i}, p{i}.CMName as parent{i}_Name"

    query = f"""
UNWIND ['SD5','SD6'] AS dataset
MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{category_label}) 
{qContains}
RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMName AS CMName, c.CMID AS CMID,
        apoc.text.join(r.Name, '; ') AS Name
        {qResult}
ORDER BY CMName
    """

else:
    raise Exception("Invalid criteria")

merged = getQuery(query, driver = driver,params = {'datasets': dataset_choices})

merged = pd.DataFrame(merged)

# if merged.empty:
    # return jsonify({"message": "No results"}), 204

indx_cols = ['CMName', 'CMID']

if criteria == "contains":
    indx_cols.append('parent')
    for i in range(1, ncontains + 1):
        indx_cols.append(f'parent{i}')

indx_cols = list(set(indx_cols))
indx_cols = [col for col in indx_cols if col in merged.columns]

merged_df = merged.pivot_table(
    index=indx_cols,
    columns='datasetID',
    values=['Key', 'Name'],
    aggfunc=lambda x: '; '.join(filter(None, x))
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
# return merged

# complex joins

name_dict = merged[[col for col in merged.columns if "parent" in col] + ["CMName","CMID"]].drop_duplicates()
name_columns = [col for col in name_dict.columns if "Name" in col]
id_columns = [col for col in name_dict.columns if col not in name_columns]
names_long = name_dict[name_columns].melt(value_name="CMName_proposed").drop(columns="variable")
ids_long = name_dict[id_columns].melt(value_name="CMID_proposed").drop(columns="variable")
name_dict = pd.concat([names_long, ids_long], axis=1).drop_duplicates().reset_index(drop=True)

pivoted_df = pd.melt(
    merged,
    id_vars=["datasetID","Key", "Name"], 
    value_vars=id_columns,  
    var_name="origin",  
    value_name="CMID_proposed",  
)

pivoted_df = pivoted_df.dropna(subset=['CMID_proposed'])

dc0v = dataset_choices[0]
dc1v = dataset_choices[1]

dc0 = pivoted_df[(pivoted_df['datasetID'] == dc0v)].copy()
dc1 = pivoted_df[(pivoted_df['datasetID'] == dc1v)].copy()


merged_df = pd.merge(dc0, dc1, how='inner', on=['CMID_proposed'], suffixes=(f'_{dc0v}', f'_{dc1v}'))
merged_df.drop(columns=[f'datasetID_{dc0v}', f'datasetID_{dc1v}'], inplace=True)


merged_df['path_length'] = (
    # Add 0 if f"origin_{dc0v}" is 'CMID'
    (merged_df[f"origin_{dc0v}"].str.contains("CMID").astype(int) * 0)
    +
    # Add 0 if f"origin_{dc1v}" is 'CMID'
    (merged_df[f"origin_{dc1v}"].str.contains("CMID").astype(int) * 0)
    +
    # Extract numeric values from f"origin_{dc0v}"
    (merged_df[f"origin_{dc0v}"].str.extract(r'(\d+)$').astype(float).fillna(0)[0])
    +
    # Extract numeric values from f"origin_{dc1v}"
    (merged_df[f"origin_{dc1v}"].str.extract(r'(\d+)$').astype(float).fillna(0)[0])
)

sp0 = merged_df.groupby([f"Key_{dc0v}"], group_keys=False).apply(
    lambda group: group[group["path_length"] == group["path_length"].min()]
).drop_duplicates()

sp1 = merged_df.groupby([f"Key_{dc1v}"], group_keys=False).apply(
    lambda group: group[group["path_length"] == group["path_length"].min()]
).drop_duplicates()

shortest_paths = pd.concat([sp0, sp1], ignore_index=True).drop_duplicates()

shortest_paths = pd.merge(shortest_paths, name_dict, how='left', on = ['CMID_proposed'])

shortest_paths = shortest_paths[['CMID_proposed', 'CMName_proposed', f"Name_{dc0v}",f"Key_{dc0v}",f"origin_{dc0v}", f"Name_{dc1v}",f"Key_{dc1v}",f"origin_{dc1v}", 'path_length']].copy().sort_values(by='CMName_proposed')

shortest_paths.rename(columns={'CMID_proposed':'CMID', 'CMName_proposed':'CMName'}, inplace=True)

shortest_paths.to_excel('merged_df.xlsx')


# merged_df[(merged_df[f"origin_{dc0v}"] == 'CMID') & (merged_df[f"origin_{dc1v}"] == 'parent3')].copy().drop_duplicates()

# shortest_paths[(shortest_paths[f"Key_{dc0v}"] == 'Q84: 1252; COUNTRY: 26') & (shortest_paths[f"Key_{dc1v}"] == "Q87: 1250; COUNTRY: 6")].copy().drop_duplicates()


database = "sociomap"
driver = getDriver(database)
query = "match (a)<-[r]-(b) unwind keys(r) as key return distinct key"
result = getQuery(query, driver = driver, type = 'list')

for key in result:
    print(key)
    query = f"match (a)<-[r]-(b) where not r.{key} is null and (r.{key} = '' or r.{key} = [] or r.{key}= ['']) set r.{key} = NULL return count(*)"
    r = getQuery(query, driver = driver, type = 'list')
    print(r)


database = "sociomap"
driver = getDriver(database)
query = f"match (a)<-[r]-(b) unwind keys(r) as key with key where r[key] is not null and apoc.meta.cypher.type(r[key]) = 'LIST OF STRING' return distinct key"
result = getQuery(query, driver = driver, type = 'list')

for key in result:
    print(key)
    query = f"""
    match (a)<-[r]-(b) 
    where not r.{key} is null and 
    apoc.meta.cypher.type(r.{key}) = 'LIST OF STRING' 
    with r, size(r.{key}) as sz, 
    [i in r.{key} where not i = ''] as fixed 
    with r, sz, fixed, size(fixed) as sz2 
    where sz2 < sz 
    set r.{key} = fixed 
    return count(*)
    """
    r = getQuery(query, driver = driver, type = 'list')
    print(r)
