import CM
import pandas as pd
import numpy as np
database = "SocioMap"
cmid = "SD10"
children = None
domain = "ETHNICITY"


if domain is None:
    domain = "CATEGORY"

driver = CM.getDriver(database)

session = driver.session()

if children is not None and str(str.lower(children)) == "true":
    query = """
    unwind $cmid as cmid
    match (:DATASET {CMID: cmid})-[:CONTAINS*..5]->(d:DATASET) return distinct d.CMID as CMID
    """
    result = CM.getQuery(query = query, driver = driver, type = "list")
    if result is not None:
        cmid = [cmid] + result

query = """
unwind $cmid as cmid
match (a:DATASET)-[r:USES]->(b) 
where a.CMID = cmid and not isEmpty([i in r.label
where i in apoc.coll.flatten([$domain],true)]) 
unwind keys(r) as property with a,r,b, property 
where not property in ['type','Key','log'] 
return distinct a.CMName as datasetName, a.CMID as datasetID, 
b.CMID as CMID, b.CMName as CMName, r.type as Type, 
r.Key as Key, property, r[property] as value, custom.getName(r[property]) as property_name
"""

data = CM.getQuery(query = query, driver = driver, params = {"cmid":cmid,"domain":domain})

df = pd.DataFrame(data)

df.dropna(axis=1, how='all', inplace=True)

df_names = df[["datasetID","CMID","property","property_name"]].copy()

df = df.drop("property_name", axis=1)

df_names.dropna(subset=["property_name"], how = "all", inplace = True)
df_names = df_names[df_names['property_name'] != '']
df_names['property'] = df_names['property'].apply(lambda x: f"{x}_name")

df_names = df_names.pivot_table(index=["datasetID","CMID"], columns='property', values='property_name', aggfunc='first').reset_index()

cols = [col for col in df.columns if col not in ['property', 'value']]
df = df.pivot_table(index=cols, columns='property', values='value', aggfunc='first').reset_index()
df = pd.merge(df, df_names, on=['datasetID', 'CMID'])
dtypes = df.dtypes.to_dict()
list_cols = []

for col_name, typ in dtypes.items():
    if typ == 'object' and not df[col_name].empty and isinstance(df[col_name].iloc[0], list):
        list_cols.append(col_name)


for col in list_cols:
    df[col] = df[col].apply(lambda x: '|'.join(map(str, x)) if isinstance(x, list) else x)

df = df.astype(str)
df.replace([np.nan, None,"nan"], '', inplace=True)

