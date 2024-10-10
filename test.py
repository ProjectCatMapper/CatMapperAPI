from CM.utils import *
from CM.USES import *
from CM.upload import *
import json
import pandas as pd
from flask import jsonify
import numpy as np
import time
import re
import warnings

dataset = pd.read_excel("Test1.xlsx")

result = input_Nodes_Uses(dataset = dataset, database = "SocioMap", CMName = "CMName",Name = "Name", CMID = None, Key = "Key", datasetID = "datasetID", label = "label", user = "1", updateProperties=False,linkContext=["parent","eventType","eventDate","religion", 'language', 'country', 'latitude','longitude'])

def contains_lists(df):
    columns_with_lists = []
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            columns_with_lists.append(col)
    return columns_with_lists

# Check the DataFrame for columns containing lists
columns_with_lists = contains_lists(result)

if columns_with_lists:
    print(f"The following columns contain lists: {columns_with_lists}")
else:
    print("No columns contain lists.")

# createUSES(result,'SocioMap','1', create = "MERGE")
# database = "SocioMap"
# CMName=None
# Name="Name"
# CMID="CMID"
# altNames=None
# Key="Key"
# formatKey=False
# datasetID="datasetID"
# label="label"
# uniqueID=None
# uniqueProperty=None 
# nodeContext=[] 
# linkContext=[]
# user="1"
# checkUnique=False
# overwriteProperty=False
# updateProperty=False
# addDistrict=False
# addRecordYear=False
# geocode=False
# batchSize=100

# driver = getDriver(database)

# with open(f"log/{user}uploadProgress.txt", 'w') as f:
#     f.write("Starting database upload")

# if 'eventType' in dataset.columns and 'eventDate' not in dataset.columns:
#     dataset['eventDate'] = np.nan

# if label is None:
#     isDataset = False
# else:
#     isDataset = label == "DATASET" or dataset['label'].iloc[0] == "DATASET"


# dataset = dataset.dropna(axis=1, how='all')

# columns_to_select = [CMName, Name, CMID, altNames, Key, datasetID, label, uniqueID, 
#                         "shortName", "DatasetCitation"] + nodeContext + linkContext
# dataset = dataset[[col for col in columns_to_select if col in dataset.columns]]

# if isDataset:
#     column_names = [CMName, label, uniqueID] + nodeContext
# else:
#     if overwriteProperty or updateProperty:
#         column_names = [CMName, Name, altNames, CMID, Key, datasetID, uniqueID] + nodeContext + linkContext
#     else:
#         column_names = [CMName, Name, altNames, label, Key, datasetID, uniqueID] + nodeContext + linkContext

# # Remove None values
# column_names = [col for col in column_names if col is not None]

# errors = [f"{col} must be in dataset" for col in column_names if col not in dataset.columns]

# if len(errors) > 0:
#     with open(f"log/{user}uploadProgress.txt", 'a') as f:
#         f.write("\n".join(errors))
#     raise ValueError("\n".join(errors))

# properties = getPropertiesMetadata(driver)
# properties = pd.DataFrame(properties)

# if not "label" in dataset.columns:
#     raise ValueError("Must include label")

# if uniqueID is None or uniqueID not in dataset.columns:
#     print("Creating import ID")
#     getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver)
#     uniqueID = 'importID'
#     uniqueProperty = 'importID'
#     dataset['importID'] = dataset.index + 1

# sq = range(0, len(dataset), batchSize)
# s = 0
# try:
# dataset_match = pd.DataFrame()
#     for s in sq:
# print(f"Beginning upload of rows {s} to {s + batchSize}")
# sub_dataset = dataset.iloc[s:s + batchSize]
# max_row = len(sub_dataset) - 1 + s
# with open(f"log/{user}uploadProgress.txt", 'a') as f:
#     f.write(f"uploading {s} to {max_row} of {len(dataset)}")

# if not isDataset:
#     print("Combining paired properties")
#     paired = properties.merge(pd.DataFrame({'property': sub_dataset.columns}), on='property')
#     paired = paired[paired['group'].notna()].groupby('group')
    
#     for group, pair in paired:
#         if pair['property'].isin(sub_dataset.columns).any():
#             sub_dataset[group] = sub_dataset[pair['property']].apply(lambda x: x.str.strip()).agg('; '.join, axis=1)
#             linkContext.append(group)

# if 'CMID' in sub_dataset.columns:
#     if "datasetID" in sub_dataset.columns and "Key" in sub_dataset.columns:
#         if 'CMID' in sub_dataset.columns:
#             sub_dataset = combine_properties(sub_dataset, ['CMID', 'datasetID', 'Key'])
#         else:
#             sub_dataset = combine_properties(sub_dataset, ['datasetID', 'Key'])

# if addDistrict:
#     print("Adding district")
#     matches = getQuery(params={'rows': sub_dataset[['datasetID']]}, q='DISTRICT QUERY', database=database, user='1')
#     if not matches.empty:
#         sub_dataset = sub_dataset.merge(matches, on="datasetID", how="left")
#         linkContext.append('country')

# if addRecordYear:
#     print("Adding record year")
#     matches = getQuery(params={'rows': sub_dataset[['datasetID']]}, q='RECORD_YEAR QUERY', driver = driver)
#     if not matches.empty:
#         sub_dataset = sub_dataset.merge(matches, on="datasetID", how="left")
#         linkContext.append('recordStart')

# sub_dataset = sub_dataset.fillna('')

# node_columns = [CMName, uniqueID, 'label'] + nodeContext
# node_columns = [col for col in node_columns if col in sub_dataset.columns]  


# if isDataset:
#     nodes = sub_dataset[[CMName, "shortName", "DatasetCitation", uniqueID, 'label'] + nodeContext].drop_duplicates()
# else:
#     if Name:
#         nodes = sub_dataset[sub_dataset['CMID'] == ''][node_columns].drop_duplicates()
#     else:
#         nodes = pd.DataFrame()

# if not nodes.empty:
#     print("Adding nodes")
#     match = createNodes(nodes,driver, uniqueID=uniqueID, uniqueProperty=uniqueProperty, user=user, checkUnique=False)
#     dataset_match = pd.concat([dataset_match, match], ignore_index=True)

# link_columns = ['datasetID', CMName, 'CMID', Name, altNames, Key, uniqueID, label] + linkContext
# link_columns = [col for col in link_columns if col in sub_dataset.columns]

# if not isDataset:
#     print("Adding USES relationships")
#     links = sub_dataset[link_columns].drop_duplicates().copy()

#     links.rename(columns={'datasetID': 'from', 'CMID': 'to'}, inplace=True)
    
#     if Name and altNames is not None:
#         links = combine_names_and_altNames(links, Name, altNames)
    
#     if linkContext is not None and 'geoCoords' in linkContext:
#         links = handle_geo_coordinates(links, properties)

#     link_cols = ['from', 'to', 'Key'] + linkContext
#     link_cols = [col for col in link_cols if col in links.columns]
#     if overwriteProperty:
#         print("Overwriting property")
#         result = updateProperty(links[link_cols], database = database, user = user, updateType = "overwrite")
#     elif updateProperty:
#         print("Updating property")
#         result = updateProperty(links[link_cols], database = database, user = user, updateType = "update")
#     else:
#         print("Adding new USES relationships")
#         result = createUSES(links[link_cols],database = database, user = user, create = "MERGE")

# cmid_values = [link['to'] for link in result['links']]
# updateAltNames(driver, CMID = cmid_values)

# if uniqueID == 'importID':
#     getQuery("MATCH (a) WHERE a.importID IS NOT NULL SET a.importID = NULL", driver = driver)

# # except Exception as e:
# # warnings.warn(str(e))
# # with open(f"log/{user}uploadProgress.txt", 'a') as f:
# #     f.write(f"Error: {e}")
# # # return None

# with open(f"log/{user}uploadProgress.txt", 'a') as f:
#     f.write("Completed dataset upload")

# # return dataset_match


from CM.USES import *
from CM.utils import *
from CM.upload import *
getAvailableID(new_id = "CMID", n = 1, database = 'sociomap')

# waitingUSES("SocioMap", BATCH_SIZE = 1000)
# BATCH_SIZE = 1000
# database = "SocioMap"
# driver = getDriver(database)
# CMID = getQuery("Match (c)<-[r:USES]-(d:DATASET) where r.status is not null and r.status = 'update' return c.CMID as CMID", driver, type = 'list')


from CM.utils import *
import pandas as pd

query = 'false'
domain = 'AREA'
database = 'sociomap'
property = 'Name'       
key = 'true'
term = 'Name'
country = None
context = None
dataset = 'datasetID'
yearStart = None
yearEnd = None
table = pd.read_excel('translate.xlsx')

if query.lower() != 'true':
    query = 'false'

if domain == "ANY DOMAIN":
    domain = "CATEGORY"
if domain == "AREA":
    domain = "DISTRICT"
if str.lower(key) != 'true':
    key = None
if str.lower(database) == "sociomap":
    driver = connectionSM()
elif str.lower(database) == "archamap":
    driver = connectionAM()
else:
    raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")


# format data
# add rowid, 
# table = [{'Name':'test1',"key": 1}, {'Name':'test1',"key": 2}, {'Name':'test2',"key": 3}]
df = pd.DataFrame(table)
df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
df['CMuniqueRowID'] = df.index
rows = pd.DataFrame({'term': df[term],'CMuniqueRowID': df["CMuniqueRowID"]})
if isinstance(country,str) and country in df.columns:
    rows['country'] = df[country]
if isinstance(context,str) and context in df.columns:
    rows['context'] = df[context]
if isinstance(dataset,str) and dataset in df.columns:
    rows['dataset'] = df[dataset]
if isinstance(yearStart,str) and yearStart is not None:
    rows['yearStart'] = yearStart
if isinstance(yearEnd,str) and yearEnd is not None:
    rows['yearEnd'] = yearEnd
rows.dropna(subset=['term'], inplace=True)
rows = rows[rows['term'] != '']
columns_to_group_by = rows.columns.difference(['CMuniqueRowID']).tolist()
rows = rows.groupby(columns_to_group_by)['CMuniqueRowID'].apply(list).reset_index()
rows = rows.to_dict('records')

# Define the Cypher query

qLoad = "unwind $rows as row with row call {"

if property == "Key":
    qStart = f"""
with row call db.index.fulltext.queryRelationships('keys','"' + tolower(row.term) +'"') yield relationship
with row, endnode(relationship) as a, relationship.Key as matching, case when row.term contains ":" then row.term else ": " + row.term end as term
where '{domain}' in labels(a) and matching ends with term
with row, a, matching, 0 as score
"""
elif property in ["glottocode","ISO","CMID"]:
    if property == "CMID":
        indx = "CMIDindex"
    else:
        indx = property

    qStart = f"""
with row call db.index.fulltext.queryNodes('{indx}','"' + toupper(row.term) +'"') yield node
with row, node as a, toupper(node['{property}']) as matching, toupper(row.term) as term
where matching = term
with row, a call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{{a:a}}) yield value
with row, value.node as a, value.matching as matching, 0 as score
"""

elif property == "Name":

    if domain != "DATASET":
        qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist(a.names, row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""
    else:
        qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""

else:
    qStart = f""" 
with row call apoc.cypher.run('match (a:{domain}) 
where not a.{property} is null and tolower(a.{property}) = tolower(\"' + row.term + '\") 
return a, a.{property} as matching',{{}}) yield value 
with row, value.a as a, value.matching as matching, 0 as score
"""

# filter by domain

qDomain = f" where '{domain}' in labels(a) with row, a, matching, score "

# filter by country
if 'country' in rows[0]:
    qCountryFilter = """
where (a)<-[:DISTRICT_OF]-(:ADM0 {CMID: row.country})
with row, a, matching, score
"""
else:
    qCountryFilter = " "

# filter by context
if 'context' in rows[0]:
    qContext = """
where (a)<-[]-({CMID: row.context})
with row, a, matching, score
"""
else:
    qContext = " "

# filter by dataset
if 'dataset' in rows[0]:
    if property == "Key":
        qDataset = """
match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
where r.Key ends with row.term
with row, a, matching, score, r.Key as Key
"""
    else: 
        qDataset = """
match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
with row, a, matching, score, r.Key as Key
"""
else:
    qDataset = "with row, a, matching, score, '' as Key"

if key is None:
    "with row, a, matching, score, '' as Key"

# filter by year
if 'yearStart' in rows[0] and 'yearEnd' in rows[0]:
    if domain == "DATASET":
        qYear = """
call {with row, a with row, a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
with node as a, matching, score, Key
"""
    else:
        qYear = f"""
call {{with row, a with row, a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years 
match (a)<-[r:USES]-(:DATASET) unwind r.yearStart as yearStart 
unwind r.yearEnd as yearEnd with years, a, r, apoc.coll.toSet(collect(yearStart) + collect(yearEnd)) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}}
with row, node as a, matching, score, Key order by score desc
"""   
else: 
    qYear = " "

# limit results
qLimit = """
with row, collect(a{a, matching, score}) as nodes, collect(score) as scores, Key
with row, nodes, apoc.coll.min(scores) as minScore, Key
unwind nodes as node
with row, node.a as a, node.matching as matching, node.score as score, minScore, Key
where score = minScore
return distinct a, matching, score, Key}
with row, a, matching, score, Key
"""

# get country
qCountry = """
optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
with row, a, matching, collect(c.CMName) as country, score, Key
"""

# return results
qReturn = """
return distinct row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
matching, score as matchingDistance, country, Key order by matchingDistance
"""
cypher_query = qLoad + qStart + qDomain + qCountryFilter + qContext + qDataset + qYear + qLimit + qCountry + qReturn
if query == "true":
    with driver.session() as session:
        result = session.run("unwind $rows as rows unwind rows as row return row.term as term", rows = rows)
        qResult = [dict(record) for record in result]
        print("printing rows")
        print(rows)
    return [{"query": cypher_query.replace("\n"," "),"params":qResult,"rows":rows}]
else:
# Execute the Cypher queries
    with driver.session() as session:
        result = session.run(cypher_query, rows = rows)

    # Process the query results and generate the dynamic JSON
        data = [dict(record) for record in result]

        driver.close()

data = pd.DataFrame(data)
data = data.replace("", pd.NA)
data = data.dropna(axis='columns', how='all')
# add matching type
data = CM.addMatchResults(results = data)
new_column_names = {col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
data = data.rename(columns=new_column_names)
data = data.explode('CMuniqueRowID')
data = data.drop(f"term_{term}", axis=1)

data['CMuniqueRowID'] = data['CMuniqueRowID'].astype(int)
df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(int)

data = pd.merge(df, data, on="CMuniqueRowID", how='outer')
data[f'matchType_{term}'] = data[f'matchType_{term}'].fillna('none')
data.fillna('', inplace=True)
dtypes = data.dtypes.to_dict()
list_cols = []
for col_name, typ in dtypes.items():
    if typ == 'object' and isinstance(data[col_name].iloc[0], list):
        list_cols.append(col_name)

for col in list_cols:
    data[col] = data[col].apply(lambda x: '|'.join(map(str, x)))

data = data.astype(str)

colOrder = [
    term,
    f"CMID_{term}",
    f"CMName_{term}",
    f"matching_{term}",
    f"matchingDistance_{term}",
    f"label_{term}",
    f"country_{term}",
    f"Key_{term}",
    f"matchType_{term}",
    "CMuniqueRowID"
]


for col in data.columns:
    if col not in colOrder:
        colOrder.append(col)


finalColOrder = [col for col in colOrder if col in data.columns]

data = data[finalColOrder]

data_dict = data.to_dict(orient='records')