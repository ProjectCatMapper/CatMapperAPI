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

dataset = pd.read_excel("UploadDatasetTest.xlsx")
dataset.columns
result = input_Nodes_Uses(dataset = dataset, database = "SocioMap", CMName = "CMName",
                          Name = None, CMID = None, Key = None, formatKey = False, datasetID = None, label = "label",
                            user = "1", updateProperties=False,linkContext=None,
                            nodeContext = ['shortName','project','DatasetCitation','DatasetLocation','DatasetVersion','parent','District'],overwriteProperties = False)

# result = input_Nodes_Uses(dataset = dataset, database = "SocioMap", CMName = None,Name = None, CMID = "CMID", Key = "Key", datasetID = "datasetID", label = None, user = "1", updateProperties=False,linkContext=['parent'],overwriteProperties = True)
# dataset = pd.read_excel("Test1.xlsx")
# result = input_Nodes_Uses(dataset = dataset, database = "SocioMap", CMName = 'CMName',Name = "Name", CMID = None, Key = "Key", datasetID = "datasetID", label = "label", user = "1", updateProperties=False,linkContext=["parent","eventType","eventDate","religion", 'language', 'country', 'latitude','longitude'])

import CM
credentials = CM.login('sociomap','rbischoff','archa')
credentials

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


# from CM.USES import *
# from CM.utils import *
# from CM.upload import *
# getAvailableID(new_id = "CMID", n = 1, database = 'sociomap')

# waitingUSES("SocioMap", BATCH_SIZE = 1000)
# BATCH_SIZE = 1000
# database = "SocioMap"
# driver = getDriver(database)
# CMID = getQuery("Match (c)<-[r:USES]-(d:DATASET) where r.status is not null and r.status = 'update' return c.CMID as CMID", driver, type = 'list')


from  CM.translate import *
from  CM.translate import *
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

data = translate(
        database = database,
        property = property,
        domain = domain,
        key = key,
        term = term,
        country = country, 
        context = context,
        dataset = dataset,
        yearStart = yearStart, 
        yearEnd = yearEnd,
        query = query,
        table = table)

data

print(data[0].get('query'))