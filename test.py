from CM.utils import *
from CM.upload import *
import pandas as pd

df = pd.read_excel("NewEthnicitiesLOC_D.xlsx")

df = df[["Name","Key","CMID","country","parent","language"]]

result = input_Nodes_Uses(dataset = df,
                            database = "SocioMap",
                            uploadOption="add_uses",
                            formatKey=False,
                            nodeContext=None, 
                            linkContext=["country","parent","religion","language"],
                            user="1",
                            addDistrict=False,
                            addRecordYear=False,
                            geocode=False,
                            batchSize=1000)


from CM import *
import pandas as pd
updateType = "update"
database = "SocioMap"
propertyType = "USES"
user = "1"
links = [{'datasetID': "SD2177", 'CMID': "SM250784", 'Key': "OWC: FF38", 'url': "https://ehrafworldcultures.yale.edu/cultures/ff38"}]
links = pd.DataFrame(links)
if not updateType in ['overwrite','update']:
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
    
vars = links.drop(columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

if updateType == "update":
    links['log'] = links.apply(lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: updated properties {', '.join([str(var) for var in vars])}", axis=1)
else:
    links['log'] = links.apply(lambda row: f"{time.strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: overwrote properties {', '.join([str(var) for var in vars])}", axis=1)

vars = links.drop(columns=[col for col in requiredCols if col in links.columns]).columns.tolist()

query = """
match (n:METADATA:PROPERTY) 
return n.property as property, n.type as type, 
n.relationship as relationship, n.description as description, 
n.display as display, n.group as group, n.metaType as metaType, n.search as search, n.translation as translation
"""

metaTypes = getQuery(query, driver)
metaTypeDict = {item['property']: item['metaType'] for item in metaTypes}

keys = []
for var in vars:
    metaType = metaTypeDict.get(var)  # Get the metaType for the given property
    if updateType == "overwrite" and var != 'log':
        keys.append(f"r.{var} = custom.combinedProperties('',row.{var},'{metaType}')[0].prop")
    else:
        keys.append(f"r.{var} = custom.combinedProperties(r.{var},row.{var},'{metaType}')[0].prop")

keys = ", ".join(keys)

if propertyType == "USES":
    q = f"""
    UNWIND $rows AS row
    MATCH (a:DATASET {{CMID: row.datasetID}})-[r:USES {{Key: row.Key}}]->(b:CATEGORY {{CMID: row.CMID}}) 
    WITH row, r, b
    SET {keys}, r.status = "update"
    RETURN id(b) as nodeID, b.CMID as CMID, row.Key as Key, row.datasetID as datasetID, row.parent as parent, row.parentContext as parentContext
    """
else:
    q = f"""
    UNWIND $rows AS row
    MATCH (r:DATASET {{CMID: row.CMID}})
    SET {keys}, r.status = "update"
    RETURN id(r) as nodeID, r.CMID as CMID
    """

links_dict = links.to_dict(orient = "records")

result = getQuery(query = q, driver = driver, params = {"rows": links_dict})

if 'geoCoords' in links.columns:
    updateLog(f"log/{user}uploadProgress.txt", "Updating geo coordinates", write = 'a')
    CMIDs = links['CMID'].unique()
    correct_geojson(CMID = CMIDs, database = database)


from CM import *
import pandas as pd
database = "sociomap"
driver = getDriver(database)

