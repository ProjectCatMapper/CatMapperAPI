from CM.utils import *
from CM.upload import *

import pandas as pd

df = pd.read_excel('USES_test.xlsx')
df = df[['Name', 'CMID', 'Key', 'yearStart','datasetID']].copy()
df
dataset = df
database = 'SocioMap'
CMName=None
Name='Name'
CMID='CMID'
altNames=None
Key='Key'
formatKey=False
datasetID=None
label='label'
uniqueID=None
uniqueProperty=None 
nodeContext=['parent'] 
linkContext=None
user='1'
overwriteProperties=False
updateProperties=True
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
                 nodeContext=['yearStart'], 
                 linkContext=None,
                 user='1',
                 overwriteProperties=False,
                 updateProperties=True,
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