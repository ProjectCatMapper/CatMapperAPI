from CM.utils import *
from CM.upload import *

import pandas as pd

df = pd.read_excel('Foci.xlsx')
df
dataset = df
database = 'SocioMap'
CMName=None
Name=None
CMID='CMID'
altNames=None
Key=None
formatKey=False
datasetID=None
label='label'
uniqueID=None
uniqueProperty=None 
nodeContext=['foci'] 
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
                 Name=None,
                 CMID='CMID',
                 altNames=None,
                 Key=None,
                 formatKey=False,
                 datasetID=None,
                 label='label',
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=['foci'], 
                 linkContext=None,
                 user='1',
                 overwriteProperties=False,
                 updateProperties=True,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=1000,
                 )